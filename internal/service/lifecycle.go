package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/pricing"
	"github.com/boundflow/boundflow/internal/storage"
)

var ErrMissingRuntimeParams = errors.New("operation_timeout_seconds must be set on the request, tenant policy, or tenant group policy")
var ErrInvalidWorkflowState = errors.New("workflow cannot be invoked in its current state")
var ErrNotInterrupted = errors.New("workflow is not interrupted or the request id does not match its last interruption")
var ErrInvalidRepeatInterval = errors.New("repeat_every_seconds is below the minimum the scheduler can honor")

// RequestScheduler is the scheduling capability the lifecycle service needs.
// Satisfied by *scheduler.Scheduler.
type RequestScheduler interface {
	ScheduleRequest(ctx context.Context, requestID string) error
}

// ApprovalResolver handles approve/reject for jobs awaiting approval.
// Satisfied by *scheduler.Scheduler.
type ApprovalResolver interface {
	ApproveJob(ctx context.Context, workflowID string, approvalID string) (bool, domain.ResolvedApproval, error)
	RejectJob(ctx context.Context, workflowID string, approvalID string) (bool, domain.ResolvedApproval, error)
}

type LifecycleService struct {
	workflows storage.WorkflowRepository
	customerRequests  storage.CustomerRequestRepository
	tenants           storage.TenantRepository
	tenantGroups      storage.TenantGroupRepository
	agentStates       storage.AgentStateRepository
	modelPricing      storage.ModelPricingRepository
	scheduler         RequestScheduler
	approvalResolver  ApprovalResolver
	audit             storage.AuditRepository
	numPartitions     int
	// minRepeatSeconds is the scheduler's periodic poll cadence; a smaller
	// repeat_every_seconds can't be honored, so CreateWorkflow rejects it.
	minRepeatSeconds int
	log              *slog.Logger
}

func NewLifecycleService(
	workflows storage.WorkflowRepository,
	customerRequests storage.CustomerRequestRepository,
	tenants storage.TenantRepository,
	tenantGroups storage.TenantGroupRepository,
	agentStates storage.AgentStateRepository,
	modelPricing storage.ModelPricingRepository,
	scheduler RequestScheduler,
	approvalResolver ApprovalResolver,
	audit storage.AuditRepository,
	numPartitions int,
	minRepeatSeconds int,
	log *slog.Logger,
) *LifecycleService {
	return &LifecycleService{
		workflows: workflows,
		customerRequests:  customerRequests,
		tenants:           tenants,
		tenantGroups:      tenantGroups,
		agentStates:       agentStates,
		modelPricing:      modelPricing,
		scheduler:         scheduler,
		approvalResolver:  approvalResolver,
		audit:             audit,
		numPartitions:     numPartitions,
		minRepeatSeconds:  minRepeatSeconds,
		log:               log.With("component", "lifecycle_service"),
	}
}

// ResolveModelPricing snapshots the tenant group's effective per-model rates
// (built-in defaults merged with overrides) into the request context, so the
// worker can price token usage without a hardcoded table.
func (s *LifecycleService) ResolveModelPricing(ctx context.Context, workflowID string, requestInfo map[string]any) error {
	groupID, err := s.workflows.TenantGroupIDForWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("resolve tenant group for pricing: %w", err)
	}
	defaults, err := s.modelPricing.ListDefaults(ctx)
	if err != nil {
		return fmt.Errorf("list default pricing: %w", err)
	}
	overrides, err := s.modelPricing.ListForTenantGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("list model pricing: %w", err)
	}
	effective := pricing.Effective(defaults, overrides)
	m := make(map[string]any, len(effective))
	for id, p := range effective {
		m[id] = map[string]any{"input_per_1m": p.InputPer1M, "output_per_1m": p.OutputPer1M}
	}
	requestInfo["modelPricing"] = m
	return nil
}

func (s *LifecycleService) ResolveRuntimeParams(params domain.WorkflowRuntimeParams, instance *domain.Workflow, userTriggered bool, requestInfo map[string]any) error {
	if !instance.WorkflowConfig.Triggerable && userTriggered {
		return fmt.Errorf("workflow is not triggerable")
	}

	// The workflow version to run is resolved dynamically at schedule time from the live
	// workflow (CurrentWorkflowVersion); it is intentionally not snapshotted here. This is
	// just a validation that a runnable version is configured.
	if instance.CurrentWorkflowVersion == 0 {
		return fmt.Errorf("no workflow version specified")
	}

	timeout := params.OperationTimeoutSeconds
	if timeout == 0 {
		timeout = int(instance.WorkflowConfig.InvokeTimeoutSeconds)
		if timeout == 0 {
			return fmt.Errorf("no invoke timeout specified")
		}
	}

	requestInfo["operationTimeoutSeconds"] = timeout
	return nil
}

func (s *LifecycleService) ResolveAgentRuntimeParams(ctx context.Context, workflowID string, requestInfo map[string]any) error {
	agents, err := s.agentStates.GetAllForWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get agent states: %w", err)
	}
	policies := make(map[string]any, len(agents))
	for name, a := range agents {
		policies[name] = a.RuntimePolicy
	}
	requestInfo["agentRuntimePolicies"] = policies
	return nil
}

func (s *LifecycleService) CreateWorkflow(ctx context.Context, correlationID, workflowType, tenantID string, cfg domain.WorkflowConfig, version int) (*domain.Workflow, error) {
	s.log.Info("creating workflow", "correlation_id", correlationID, "workflow_type", workflowType, "tenant_id", tenantID)

	if cfg.RepeatEverySeconds > 0 && int(cfg.RepeatEverySeconds) < s.minRepeatSeconds {
		return nil, fmt.Errorf("%w: must be 0 (no repeat) or >= %d", ErrInvalidRepeatInterval, s.minRepeatSeconds)
	}

	id := uuid.New().String()
	workflow := domain.Workflow{
		ID:                     id,
		TenantID:               tenantID,
		WorkflowType:           workflowType,
		WorkflowConfig:         cfg,
		LifecycleState:         domain.LifecycleStateActive,
		WorkflowState:          domain.WorkflowStatePaused,
		LifecyclePolicy:        domain.WorkflowLifecyclePolicy{Rules: []domain.WorkflowLifecyclePolicyRule{}},
		CurrentWorkflowVersion: version,
		SchedulerPartitionID:   partitionForID(id, s.numPartitions),
		TargetVersion:          0,
		CurrentVersion:         0,
	}

	if err := s.workflows.Create(ctx, &workflow); err != nil {
		s.log.Error("failed to create workflow instance", "correlation_id", correlationID, "error", err)
		return nil, fmt.Errorf("create workflow: %w", err)
	}

	s.log.Info("workflow created and paused", "correlation_id", correlationID, "workflow_id", workflow.ID)
	return &workflow, nil
}

// InvokeWorkflow triggers a run and returns the created request's id — the run id
// the caller can use to correlate this invocation (e.g. with its trace).
func (s *LifecycleService) InvokeWorkflow(ctx context.Context, correlationID, workflowID string, params domain.WorkflowRuntimeParams) (string, error) {
	s.log.Info("invoking workflow", "correlation_id", correlationID, "workflow_id", workflowID)

	instance, err := s.workflows.Get(ctx, workflowID)
	if err != nil {
		return "", fmt.Errorf("get workflow instance: %w", err)
	}

	if instance.WorkflowState != domain.WorkflowStateActive {
		return "", fmt.Errorf("%w: workflow is in state %s", ErrInvalidWorkflowState, instance.WorkflowState)
	}

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	if err := s.ResolveRuntimeParams(params, instance, true, requestInfo); err != nil {
		return "", err
	}
	if err := s.ResolveAgentRuntimeParams(ctx, workflowID, requestInfo); err != nil {
		return "", err
	}
	if err := s.ResolveModelPricing(ctx, workflowID, requestInfo); err != nil {
		return "", err
	}

	request := domain.CustomerRequest{
		ID:                 uuid.New().String(),
		WorkflowID: workflowID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeInvoke,
		RequestInfo:        requestInfo,
	}

	// Atomically allocates the version, flips to invoking, and inserts the request.
	ver, err := s.customerRequests.CreateInvocationRequest(ctx, &request,
		[]domain.LifecycleState{domain.LifecycleStateDeleting, domain.LifecycleStateDeleted})
	if err != nil {
		s.log.Error("failed to create invoke request", "correlation_id", correlationID, "workflow_id", workflowID, "error", err)
		return "", fmt.Errorf("create invocation request: %w", err)
	}

	s.log.Info("invoke request created, attempting immediate schedule", "correlation_id", correlationID, "workflow_id", workflowID, "request_id", request.ID, "version", ver)
	if err := s.scheduler.ScheduleRequest(ctx, request.ID); err != nil {
		s.log.Warn("immediate schedule failed, scheduler will retry", "request_id", request.ID, "error", err)
	}

	return request.ID, nil
}

func (s *LifecycleService) DeleteWorkflow(ctx context.Context, correlationID, workflowID string) error {
	s.log.Info("deleting workflow", "correlation_id", correlationID, "workflow_id", workflowID)

	if _, err := s.workflows.Get(ctx, workflowID); err != nil {
		return fmt.Errorf("get workflow instance: %w", err)
	}

	if err := s.workflows.MarkDeleted(ctx, workflowID); err != nil {
		s.log.Error("failed to delete workflow", "correlation_id", correlationID, "workflow_id", workflowID, "error", err)
		return fmt.Errorf("delete workflow: %w", err)
	}

	s.log.Info("workflow deleted", "correlation_id", correlationID, "workflow_id", workflowID)
	return nil
}

func (s *LifecycleService) GetWorkflow(ctx context.Context, workflowID string) (*domain.Workflow, error) {
	s.log.Debug("getting workflow state", "workflow_id", workflowID)
	return s.workflows.Get(ctx, workflowID)
}

// ListWorkflows returns all workflows owned by the given tenant group, newest first.
// GetApprovalAudit returns a workflow's approval decisions (newest first).
func (s *LifecycleService) GetApprovalAudit(ctx context.Context, tenantGroupID, workflowID string) ([]domain.AuditEvent, error) {
	return s.audit.ListAuditEvents(ctx, tenantGroupID, workflowID, "", []domain.AuditEventType{domain.AuditEventApproval})
}

// GetApprovalAuditByID returns the single approval event for an approval_id, or nil.
func (s *LifecycleService) GetApprovalAuditByID(ctx context.Context, tenantGroupID, approvalID string) (*domain.AuditEvent, error) {
	return s.audit.GetApprovalByID(ctx, tenantGroupID, approvalID)
}

// GetWorkflowPolicyAudit returns a workflow's workflow-lifecycle policy firings.
func (s *LifecycleService) GetWorkflowPolicyAudit(ctx context.Context, tenantGroupID, workflowID string) ([]domain.AuditEvent, error) {
	return s.audit.ListAuditEvents(ctx, tenantGroupID, workflowID, "", []domain.AuditEventType{domain.AuditEventPolicyAction})
}

// GetAgentPolicyAudit returns a specific agent's lifecycle policy firings — agents
// are identified by (workflowID, agentName).
func (s *LifecycleService) GetAgentPolicyAudit(ctx context.Context, tenantGroupID, workflowID, agentName string) ([]domain.AuditEvent, error) {
	return s.audit.ListAuditEvents(ctx, tenantGroupID, workflowID, agentName, []domain.AuditEventType{domain.AuditEventAgentPolicyAction})
}

// GetAuditLog returns all audit events for the tenant, newest first; workflowID is
// optional (empty = whole tenant group).
func (s *LifecycleService) GetAuditLog(ctx context.Context, tenantGroupID, workflowID string) ([]domain.AuditEvent, error) {
	return s.audit.ListAuditEvents(ctx, tenantGroupID, workflowID, "", nil)
}

func (s *LifecycleService) ListWorkflows(ctx context.Context, tenantGroupID string) ([]*domain.Workflow, error) {
	s.log.Debug("listing workflows", "tenant_group_id", tenantGroupID)
	return s.workflows.ListForTenantGroup(ctx, tenantGroupID)
}

func (s *LifecycleService) SetAgentRuntimePolicy(ctx context.Context, workflowID, agentName string, policy map[string]any) error {
	s.log.Info("setting agent runtime policy", "workflow_id", workflowID, "agent", agentName)
	if err := s.agentStates.UpsertRuntimePolicy(ctx, workflowID, agentName, policy); err != nil {
		return fmt.Errorf("upsert agent runtime policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) SetAgentLifecyclePolicy(ctx context.Context, workflowID, agentName string, policy map[string]any) error {
	s.log.Info("setting agent lifecycle policy", "workflow_id", workflowID, "agent", agentName)
	if err := s.agentStates.UpsertLifecyclePolicy(ctx, workflowID, agentName, policy); err != nil {
		return fmt.Errorf("upsert agent lifecycle policy: %w", err)
	}
	return nil
}

func (s *LifecycleService) DeleteAgent(ctx context.Context, workflowID, agentName string) error {
	s.log.Info("deleting agent state", "workflow_id", workflowID, "agent", agentName)
	if err := s.agentStates.Delete(ctx, workflowID, agentName); err != nil {
		return fmt.Errorf("delete agent state: %w", err)
	}
	return nil
}

func (s *LifecycleService) SetWorkflowLifecyclePolicy(ctx context.Context, workflowID string, policy domain.WorkflowLifecyclePolicy) error {
	s.log.Info("setting workflow lifecycle policy", "workflow_id", workflowID)
	if err := s.workflows.UpdateLifecyclePolicy(ctx, workflowID, policy); err != nil {
		return fmt.Errorf("set workflow lifecycle policy: %w", err)
	}
	return nil
}

// GetWorkflowLifecyclePolicy returns the armed workflow-lifecycle policy (empty
// rules if none is set). Returns storage.ErrNotFound if the workflow doesn't exist.
func (s *LifecycleService) GetWorkflowLifecyclePolicy(ctx context.Context, workflowID string) (domain.WorkflowLifecyclePolicy, error) {
	wf, err := s.workflows.Get(ctx, workflowID)
	if err != nil {
		return domain.WorkflowLifecyclePolicy{}, err
	}
	return wf.LifecyclePolicy, nil
}

// GetAgentRuntimePolicy returns the armed runtime policy for one agent (nil if the
// agent has no state/policy set).
func (s *LifecycleService) GetAgentRuntimePolicy(ctx context.Context, workflowID, agentName string) (map[string]any, error) {
	states, err := s.agentStates.GetAllForWorkflow(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("get agent runtime policy: %w", err)
	}
	if st, ok := states[agentName]; ok {
		return st.RuntimePolicy, nil
	}
	return nil, nil
}

// GetAgentLifecyclePolicy returns the armed lifecycle policy for one agent (nil if
// the agent has no state/policy set).
func (s *LifecycleService) GetAgentLifecyclePolicy(ctx context.Context, workflowID, agentName string) (map[string]any, error) {
	states, err := s.agentStates.GetAllForWorkflow(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("get agent lifecycle policy: %w", err)
	}
	if st, ok := states[agentName]; ok {
		return st.LifecyclePolicy, nil
	}
	return nil, nil
}

func (s *LifecycleService) ActivateWorkflow(ctx context.Context, workflowID string) error {
	s.log.Info("activating workflow", "workflow_id", workflowID)
	if err := s.workflows.UpdateWorkflowState(ctx, workflowID, domain.WorkflowStateActive); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return err
		}
		return fmt.Errorf("activate workflow: %w", err)
	}
	return nil
}

// GetRequestInfo returns the full state of a single run (request) by id.
func (s *LifecycleService) GetRequestInfo(ctx context.Context, requestID string) (*domain.CustomerRequest, error) {
	req, err := s.customerRequests.Get(ctx, requestID)
	if err != nil {
		return nil, fmt.Errorf("get request info: %w", err)
	}
	return req, nil
}

// ListWorkflowRuns returns every run (request) for a workflow, newest first.
func (s *LifecycleService) ListWorkflowRuns(ctx context.Context, workflowID string) ([]*domain.CustomerRequest, error) {
	runs, err := s.customerRequests.ListForWorkflow(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("list workflow runs: %w", err)
	}
	return runs, nil
}

// ResolveInterruptedWorkflow flips an interrupted workflow back to active, but only if
// requestID matches the last_interrupted_request_id it is currently interrupted on — so a
// caller can't resolve past an interruption they haven't seen.
func (s *LifecycleService) ResolveInterruptedWorkflow(ctx context.Context, workflowID string, requestID string) error {
	s.log.Info("resolving interrupted workflow", "workflow_id", workflowID, "request_id", requestID)
	resolved, err := s.workflows.ResolveInterruptedWorkflow(ctx, workflowID, requestID)
	if err != nil {
		return fmt.Errorf("resolve interrupted workflow: %w", err)
	}
	if !resolved {
		return ErrNotInterrupted
	}
	return nil
}

func (s *LifecycleService) ApproveWorkflow(ctx context.Context, workflowID string, approvalID string, actor string) error {
	s.log.Info("approving workflow", "workflow_id", workflowID, "approval_id", approvalID, "actor", actor)
	resolved, info, err := s.approvalResolver.ApproveJob(ctx, workflowID, approvalID)
	if err != nil {
		return fmt.Errorf("approve workflow: %w", err)
	}
	if !resolved {
		return fmt.Errorf("%w: approval ID did not match or workflow is not awaiting approval", ErrInvalidWorkflowState)
	}
	s.recordApprovalDecision(ctx, workflowID, approvalID, actor, domain.ApprovalApproved, info)
	return nil
}

func (s *LifecycleService) RejectWorkflow(ctx context.Context, workflowID string, approvalID string, actor string) error {
	s.log.Info("rejecting workflow", "workflow_id", workflowID, "approval_id", approvalID, "actor", actor)
	resolved, info, err := s.approvalResolver.RejectJob(ctx, workflowID, approvalID)
	if err != nil {
		return fmt.Errorf("reject workflow: %w", err)
	}
	if !resolved {
		return fmt.Errorf("%w: approval ID did not match or workflow is not awaiting approval", ErrInvalidWorkflowState)
	}
	s.recordApprovalDecision(ctx, workflowID, approvalID, actor, domain.ApprovalRejected, info)
	return nil
}

// recordApprovalDecision appends the approval audit row after an explicit decision.
// Best-effort: the decision already committed, so a failed audit write is logged,
// not surfaced as an error (matches the timeout sweep's behavior).
func (s *LifecycleService) recordApprovalDecision(ctx context.Context, workflowID, approvalID, actor string, decision domain.ApprovalDecision, info domain.ResolvedApproval) {
	now := time.Now()
	details, err := json.Marshal(domain.ApprovalAuditDetails{
		ApprovalID: approvalID,
		OpenedAt:   info.OpenedAt,
		DecidedAt:  &now,
		Decision:   decision,
	})
	if err != nil {
		s.log.Error("failed to marshal approval audit details", "approval_id", approvalID, "error", err)
		return
	}
	if err := s.audit.Append(ctx, domain.AuditEvent{
		TenantGroupID: info.TenantGroupID,
		WorkflowID:    workflowID,
		RequestID:     info.RequestID,
		EventType:     domain.AuditEventApproval,
		Actor:         actor,
		OccurredAt:    now,
		Details:       details,
	}); err != nil {
		s.log.Error("failed to append approval audit", "approval_id", approvalID, "error", err)
	}
}

// TenantGroupIDForWorkflow returns the tenant_group_id that owns a workflow (single JOIN).
// Returns storage.ErrNotFound if the workflow does not exist.
func (s *LifecycleService) TenantGroupIDForWorkflow(ctx context.Context, workflowID string) (string, error) {
	return s.workflows.TenantGroupIDForWorkflow(ctx, workflowID)
}

// TenantGroupIDForTenant returns the tenant_group_id for a tenant.
// Returns storage.ErrNotFound if the tenant does not exist.
func (s *LifecycleService) TenantGroupIDForTenant(ctx context.Context, tenantID string) (string, error) {
	tenant, err := s.tenants.Get(ctx, tenantID)
	if err != nil {
		return "", err
	}
	return tenant.TenantGroupID, nil
}

func partitionForID(id string, numPartitions int) string {
	h := fnv.New32a()
	h.Write([]byte(id))
	return strconv.Itoa(int(h.Sum32()) % numPartitions)
}
