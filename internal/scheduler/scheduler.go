package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

type MetricsHandler interface {
	HandleAgentMetrics(ctx context.Context, requestID string, invocationMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics, workflow *domain.Workflow) (error, *domain.WorkflowVersionMetrics)
}

type PolicyResolver interface {
	ResolveLifecyclePolicy(ctx context.Context, workflow *domain.Workflow, versionMetrics *domain.WorkflowVersionMetrics) (*domain.PolicyActionDetails, error)
}

// PartitionWorker is a partition-scoped loop the scheduler owns. The scheduler starts each worker
// when it acquires a partition and cancels them when it loses it (see Run).
type PartitionWorker interface {
	Run(ctx context.Context, partitionID string) error
}

type Scheduler struct {
	id                         string
	interval                   int
	orphanedJobGracePeriodSecs int
	partitions                 storage.SchedulerPartitionRepository
	scheduler                  storage.SchedulerRepository
	requests                   storage.CustomerRequestRepository
	workflow                   storage.WorkflowRepository
	agentStates                storage.AgentStateRepository
	log                        *slog.Logger
	metricsHandler             MetricsHandler
	jobs                       storage.JobRepository
	policyResolver             PolicyResolver
	audit                      storage.AuditRepository
	partitionWorkers           []PartitionWorker
}

// Functions of the scheduler:
// 1, Grabs partition id from the partitions table, and manages the workflows belonging to that partition
// 2. Schedules unscheduled requests onto the job queue (picking priority by version number)
// 3. Checks for completed jobs, and updates current config state of the workflow and lifecycle state, then deletes the job

func NewScheduler(id string, interval int, orphanedJobGracePeriodSecs int, parts storage.SchedulerPartitionRepository, scheduler storage.SchedulerRepository, requests storage.CustomerRequestRepository, workflow storage.WorkflowRepository, agentStates storage.AgentStateRepository, jobs storage.JobRepository, metricsHandler MetricsHandler, policyResolver PolicyResolver, audit storage.AuditRepository, log *slog.Logger) *Scheduler {
	return &Scheduler{
		id:                         id,
		interval:                   interval,
		orphanedJobGracePeriodSecs: orphanedJobGracePeriodSecs,
		partitions:                 parts,
		scheduler:                  scheduler,
		requests:                   requests,
		workflow:                   workflow,
		agentStates:                agentStates,
		jobs:                       jobs,
		metricsHandler:             metricsHandler,
		policyResolver:             policyResolver,
		audit:                      audit,
		log:                        log.With("component", "scheduler", "scheduler_id", id),
	}
}

// SetPartitionWorkers registers the partition-scoped workers the scheduler starts/stops as it
// gains/loses its partition. Set after construction to break the scheduler↔worker reference cycle
// (e.g. the periodic handler needs a *Scheduler to schedule requests).
func (s *Scheduler) SetPartitionWorkers(workers ...PartitionWorker) {
	s.partitionWorkers = workers
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.log.Info("scheduler starting", "interval_seconds", s.interval)

	ticker := time.NewTicker(time.Duration(s.interval) * time.Second)
	leaseTime := time.Duration(s.interval)*time.Second + (2 * time.Second)

	for {
		partition, err := s.partitions.AcquireAvailable(ctx, s.id, leaseTime)
		if err != nil {
			s.log.Error("failed to acquire partition", "error", err)
		} else if partition != nil {
			s.log.Info("partition acquired", "partition_id", partition.ID)
			if cancelled := s.runPartition(ctx, partition, ticker, leaseTime); cancelled {
				return nil
			}
		} else {
			s.log.Debug("no partition available, retrying in 10s")
		}

		time.Sleep(time.Second * 10)
	}
}

// runPartition owns an acquired partition: it starts the partition-scoped workers (lifecycle
// resolver, periodic handler) under a child context, runs the scheduling tick until the lease is
// lost or ctx is cancelled, then cancels the workers and waits for them to stop. Returns true if
// ctx was cancelled (caller should terminate), false if the lease was lost (caller should retry).
func (s *Scheduler) runPartition(ctx context.Context, partition *domain.SchedulerPartition, ticker *time.Ticker, leaseTime time.Duration) (cancelled bool) {
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	var workerWg sync.WaitGroup
	for _, w := range s.partitionWorkers {
		workerWg.Add(1)
		go func(w PartitionWorker) {
			defer workerWg.Done()
			if err := w.Run(workerCtx, partition.ID); err != nil {
				s.log.Error("partition worker exited with error", "partition_id", partition.ID, "error", err)
			}
		}(w)
	}
	defer func() {
		cancelWorkers()
		workerWg.Wait()
	}()

	ticker.Reset(time.Duration(s.interval) * time.Second)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("context cancelled, releasing partition", "partition_id", partition.ID)
			s.partitions.Release(ctx, partition.ID, s.id)
			return true
		case <-ticker.C:
			s.log.Debug("tick", "partition_id", partition.ID)

			var wg sync.WaitGroup
			wg.Add(4)
			go func() {
				defer wg.Done()
				s.failJobs(ctx, partition.ID)
			}()
			go func() {
				defer wg.Done()
				s.completeJobs(ctx, partition.ID)
			}()
			go func() {
				defer wg.Done()
				s.reconcileWorkflowLifecycles(ctx, partition.ID)
			}()
			go func() {
				defer wg.Done()
				s.markOrphanedJobsFailed(ctx, partition.ID)
			}()
			wg.Wait()
			s.scheduleJobs(ctx, partition.ID)

			renewed, err := s.partitions.Renew(ctx, partition.ID, s.id, leaseTime)
			if err != nil {
				s.log.Error("error renewing partition lease, releasing", "partition_id", partition.ID, "error", err)
				return false
			}
			if !renewed {
				s.log.Warn("partition lease not renewed, another scheduler may have taken it", "partition_id", partition.ID)
				return false
			}
			s.log.Debug("partition lease renewed", "partition_id", partition.ID)
			ticker.Reset(time.Duration(s.interval) * time.Second)
		}
	}
}

func (s *Scheduler) ApproveJob(ctx context.Context, workflowID string, approvalID string) (bool, domain.ResolvedApproval, error) {
	return s.jobs.ResolveApproval(ctx, workflowID, approvalID, domain.JobStatusApproved)
}

func (s *Scheduler) RejectJob(ctx context.Context, workflowID string, approvalID string) (bool, domain.ResolvedApproval, error) {
	return s.jobs.ResolveApproval(ctx, workflowID, approvalID, domain.JobStatusRejected)
}

func (s *Scheduler) AnswerJob(ctx context.Context, workflowID string, inputID string, answer map[string]any) (bool, domain.ResolvedInput, error) {
	return s.jobs.ResolveInput(ctx, workflowID, inputID, answer)
}

// recordPolicyAction appends the audit row for a lifecycle-policy firing. Best-effort:
// the policy already applied, so a failed audit write is logged, not surfaced.
func (s *Scheduler) recordPolicyAction(ctx context.Context, workflow *domain.Workflow, requestID string, action *domain.PolicyActionDetails) {
	groupID, err := s.workflow.TenantGroupIDForWorkflow(ctx, workflow.ID)
	if err != nil {
		s.log.Error("failed to resolve tenant group for policy audit", "workflow_id", workflow.ID, "error", err)
		return
	}
	details, err := json.Marshal(action)
	if err != nil {
		s.log.Error("failed to marshal policy action details", "workflow_id", workflow.ID, "error", err)
		return
	}
	if err := s.audit.Append(ctx, domain.AuditEvent{
		TenantGroupID: groupID,
		WorkflowID:    workflow.ID,
		RequestID:     requestID,
		EventType:     domain.AuditEventPolicyAction,
		Actor:         "system",
		OccurredAt:    time.Now(),
		Details:       details,
	}); err != nil {
		s.log.Error("failed to append policy action audit", "workflow_id", workflow.ID, "error", err)
	}
}

func (s *Scheduler) MarkInvoking(ctx context.Context, workflowID string) error {
	return s.scheduler.MarkWorkflowInvoking(ctx, workflowID)
}

func (s *Scheduler) MarkAwaitingApproval(ctx context.Context, workflowID string) error {
	return s.scheduler.MarkWorkflowAwaitingApproval(ctx, workflowID)
}

func (s *Scheduler) MarkAwaitingInput(ctx context.Context, workflowID string) error {
	return s.scheduler.MarkWorkflowAwaitingInput(ctx, workflowID)
}

func (s *Scheduler) markOrphanedJobsFailed(ctx context.Context, partitionID string) {
	count, err := s.jobs.MarkOrphanedJobsFailed(ctx, partitionID, s.orphanedJobGracePeriodSecs)
	if err != nil {
		s.log.Error("failed to mark orphaned jobs as failed", "partition_id", partitionID, "error", err)
		return
	}
	if count > 0 {
		s.log.Info("marked orphaned jobs as failed", "count", count, "partition_id", partitionID)
	}
}

// blockedAfterSecs: how long a run may sit unowned before it's reported blocked. TODO: config.
const blockedAfterSecs = 60

func (s *Scheduler) reconcileWorkflowLifecycles(ctx context.Context, partitionID string) {
	reconciled, err := s.scheduler.ReconcileWorkflowLifecycles(ctx, partitionID, blockedAfterSecs)
	if err != nil {
		s.log.Error("failed to reconcile workflow lifecycles", "partition_id", partitionID, "error", err)
		return
	}
	if len(reconciled) > 0 {
		s.log.Info("reconciled workflow lifecycle states", "partition_id", partitionID, "count", len(reconciled), "workflow_ids", reconciled)
	}
}

func (s *Scheduler) failJobs(ctx context.Context, partitionID string) error {
	jobs, err := s.scheduler.GetFailedJobs(ctx, partitionID)
	if err != nil {
		s.log.Error("failed to get failed jobs", "partition_id", partitionID, "error", err)
		return fmt.Errorf("get failed jobs error: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	s.log.Info("processing failed jobs", "partition_id", partitionID, "count", len(jobs))

	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job domain.FailedJob) {
			defer wg.Done()
			if _, err := s.FailRequest(ctx, job.RequestID, job.FailureReason); err != nil {
				s.log.Error("failed to process failed request", "request_id", job.RequestID, "error", err)
			}
		}(job)
	}
	wg.Wait()
	return nil
}

// interruptedReason is the static run_outcome reason recorded whenever a request is
// failed. A failed request is always a platform interruption (the worker was lost, an
// internal error occurred, etc.); customer-domain failures complete instead.
const interruptedReason = "the run was interrupted before it could complete (platform failure)"

func (s *Scheduler) FailRequest(ctx context.Context, req string, reason string) (bool, error) {
	s.log.Debug("marking request as failed", "request_id", req)

	if reason == "" {
		reason = interruptedReason
	}
	request, err := s.requests.FailRequest(ctx, req, reason)
	if err != nil {
		return false, fmt.Errorf("error failing request %s: %w", req, err)
	}

	applied, err := s.workflow.ApplyFailedJob(ctx, request.WorkflowID, req, domain.LifecycleStateInterrupted, domain.WorkflowStateDisabled, request.Version)
	if err != nil {
		s.log.Error("failed to apply failed job to workflow", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return false, err
	}
	if applied {
		s.log.Info("request failed, workflow updated", "request_id", req, "workflow_id", request.WorkflowID, "version", request.Version)
	} else {
		s.log.Warn("request failed but workflow version check skipped update (newer version already applied)", "request_id", req, "workflow_id", request.WorkflowID, "version", request.Version)
	}

	if _, err := s.scheduler.DeleteTerminalJob(ctx, request.WorkflowID, req); err != nil {
		s.log.Error("failed to delete terminal job", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
	}
	return applied, nil
}

func (s *Scheduler) completeJobs(ctx context.Context, partitionID string) error {
	jobs, err := s.scheduler.GetCompletedJobs(ctx, partitionID)
	if err != nil {
		s.log.Error("failed to get completed jobs", "partition_id", partitionID, "error", err)
		return fmt.Errorf("get completed jobs error: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	s.log.Info("processing completed jobs", "partition_id", partitionID, "count", len(jobs))

	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job domain.CompletedJob) {
			defer wg.Done()
			// Transfer the run result the worker recorded on the job onto the request.
			if _, err := s.CompleteRequest(ctx, job.RequestID, job.ResultType, job.FailureReason, job.Result); err != nil {
				s.log.Error("failed to process completed request", "request_id", job.RequestID, "error", err)
			}
		}(job)
	}
	wg.Wait()
	return nil
}

// For now this is idempotent, in the future we can have more fine grained lifecycle states to avoid redundant stuff
func (s *Scheduler) CompleteRequest(ctx context.Context, req string, outcome domain.RunOutcome, reason string, result map[string]any) (bool, error) {
	s.log.Debug("marking request as completed", "request_id", req, "outcome", outcome)

	request, err := s.requests.CompleteRequest(ctx, req, outcome, reason, result)
	if err != nil {
		return false, fmt.Errorf("error completing request %s: %w", req, err)
	}

	var lifecycleState domain.LifecycleState
	switch request.RequestType {
	case domain.CustomerRequestTypeCreate:
		lifecycleState = domain.LifecycleStateActive
	case domain.CustomerRequestTypeInvoke:
		lifecycleState = domain.LifecycleStateActive
	case domain.CustomerRequestTypeDelete:
		lifecycleState = domain.LifecycleStateDeleted
	}

	applied, err := s.workflow.ApplyCompletedJob(ctx, request.WorkflowID, lifecycleState, request.Version)
	if err != nil {
		s.log.Error("failed to apply completed job to workflow", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return false, err
	}
	if applied {
		s.log.Info("request completed, workflow updated", "request_id", req, "workflow_id", request.WorkflowID, "request_type", request.RequestType, "lifecycle_state", lifecycleState, "version", request.Version)
	} else {
		s.log.Warn("request completed but workflow version check skipped update (newer version already applied)", "request_id", req, "workflow_id", request.WorkflowID, "version", request.Version)
	}

	// get the accumulated metrics for this job
	metrics, workflowMetrics, err := s.jobs.GetJobMetrics(ctx, request.WorkflowID, request.ID)
	if err != nil {
		s.log.Error("failed to get job metrics", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return false, err
	}

	workflow, err := s.workflow.Get(ctx, request.WorkflowID)
	if err != nil {
		s.log.Error("failed to get workflow instance for metrics handling", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return false, err
	}

	// update metrics in storage
	var versionMetrics *domain.WorkflowVersionMetrics
	err, versionMetrics = s.metricsHandler.HandleAgentMetrics(ctx, req, metrics, workflowMetrics, workflow)
	if err != nil {
		s.log.Error("failed to handle agent metrics", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return false, err
	}

	// resolve policy
	policyAction, err := s.policyResolver.ResolveLifecyclePolicy(ctx, workflow, versionMetrics)
	if err != nil {
		s.log.Error("failed to resolve lifecycle policy", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return false, err
	}
	if policyAction != nil {
		s.recordPolicyAction(ctx, workflow, req, policyAction)
	}

	if _, err := s.scheduler.DeleteTerminalJob(ctx, request.WorkflowID, req); err != nil {
		s.log.Error("failed to delete terminal job", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
	}
	return applied, nil
}

func (s *Scheduler) scheduleJobs(ctx context.Context, partitionID string) error {
	reqs, err := s.scheduler.GetTopUnscheduledRequests(ctx, partitionID)
	if err != nil {
		s.log.Error("failed to get unscheduled requests", "partition_id", partitionID, "error", err)
		return fmt.Errorf("get unscheduled jobs error: %w", err)
	}

	if len(reqs) == 0 {
		s.log.Debug("no unscheduled requests", "partition_id", partitionID)
		return nil
	}

	s.log.Info("scheduling requests", "partition_id", partitionID, "count", len(reqs))

	var wg sync.WaitGroup
	for _, req := range reqs {
		wg.Add(1)
		go func(req string) {
			defer wg.Done()
			if err := s.ScheduleRequest(ctx, req); err != nil {
				s.log.Error("failed to schedule request", "request_id", req, "error", err)
			}
		}(req)
	}
	wg.Wait()
	return nil
}

func (s *Scheduler) validateWorkflowState(workflow *domain.Workflow) bool {

	if workflow.WorkflowState != domain.WorkflowStateActive {
		s.log.Debug("skipping schedule: workflow not active", "workflow_id", workflow.ID, "workflow_state", workflow.WorkflowState)
		return false
	}

	if workflow.LifecycleLastResolved != workflow.CurrentVersion {
		s.log.Debug("skipping schedule: metrics not resolved up to current run", "workflow_id", workflow.ID, "lifecycle_last_resolved", workflow.LifecycleLastResolved, "current_version", workflow.CurrentVersion)
		return false
	}

	return true
}

func (s *Scheduler) ScheduleRequest(ctx context.Context, req string) error {
	s.log.Debug("attempting to schedule request", "request_id", req)

	request, err := s.requests.Get(ctx, req)
	if err != nil {
		return fmt.Errorf("get customer request %s: %w", req, err)
	}

	workflow, err := s.workflow.Get(ctx, request.WorkflowID)
	if err != nil {
		s.log.Error("failed to get workflow instance for scheduling", "request_id", req, "workflow_id", request.WorkflowID, "error", err)
		return fmt.Errorf("get workflow instance %s: %w", request.WorkflowID, err)
	}

	if !s.validateWorkflowState(workflow) {
		s.log.Debug("request not scheduled, workflow state validation failed", "request_id", req, "workflow_id", request.WorkflowID)
		return fmt.Errorf("workflow validation failed")
	}

	agentStates, err := s.agentStates.GetAllForWorkflow(ctx, request.WorkflowID)
	if err != nil {
		return fmt.Errorf("get agent states for workflow %s: %w", request.WorkflowID, err)
	}

	// Build the full job context in memory: start from the snapshotted request info
	// (which already carries agentRuntimePolicies, initialVersion, operationTimeoutSeconds)
	// then layer in live agent lifecycle policies and metrics.
	jobContext := maps.Clone(request.RequestInfo)
	if len(agentStates) > 0 {
		agentStateMap := make(map[string]any, len(agentStates))
		for name, a := range agentStates {
			agentStateMap[name] = map[string]any{
				"lifecycle_policy":   a.LifecyclePolicy,
				"invocation_metrics": a.InvocationMetrics,
			}
		}
		jobContext["agentStates"] = agentStateMap
	}

	contextJSON, err := json.Marshal(jobContext)
	if err != nil {
		return fmt.Errorf("marshal job context: %w", err)
	}

	currentAtomicOperation := string(request.RequestType) + "_entry"

	// Both values are always float64 after JSON unmarshal from JSONB.
	timeoutSeconds := int(request.RequestInfo["operationTimeoutSeconds"].(float64))

	invokeMode := workflow.WorkflowConfig.InvokeMode
	if invokeMode == "" {
		invokeMode = domain.InvokeModeCoalesce
	}

	workflowID, ver, written, err := s.scheduler.UpsertJobAndSchedule(ctx, req, string(contextJSON), currentAtomicOperation, timeoutSeconds, workflow.CurrentWorkflowVersion, workflow.CurrentVersion, string(invokeMode))
	if err != nil {
		return fmt.Errorf("error scheduling request %s: %w", req, err)
	}

	if !written {
		s.log.Debug("request not scheduled, slot occupied or newer version present", "request_id", req)
		return nil
	}

	s.log.Info("request scheduled", "request_id", req, "workflow_id", workflowID, "version", ver)

	// Queue mode keeps every request alive (drains oldest-first); only coalesce drops the
	// older pending ones in favor of the one just scheduled.
	if invokeMode != domain.InvokeModeQueue {
		if err := s.scheduler.SupercedeOlderRequests(ctx, workflowID, ver); err != nil {
			return fmt.Errorf("error with supercede requests with request %s: %w", req, err)
		}
		s.log.Debug("older requests superceded", "workflow_id", workflowID, "version", ver)
	}

	// Best-effort: a run is now scheduled (the worker flips it to invoking on dispatch; the
	// sweep reconciles either if lost).
	if err := s.scheduler.MarkWorkflowScheduled(ctx, workflowID); err != nil {
		s.log.Warn("failed to mark workflow scheduled, sweep will reconcile", "workflow_id", workflowID, "error", err)
	}

	return nil
}
