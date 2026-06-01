package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type MetricsHandler interface {
	HandleAgentMetrics(ctx context.Context, invocationMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workflow *domain.ResourceInstance) (error, *domain.WorkflowVersionMetrics)
}

type PolicyResolver interface {
	ResolveLifecyclePolicy(ctx context.Context, workflow *domain.ResourceInstance, versionMetrics *domain.WorkflowVersionMetrics) error
}

type Scheduler struct {
	id             string
	interval       int
	partitions     storage.SchedulerPartitionRepository
	scheduler      storage.SchedulerRepository
	requests       storage.CustomerRequestRepository
	resource       storage.ResourceInstanceRepository
	agentStates    storage.AgentStateRepository
	log            *slog.Logger
	metricsHandler MetricsHandler
	jobs           storage.JobRepository
	policyResolver PolicyResolver
}

// Functions of the scheduler:
// 1, Grabs partition id from the partitions table, and manages the resources belonging to that partition
// 2. Schedules unscheduled requests onto the job queue (picking priority by version number)
// 3. Checks for completed jobs, and updates current config state of the resource and lifecycle state, then deletes the job

func NewScheduler(id string, interval int, parts storage.SchedulerPartitionRepository, scheduler storage.SchedulerRepository, requests storage.CustomerRequestRepository, resource storage.ResourceInstanceRepository, agentStates storage.AgentStateRepository, jobs storage.JobRepository, metricsHandler MetricsHandler, policyResolver PolicyResolver, log *slog.Logger) *Scheduler {
	return &Scheduler{
		id:             id,
		interval:       interval,
		partitions:     parts,
		scheduler:      scheduler,
		requests:       requests,
		resource:       resource,
		agentStates:    agentStates,
		jobs:           jobs,
		metricsHandler: metricsHandler,
		policyResolver: policyResolver,
		log:            log.With("component", "scheduler", "scheduler_id", id),
	}
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
			ticker.Reset(time.Duration(s.interval) * time.Second)

		partitionLoop:
			for {
				select {
				case <-ctx.Done():
					s.log.Info("context cancelled, releasing partition", "partition_id", partition.ID)
					s.partitions.Release(ctx, partition.ID, s.id)
					return nil
				case <-ticker.C:
					s.log.Debug("tick", "partition_id", partition.ID)

					var wg sync.WaitGroup
					wg.Add(2)
					go func() {
						defer wg.Done()
						s.failJobs(ctx, partition.ID)
					}()
					go func() {
						defer wg.Done()
						s.completeJobs(ctx, partition.ID)
					}()
					wg.Wait()
					s.scheduleJobs(ctx, partition.ID)

					renewed, err := s.partitions.Renew(ctx, partition.ID, s.id, leaseTime)
					if err != nil {
						s.log.Error("error renewing partition lease, releasing", "partition_id", partition.ID, "error", err)
						break partitionLoop
					}
					if !renewed {
						s.log.Warn("partition lease not renewed, another scheduler may have taken it", "partition_id", partition.ID)
						break partitionLoop
					}
					s.log.Debug("partition lease renewed", "partition_id", partition.ID)
					ticker.Reset(time.Duration(s.interval) * time.Second)
				}
			}
		} else {
			s.log.Debug("no partition available, retrying in 10s")
		}

		time.Sleep(time.Second * 10)
	}
}

func (s *Scheduler) failJobs(ctx context.Context, partitionID string) error {
	reqs, err := s.scheduler.GetFailedJobRequestIDs(ctx, partitionID)
	if err != nil {
		s.log.Error("failed to get failed job request IDs", "partition_id", partitionID, "error", err)
		return fmt.Errorf("get failed jobs error: %w", err)
	}

	if len(reqs) == 0 {
		return nil
	}

	s.log.Info("processing failed jobs", "partition_id", partitionID, "count", len(reqs))

	var wg sync.WaitGroup
	for _, req := range reqs {
		wg.Add(1)
		go func(req string) {
			defer wg.Done()
			if _, err := s.FailRequest(ctx, req); err != nil {
				s.log.Error("failed to process failed request", "request_id", req, "error", err)
			}
		}(req)
	}
	wg.Wait()
	return nil
}

func (s *Scheduler) FailRequest(ctx context.Context, req string) (bool, error) {
	s.log.Debug("marking request as failed", "request_id", req)

	request, err := s.requests.FailRequest(ctx, req)
	if err != nil {
		return false, fmt.Errorf("error failing request %s: %w", req, err)
	}

	applied, err := s.resource.ApplyCompletedJob(ctx, request.ResourceInstanceID, domain.LifecycleStateFailed, request.Version)
	if err != nil {
		s.log.Error("failed to apply failed job to resource", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
		return false, err
	}
	if applied {
		s.log.Info("request failed, resource updated", "request_id", req, "resource_id", request.ResourceInstanceID, "version", request.Version)
	} else {
		s.log.Warn("request failed but resource version check skipped update (newer version already applied)", "request_id", req, "resource_id", request.ResourceInstanceID, "version", request.Version)
	}

	if _, err := s.scheduler.DeleteTerminalJob(ctx, request.ResourceInstanceID, req); err != nil {
		s.log.Error("failed to delete terminal job", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
	}
	return applied, nil
}

func (s *Scheduler) completeJobs(ctx context.Context, partitionID string) error {
	reqs, err := s.scheduler.GetCompletedJobRequestIDs(ctx, partitionID)
	if err != nil {
		s.log.Error("failed to get completed job request IDs", "partition_id", partitionID, "error", err)
		return fmt.Errorf("get completed jobs error: %w", err)
	}

	if len(reqs) == 0 {
		return nil
	}

	s.log.Info("processing completed jobs", "partition_id", partitionID, "count", len(reqs))

	var wg sync.WaitGroup
	for _, req := range reqs {
		wg.Add(1)
		go func(req string) {
			defer wg.Done()
			if _, err := s.CompleteRequest(ctx, req); err != nil {
				s.log.Error("failed to process completed request", "request_id", req, "error", err)
			}
		}(req)
	}
	wg.Wait()
	return nil
}

// For now this is idempotent, in the future we can have more fine grained lifecycle states to avoid redundant stuff
func (s *Scheduler) CompleteRequest(ctx context.Context, req string) (bool, error) {
	s.log.Debug("marking request as completed", "request_id", req)

	request, err := s.requests.CompleteRequest(ctx, req)
	if err != nil {
		return false, fmt.Errorf("error completing request %s: %w", req, err)
	}

	var lifecycleState domain.LifecycleState
	switch request.RequestType {
	case domain.CustomerRequestTypeCreate:
		lifecycleState = domain.LifecycleStateActive
	case domain.CustomerRequestTypeReconcile:
		lifecycleState = domain.LifecycleStateActive
	case domain.CustomerRequestTypeDelete:
		lifecycleState = domain.LifecycleStateDeleted
	}

	applied, err := s.resource.ApplyCompletedJob(ctx, request.ResourceInstanceID, lifecycleState, request.Version)
	if err != nil {
		s.log.Error("failed to apply completed job to resource", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
		return false, err
	}
	if applied {
		s.log.Info("request completed, resource updated", "request_id", req, "resource_id", request.ResourceInstanceID, "request_type", request.RequestType, "lifecycle_state", lifecycleState, "version", request.Version)
	} else {
		s.log.Warn("request completed but resource version check skipped update (newer version already applied)", "request_id", req, "resource_id", request.ResourceInstanceID, "version", request.Version)
	}

	// get the agentMetrics
	metrics, err := s.jobs.GetAgentMetrics(ctx, request.ResourceInstanceID, request.ID)
	if err != nil {
		s.log.Error("failed to get agent metrics from job", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
		return false, err
	}

	workflow, err := s.resource.Get(ctx, request.ResourceInstanceID)
	if err != nil {
		s.log.Error("failed to get resource instance for metrics handling", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
		return false, err
	}

	// update metrics in storage
	var versionMetrics *domain.WorkflowVersionMetrics
	err, versionMetrics = s.metricsHandler.HandleAgentMetrics(ctx, metrics, workflow)
	if err != nil {
		s.log.Error("failed to handle agent metrics", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
		return false, err
	}

	// resolve policy
	err = s.policyResolver.ResolveLifecyclePolicy(ctx, workflow, versionMetrics)
	if err != nil {
		s.log.Error("failed to resolve lifecycle policy", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
		return false, err
	}

	if _, err := s.scheduler.DeleteTerminalJob(ctx, request.ResourceInstanceID, req); err != nil {
		s.log.Error("failed to delete terminal job", "request_id", req, "resource_id", request.ResourceInstanceID, "error", err)
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

func (s *Scheduler) validateWorkflowState(workflow *domain.ResourceInstance) bool {

	if workflow.WorkflowState != domain.WorkflowStateActive {
		// log
		return false
	}

	if workflow.LifecycleLastResolved != workflow.CurrentVersion {
		// log
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

	workflow, err := s.resource.Get(ctx, request.ResourceInstanceID)
	if err != nil {
		// log error
		return err
	}

	if s.validateWorkflowState(workflow) == false {
		// log error
		return fmt.Errorf("Workflow validation failed")
	}

	agentStates, err := s.agentStates.GetAllForResource(ctx, request.ResourceInstanceID)
	if err != nil {
		return fmt.Errorf("get agent states for resource %s: %w", request.ResourceInstanceID, err)
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

	resourceID, ver, written, err := s.scheduler.UpsertJobAndSchedule(ctx, req, string(contextJSON), currentAtomicOperation, timeoutSeconds, workflow.CurrentWorkflowVersion, workflow.CurrentVersion)
	if err != nil {
		return fmt.Errorf("error scheduling request %s: %w", req, err)
	}

	if !written {
		s.log.Debug("request not scheduled, existing job has equal or higher version", "request_id", req)
		return nil
	}

	s.log.Info("request scheduled", "request_id", req, "resource_id", resourceID, "version", ver)

	if err := s.scheduler.SupercedeOlderRequests(ctx, resourceID, ver); err != nil {
		return fmt.Errorf("error with supercede requests with request %s: %w", req, err)
	}
	s.log.Debug("older requests superceded", "resource_id", resourceID, "version", ver)

	return nil
}

func (s *Scheduler) UpdateAgentMetrics(ctx context.Context, resourceInstanceID string, updates map[string][]*convergeplanev1.AgentInvocationMetrics) error {
	for agentName, metrics := range updates {
		if err := s.agentStates.UpdateMetrics(ctx, resourceInstanceID, agentName, metrics); err != nil {
			s.log.Warn("failed to update agent metrics", "resource_id", resourceInstanceID, "agent", agentName, "error", err)
		}
	}
	return nil
}
