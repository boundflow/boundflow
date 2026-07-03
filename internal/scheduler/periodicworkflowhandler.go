package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage"
)

type PeriodicWorkflowHandler struct {
	interval      int
	log           *slog.Logger
	scheduler     *Scheduler
	lifecycle     *service.LifecycleService
	schedulerRepo storage.SchedulerRepository
	requests      storage.CustomerRequestRepository
}

func NewPeriodicWorkflowHandler(
	interval int,
	log *slog.Logger,
	scheduler *Scheduler,
	lifecycle *service.LifecycleService,
	schedulerRepo storage.SchedulerRepository,
	requests storage.CustomerRequestRepository,
) *PeriodicWorkflowHandler {
	return &PeriodicWorkflowHandler{
		interval:      interval,
		log:           log.With("component", "periodic_workflow_handler"),
		scheduler:     scheduler,
		lifecycle:     lifecycle,
		schedulerRepo: schedulerRepo,
		requests:      requests,
	}
}

func (r *PeriodicWorkflowHandler) Run(ctx context.Context, partitionID string) error {
	r.log.Info("periodic workflow handler starting", "partition_id", partitionID)
	ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			workflows, err := r.schedulerRepo.GetDuePeriodicWorkflows(ctx, partitionID)
			if err != nil {
				r.log.Error("failed to get due periodic workflows", "partition_id", partitionID, "error", err)
				continue
			}
			var wg sync.WaitGroup
			for _, wf := range workflows {
				wg.Add(1)
				go func(wf *domain.Workflow) {
					defer wg.Done()
					err := r.createPeriodicRequest(ctx, wf)
					if err != nil {
						r.log.Error("failed to create periodic request", "workflow_id", wf.ID, "error", err)
					}
				}(wf)
			}
			wg.Wait()
		case <-ctx.Done():
			return nil
		}
	}
}

func (r *PeriodicWorkflowHandler) createPeriodicRequest(ctx context.Context, workflow *domain.Workflow) error {

	correlationID := uuid.New().String()

	requestInfo := map[string]any{
		"correlationId": correlationID,
	}

	err := r.lifecycle.ResolveRuntimeParams(domain.WorkflowRuntimeParams{}, workflow, false, requestInfo)
	if err != nil {
		r.log.Error("failed to resolve runtime params", "correlation_id", correlationID, "workflow_id", workflow.ID, "error", err)
		return err
	}

	err = r.lifecycle.ResolveAgentRuntimeParams(ctx, workflow.ID, requestInfo)
	if err != nil {
		r.log.Error("failed to resolve agent runtime params", "correlation_id", correlationID, "workflow_id", workflow.ID, "error", err)
		return err
	}

	request := domain.CustomerRequest{
		ID:                 uuid.New().String(),
		WorkflowID: workflow.ID,
		Status:             domain.CustomerRequestStatusUnscheduled,
		RequestType:        domain.CustomerRequestTypeInvoke,
		RequestInfo:        requestInfo,
	}

	// Atomically guards (gap + no in-flight request + valid state), allocates the version,
	// flips to invoking, and inserts — all-or-nothing.
	ver, created, err := r.requests.CreateDuePeriodicRequest(ctx, &request,
		time.Duration(workflow.WorkflowConfig.RepeatEverySeconds)*time.Second,
		[]domain.LifecycleState{domain.LifecycleStateDeleting, domain.LifecycleStateDeleted})
	if err != nil {
		r.log.Error("failed to create periodic request", "correlation_id", correlationID, "workflow_id", workflow.ID, "error", err)
		return err
	}

	if !created {
		r.log.Debug("periodic request not created (gap not elapsed or request already in flight)", "correlation_id", correlationID, "workflow_id", workflow.ID)
		return nil
	}

	r.log.Info("invoke request created, attempting immediate schedule", "correlation_id", correlationID, "workflow_id", workflow.ID, "request_id", request.ID, "version", ver)
	if err := r.scheduler.ScheduleRequest(ctx, request.ID); err != nil {
		r.log.Warn("immediate schedule failed, scheduler will retry", "request_id", request.ID, "error", err)
	}

	return nil
}
