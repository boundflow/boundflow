package scheduler

import (
	"github.com/convergeplane/convergeplane/internal/storage"
)

type Scheduler struct {
	id         string
	interval   int
	partitions storage.SchedulerPartitionRepository
}

// Functions of the scheduler:
// 1, Grabs partition id from the partitions table, and manages the resources belonging to that partition
// 2. Schedules unscheduled requests onto the job queue (picking priority by version number)
// 3. Checks for completed jobs, and updates current config state of the resource and lifecycle state, then deletes the job

func New(id string, interval int, parts storage.SchedulerPartitionRepository) *Scheduler {
	return &Scheduler{
		id:         id,
		interval:   interval,
		partitions: parts,
	}
}

/*func (s *Scheduler) Run(ctx context.Context) error {

	ticker := time.NewTicker(time.Duration(s.interval))
	leaseTime := time.Duration(s.interval) * time.Second + (2 * time.Second) // give a two second buffer

	for {
		partition,err := s.partitions.AcquireAvailable(ctx, s.id, leaseTime)
		if partition != nil && err == nil {
			ticker.Reset(time.Duration(s.interval))
			for {
				select {
				case <-ctx.Done():
					s.partitions.Release(ctx, partition.ID, s.id)
				case <-ticker.C:

				}
			}
		}

	}

	return nil
}*/

//func (s *Scheduler) scheduleJobs(ctx context.Context, partitionId int, )
