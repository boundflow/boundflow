package convert

import (
	"google.golang.org/protobuf/types/known/durationpb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

func PolicySetFromProto(pb *boundflowv1.PolicySet) domain.PolicySet {
	if pb == nil {
		return domain.PolicySet{}
	}

	ps := domain.PolicySet{}

	if pb.Concurrency != nil {
		ps.Concurrency = domain.ConcurrencyPolicy{
			MaxConcurrentOperations: pb.Concurrency.MaxConcurrentOperations,
		}
	}

	if pb.Failure != nil {
		ps.Failure = domain.FailurePolicy{
			EnableRollback: pb.Failure.EnableRollback,
		}
		if pb.Failure.Retry != nil {
			ps.Failure.Retry = domain.RetryPolicy{
				MaxRetries:        pb.Failure.Retry.MaxRetries,
				BackoffMultiplier: pb.Failure.Retry.BackoffMultiplier,
			}
			if pb.Failure.Retry.InitialBackoff != nil {
				ps.Failure.Retry.InitialBackoff = pb.Failure.Retry.InitialBackoff.AsDuration()
			}
			if pb.Failure.Retry.MaxBackoff != nil {
				ps.Failure.Retry.MaxBackoff = pb.Failure.Retry.MaxBackoff.AsDuration()
			}
		}
	}

	if pb.MaintenanceWindow != nil {
		for _, w := range pb.MaintenanceWindow.Windows {
			mw := domain.MaintenanceWindow{
				CronExpression: w.CronExpression,
			}
			if w.Duration != nil {
				mw.Duration = w.Duration.AsDuration()
			}
			ps.MaintenanceWindow.Windows = append(ps.MaintenanceWindow.Windows, mw)
		}
	}

	if pb.Upgrade != nil {
		ps.Upgrade = domain.UpgradePolicy{
			AutoPause: pb.Upgrade.AutoPause,
		}
		if pb.Upgrade.BakeTime != nil {
			ps.Upgrade.BakeTime = pb.Upgrade.BakeTime.AsDuration()
		}
	}

	return ps
}

func PolicySetToProto(ps domain.PolicySet) *boundflowv1.PolicySet {
	pb := &boundflowv1.PolicySet{
		Concurrency: &boundflowv1.ConcurrencyPolicy{
			MaxConcurrentOperations: ps.Concurrency.MaxConcurrentOperations,
		},
		Failure: &boundflowv1.FailurePolicy{
			EnableRollback: ps.Failure.EnableRollback,
			Retry: &boundflowv1.RetryPolicy{
				MaxRetries:        ps.Failure.Retry.MaxRetries,
				InitialBackoff:    durationpb.New(ps.Failure.Retry.InitialBackoff),
				MaxBackoff:        durationpb.New(ps.Failure.Retry.MaxBackoff),
				BackoffMultiplier: ps.Failure.Retry.BackoffMultiplier,
			},
		},
		MaintenanceWindow: &boundflowv1.MaintenanceWindowPolicy{},
		Upgrade: &boundflowv1.UpgradePolicy{
			AutoPause: ps.Upgrade.AutoPause,
			BakeTime:  durationpb.New(ps.Upgrade.BakeTime),
		},
	}

	for _, w := range ps.MaintenanceWindow.Windows {
		pb.MaintenanceWindow.Windows = append(pb.MaintenanceWindow.Windows, &boundflowv1.MaintenanceWindow{
			CronExpression: w.CronExpression,
			Duration:       durationpb.New(w.Duration),
		})
	}

	return pb
}
