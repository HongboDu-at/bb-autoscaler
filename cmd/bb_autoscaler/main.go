package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/buildbarn/bb-autoscaler/pkg/proto/configuration/bb_autoscaler"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// bb_autoscaler: Automatically adjust the capacity of Amazon EC2 Auto
// Scaling Groups (ASG) based on Prometheus metrics.
//
// This utility can be used to automatically scale the number of
// Buildbarn workers based on queue size metrics exposed by
// bb_scheduler. It supports two execution modes:
//   - One-shot mode: Runs once and exits (suitable for Kubernetes cron jobs).
//     This is the default when execution_interval_seconds is not set.
//   - Continuous mode: Runs indefinitely at a configured interval when
//     execution_interval_seconds is set to a positive value.
//
// This tool is written in such a way that it almost literally takes the
// values obtained from Prometheus and uses those as the desired ASG
// capacity. Any smartness in the autoscaling behavior should be added
// by using PromQL functions such as quantile_over_time(),
// max_over_time(), etc.

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: bb_autoscaler bb_autoscaler.jsonnet")
		}
		var configuration bb_autoscaler.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}

		autoscaler, err := NewAutoscaler(&configuration)
		if err != nil {
			return err
		}

		// Determine if we should run once or indefinitely
		executionInterval := time.Duration(configuration.ExecutionIntervalSeconds) * time.Second
		if executionInterval <= 0 {
			// One-shot mode
			return autoscaler.RunOnce(ctx)
		}

		// Continuous mode - reuses the same autoscaler instance
		log.Printf("Running autoscaler indefinitely with interval of %v", executionInterval)
		const maxConsecutiveFailures = 5
		consecutiveFailures := 0
		for {
			if err := autoscaler.RunOnce(ctx); err != nil {
				consecutiveFailures++
				log.Printf("Error in autoscaler (%d/%d consecutive failures): %v", consecutiveFailures, maxConsecutiveFailures, err)
				if consecutiveFailures >= maxConsecutiveFailures {
					return util.StatusWrapf(err, "Exiting after %d consecutive failures", consecutiveFailures)
				}
			} else {
				if consecutiveFailures > 0 {
					log.Printf("Recovered after %d consecutive failure(s)", consecutiveFailures)
				}
				consecutiveFailures = 0
			}

			log.Printf("Waiting %v until next execution", executionInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(executionInterval):
			}
		}
	})
}
