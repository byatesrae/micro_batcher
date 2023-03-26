package microbatcher

import (
	"context"
	"time"
)

// Service can be used to process Job's in micro-batches.
type Service[Job any, JobResult any] struct {
	// BatchCycle is the time after a batch starts before another batch will be started.
	BatchCycle time.Duration

	// BatchSizeLimit is the maximum batch size. Should the pending Job count exceed
	// this limit, a batch will start earlier than the BatchCycle.
	BatchSizeLimit int
}

// New creates a new [Service].
func New[Job any, JobResult any](p BatchProcessor[Job, JobResult]) *Service[Job, JobResult] {
	return nil
}

// Process job in a micro-batch.
func (s *Service[Job, JobResult]) Process(ctx context.Context, job Job) (JobResult, error) {
	var res JobResult

	return res, nil
}

// Shutdown the processing.
func (s *Service[Job, JobResult]) Shutdown(ctx context.Context) error {
	return nil
}
