package microbatcher

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Service can be used to process Job's in micro-batches.
type Service[Job any, JobResult any] struct {
	// batchCycle is the time after a batch starts before another batch will be started.
	batchCycle time.Duration

	// batchSizeLimit is the maximum batch size. Should the pending Job count exceed
	// this limit, a batch will start earlier than the BatchCycle.
	batchSizeLimit int

	processor BatchProcessor[Job, JobResult]

	// mu covers this whole object.
	mu sync.Mutex

	// batchCycleCancel is set when a cycle is active and can be used to cancel it.
	batchCycleCancel func()

	// processorErr is set once if processor ever returns an error.
	processorErr error

	isShutdown bool

	batchProcessorWG sync.WaitGroup

	// pendingJobs are jobs ready to be processed.
	pendingJobs []ProcessableJob[Job, JobResult]
}

type options struct {
	batchCycle     time.Duration
	batchSizeLimit int
}

func WithBatchCycle(d time.Duration) func(o *options) {
	return func(o *options) {
		o.batchCycle = d
	}
}

func WithBatchSizeLimit(limit int) func(o *options) {
	return func(o *options) {
		o.batchSizeLimit = limit
	}
}

// New creates a new [Service].
func New[Job any, JobResult any](p BatchProcessor[Job, JobResult], overrides ...func(o *options)) *Service[Job, JobResult] {
	options := options{
		batchCycle:     time.Second,
		batchSizeLimit: 32,
	}

	for _, override := range overrides {
		override(&options)
	}

	return &Service[Job, JobResult]{
		processor:      p,
		batchCycle:     options.batchCycle,
		batchSizeLimit: options.batchSizeLimit,
	}
}

// Process job in a micro-batch.
func (s *Service[Job, JobResult]) Process(ctx context.Context, job Job) (JobResult, error) {
	resOut := make(chan JobResult)
	errOut := make(chan error)

	if err := s.queueJob(ctx, job, resOut, errOut); err != nil {
		var res JobResult
		return res, fmt.Errorf("queue job: %w", err)
	}

	select {
	case res := <-resOut:
		return res, nil
	case err := <-errOut:
		var res JobResult
		return res, err
	}
}

// Shutdown the processing.
func (s *Service[Job, JobResult]) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.isShutdown = true

	jobsToProcess := s.pendingJobs
	s.pendingJobs = nil

	s.startProcessing(jobsToProcess)

	s.mu.Unlock()

	s.batchProcessorWG.Wait()

	return nil
}

// PendingJobCount returns the number of pending jobs.
func (s *Service[Job, JobResult]) PendingJobCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.pendingJobs)
}

// ProcessorErr returns the last BatchProcessor error, if any. This value will not
// change once BatchProcessor returns an error.
func (s *Service[Job, JobResult]) ProcessorErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.processorErr
}

func (s *Service[Job, JobResult]) queueJob(ctx context.Context, job Job, resultOut chan<- JobResult, errOut chan<- error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.processorErr != nil {
		return fmt.Errorf("batch processor is no longer processing: %w", s.processorErr)
	}

	s.pendingJobs = append(s.pendingJobs, ProcessableJob[Job, JobResult]{
		JobCtx:    ctx,
		Job:       job,
		ResultOut: resultOut,
		ErrOut:    errOut,
	})

	if len(s.pendingJobs) >= s.batchSizeLimit {
		if s.batchCycleCancel != nil {
			s.batchCycleCancel()

			s.batchCycleCancel = nil
		}

		jobsToProcess := s.pendingJobs
		s.pendingJobs = nil

		s.startProcessing(jobsToProcess)

		return nil
	}

	if s.batchCycleCancel == nil {
		batchCycleTimer := time.NewTimer(s.batchCycle)
		batchCycleCtx, batchCycleCtxCancel := context.WithCancel(context.Background())

		s.batchCycleCancel = batchCycleCtxCancel

		go func() {
			select {
			case <-batchCycleTimer.C:
			case <-batchCycleCtx.Done():
				if !batchCycleTimer.Stop() {
					<-batchCycleTimer.C
				}

				return
			}

			s.mu.Lock()
			defer s.mu.Unlock()

			s.batchCycleCancel = nil

			// Catch the edge case where the timer has fired above, but the ctx
			// is cancelled before this goroutine can lock s.batchMu.
			select {
			case <-batchCycleCtx.Done():
				return
			default:
			}

			jobsToProcess := s.pendingJobs
			s.pendingJobs = nil

			s.startProcessing(jobsToProcess)
		}()
	}

	return nil
}

func (s *Service[Job, JobResult]) startProcessing(jobsToProcess []ProcessableJob[Job, JobResult]) {
	if len(jobsToProcess) == 0 {
		return
	}

	s.batchProcessorWG.Add(1)

	go func() {
		err := s.processor.Do(jobsToProcess)
		defer s.batchProcessorWG.Done()

		s.mu.Lock()
		defer s.mu.Unlock()

		if err != nil {
			s.processorErr = err

			for a := range s.pendingJobs {
				s.pendingJobs[a].ErrOut <- fmt.Errorf("job not processed, BatchProcessor is in an error state: %w", s.processorErr)
			}
		}
	}()
}
