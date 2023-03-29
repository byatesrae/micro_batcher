package microbatcher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServiceProcess(t *testing.T) {
	t.Parallel()

	t.Run("can_process", testServiceProcessCanProcess)
	t.Run("calls_batch_processor", testServiceCallsBatchProcessor)
	t.Run("refuses_more_jobs", testServiceProcessRefusesJobs)
	t.Run("unblocks_pending_jobs", testServiceProcessUnblocksPendingJobs)
	t.Run("processes_in_batches", testServiceProcessesInBatches)
	t.Run("processes_in_cycles", testServiceProcessesInCycles)
	t.Run("limits_batch_size", testServiceLimitsBatchSize)
}

func testServiceProcessCanProcess(t *testing.T) {
	t.Parallel()

	// Setup
	with := New[string, string](&BatchProcessorMock[string, string]{
		DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
			for a := range processableJobs {
				processableJob := &processableJobs[a]

				processableJob.ResultOut <- processableJob.Job + "ABC"
			}

			return nil
		},
	})

	// Do
	actual, err := with.Process(context.Background(), "Test123")

	// Assert
	assert.NoError(t, err, "with.Process()")
	assert.Equal(t, "Test123ABC", actual, "actual")
}

func testServiceCallsBatchProcessor(t *testing.T) {
	t.Parallel()

	// Setup
	type ctxValueKey struct{}

	with := New[string, string](&BatchProcessorMock[string, string]{
		DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
			for a := range processableJobs {
				processableJob := &processableJobs[a]

				// Assert - does the processor get the right job & ctx passed to it?
				assert.Equal(t, processableJob.Job, processableJob.JobCtx.Value(ctxValueKey{}).(string), "job = ctx value")

				processableJob.ResultOut <- processableJob.Job
			}

			return nil
		},
	})

	wg := sync.WaitGroup{}
	wg.Add(3) // 3 jobs added below

	go func() {
		// Do
		_, err := with.Process(context.WithValue(context.Background(), ctxValueKey{}, "Test123"), "Test123")
		require.NoError(t, err, "with.Process(Test123)")

		wg.Done()
	}()
	go func() {
		// Do
		_, err := with.Process(context.WithValue(context.Background(), ctxValueKey{}, "Test456"), "Test456")
		require.NoError(t, err, "with.Process(Test456)")

		wg.Done()
	}()
	go func() {
		// Do
		_, err := with.Process(context.WithValue(context.Background(), ctxValueKey{}, "Test789"), "Test789")
		require.NoError(t, err, "with.Process(Test789)")

		wg.Done()
	}()

	wg.Wait()
}

func testServiceProcessRefusesJobs(t *testing.T) {
	t.Parallel()

	// Setup
	with := New[string, string](&BatchProcessorMock[string, string]{
		DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
			err := errors.New("Test123 processer error")

			for a := range processableJobs {
				processableJob := &processableJobs[a]

				processableJob.ErrOut <- err
			}

			return err
		},
	})

	_, err := with.Process(context.Background(), "Test456")
	require.EqualError(t, err, "Test123 processer error", "with.Process(Test456)")

	// Do
	_, err = with.Process(context.Background(), "Test789")

	// Assert
	assert.EqualError(t, err, "queue job: batch processor is no longer processing: Test123 processer error", "with.Process(Test789)")
}

// testServiceProcessUnblocksPendingJobs tests that if the BatchProcessor fails and
// there are pending jobs then the pending jobs are unblocked.
func testServiceProcessUnblocksPendingJobs(t *testing.T) {
	t.Parallel()

	// To guarantee 1 pending job at the time the BatchProcessor fails, disable
	// the cycle trigger and set the batch size limit to > 1 (2 in this case).
	//
	// To trigger the BatchProcessor failure at the time there is 1 pending job,
	// the processing must have already started but not finished (the use of processingCh
	// below).

	// Setup
	processingCh := make(chan interface{})

	with := New[string, string](
		&BatchProcessorMock[string, string]{
			DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
				processingCh <- nil // Signal that processing has started

				<-processingCh // Wait for signal to continue

				err := errors.New("Test123 job processing err")

				for a := range processableJobs {
					processableJob := &processableJobs[a]

					processableJob.ErrOut <- err
				}

				return err
			},
		},
		WithBatchCycle(time.Hour*9999), // Effectively disable the cycle trigger
		WithBatchSizeLimit(2),          // 1 pending job is needed without triggering processing
	)

	wg := sync.WaitGroup{}
	wg.Add(3) // 3 jobs being processed

	go func() { // Trigger the processor with these 2 jobs
		_, err := with.Process(context.Background(), "Test_1")
		require.EqualError(t, err, "Test123 job processing err", "with.Process(Test_1)")

		wg.Done()
	}()

	go func() {
		_, err := with.Process(context.Background(), "Test_2")
		require.EqualError(t, err, "Test123 job processing err", "with.Process(Test_2)")

		wg.Done()
	}()

	<-processingCh // The above jobs are being processed when this receives

	// Do
	go func() {
		_, err := with.Process(context.Background(), "Test_3")
		assert.EqualError(t, err, "job not processed, BatchProcessor is in an error state: Test123 job processing err", "with.Process(Test_3)")

		wg.Done()
	}()

	waitUntilServiceHasPendingJobCount(with, 1)

	processingCh <- nil // Signal the processor to continue and fail

	wg.Wait() // Wait for all assertions
}

func testServiceProcessesInBatches(t *testing.T) {
	t.Parallel()

	// Setup
	jobs := []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}

	batchProcessor := &BatchProcessorMock[int, int]{
		DoFunc: func(processableJobs []ProcessableJob[int, int]) error {
			for a := range processableJobs {
				processableJobs[a].ResultOut <- 1
			}

			return nil
		},
	}

	with := New[int, int](
		batchProcessor,
		WithBatchCycle(time.Hour*9999), // Timer effectively disabled
		WithBatchSizeLimit(len(jobs)),  // Don't trigger until all jobs are in
	)

	// Do
	wg := sync.WaitGroup{}
	wg.Add(len(jobs))

	for a := range jobs {
		job := jobs[a]

		go func() {
			_, err := with.Process(context.Background(), job)
			require.NoError(t, err, fmt.Sprintf("with.Process(%v)", job))

			wg.Done()
		}()
	}

	wg.Wait()

	// Assert
	if !assert.Len(t, batchProcessor.calls.Do, 1, "batchProcessor.calls.Do len") {
		return
	}

	var processedJobs []int
	for a := range batchProcessor.calls.Do[0].ProcessableJobs {
		processableJob := batchProcessor.calls.Do[0].ProcessableJobs[a]

		processedJobs = append(processedJobs, processableJob.Job)
	}
	sort.Ints(processedJobs)

	assert.Equal(t, jobs, processedJobs, "processedJobs")
}

func testServiceProcessesInCycles(t *testing.T) {
	t.Parallel()

	// Setup
	cycleDuration := time.Millisecond * 50

	with := New[string, string](
		&BatchProcessorMock[string, string]{
			DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
				for a := range processableJobs {
					processableJob := &processableJobs[a]

					processableJob.ResultOut <- "done"
				}

				return nil
			},
		},
		WithBatchCycle(cycleDuration),
		WithBatchSizeLimit(9999), // Effectively disable for this test
	)

	// Do
	start := time.Now()

	iterations := 20
	for a := 0; a < iterations; a++ {
		name := fmt.Sprintf("TestJob%v", a)

		_, err := with.Process(context.Background(), name)
		require.NoError(t, err, fmt.Sprintf("with.Process(%v)", name))
	}

	totalDuration := time.Since(start)

	// Assert - If these prove flaky, may need to swap out Service's internal timer for something this test can control.
	assert.Greater(t, totalDuration, cycleDuration*time.Duration(iterations-1), "lower bound")
	assert.Less(t, totalDuration, cycleDuration*time.Duration(iterations+1), "upper bound")
}

func testServiceLimitsBatchSize(t *testing.T) {
	t.Parallel()

	// Setup
	batchSizeLimit := 5

	batchProcessor := &BatchProcessorMock[string, string]{
		DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
			for a := range processableJobs {
				processableJobs[a].ResultOut <- "done"
			}

			return nil
		},
	}

	with := New[string, string](batchProcessor, WithBatchSizeLimit(batchSizeLimit))

	// Do
	totalJobsCount := batchSizeLimit * 5

	wg := sync.WaitGroup{}
	wg.Add(totalJobsCount)

	for a := 0; a < totalJobsCount; a++ {
		a := a

		go func() {
			name := fmt.Sprintf("TestJob%v", a)

			_, err := with.Process(context.Background(), name)
			require.NoError(t, err, fmt.Sprintf("with.Process(%v)", name))

			wg.Done()
		}()
	}

	wg.Wait()

	// Assert
	totalProcessedJobsCount := 0
	for a := range batchProcessor.calls.Do {
		count := len(batchProcessor.calls.Do[a].ProcessableJobs)
		totalProcessedJobsCount += count

		assert.LessOrEqual(t, count, batchSizeLimit, fmt.Sprintf("batchProcessor.calls.Do[%v].ProcessableJobs len", a))
	}

	assert.Equal(t, totalProcessedJobsCount, totalJobsCount, "totalProcessedJobsCount")
}

func waitUntilServiceHasPendingJobCount[Job any, JobResult any](s *Service[Job, JobResult], n int) {
	c := make(chan interface{})
	go func() {
		for {
			if s.PendingJobCount() == n {
				c <- nil
				return
			}

			time.Sleep(time.Millisecond * 100)
		}
	}()
	<-c
}
