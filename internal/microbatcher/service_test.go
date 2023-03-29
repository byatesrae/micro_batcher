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
	assert.NoError(t, err, "Process err")
	assert.Equal(t, "Test123ABC", actual, "Actual")
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
				assert.Equal(t, processableJob.Job, processableJob.JobCtx.Value(ctxValueKey{}).(string), "ctx value")

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
		require.NoError(t, err, "Test123 err")

		wg.Done()
	}()
	go func() {
		// Do
		_, err := with.Process(context.WithValue(context.Background(), ctxValueKey{}, "Test456"), "Test456")
		require.NoError(t, err, "Test456 err")

		wg.Done()
	}()
	go func() {
		// Do
		_, err := with.Process(context.WithValue(context.Background(), ctxValueKey{}, "Test789"), "Test789")
		require.NoError(t, err, "Test789 err")

		wg.Done()
	}()

	wg.Wait()
}

func testServiceProcessRefusesJobs(t *testing.T) {
	t.Parallel()

	// Setup
	with := New[string, string](&BatchProcessorMock[string, string]{
		DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
			err := errors.New("Test123")

			for a := range processableJobs {
				processableJob := &processableJobs[a]

				processableJob.ErrOut <- err
			}

			return err
		},
	})

	_, err := with.Process(context.Background(), "Test123")
	require.EqualError(t, err, "Test123")

	// Do
	_, err = with.Process(context.Background(), "Test123")

	// Assert
	assert.EqualError(t, err, "queue job: batch processor is no longer processing: Test123")
}

// testServiceProcessUnblocksPendingJobs tests that if the BatchProcessor fails and
// there are pending jobs then the pending jobs are unblocked.
func testServiceProcessUnblocksPendingJobs(t *testing.T) {
	t.Parallel()

	// To guarantee 1 pending job at the time the BatchProcessor fails, disable
	// the cycle trigger and set the batch size limit to > 1 (2 in this case).
	//
	// To trigger the BatchProcessor failure at the time there is 1 pending job,
	// the processing must have already started but not finished (the use of processingSignaler
	// below).

	// Setup
	processingSignaler := make(chan interface{})

	with := New[string, string](
		&BatchProcessorMock[string, string]{
			DoFunc: func(processableJobs []ProcessableJob[string, string]) error {
				processingSignaler <- nil // Signal that processing has started

				<-processingSignaler // Wait for signal to continue

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
		require.EqualError(t, err, "Test123 job processing err", "first job")

		wg.Done()
	}()

	go func() {
		_, err := with.Process(context.Background(), "Test_2")
		require.EqualError(t, err, "Test123 job processing err", "first job")

		wg.Done()
	}()

	<-processingSignaler // The above jobs are being processed when this receives

	// Do
	go func() {
		_, err := with.Process(context.Background(), "Test_3")
		assert.EqualError(t, err, "job not processed, BatchProcessor is in an error state: Test123 job processing err", "third job")

		wg.Done()
	}()

	waitUntilServiceHasPendingJobCount(with, 1)

	processingSignaler <- nil // Signal the processor to continue and fail

	wg.Wait() // Wait for all assertions
}

func testServiceProcessesInBatches(t *testing.T) {
	// Steup
	jobs := []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}

	processedJobsCh := make(chan []int)

	with := New[int, int](
		&BatchProcessorMock[int, int]{
			DoFunc: func(processableJobs []ProcessableJob[int, int]) error {
				var processedJobs []int

				for a := range processableJobs {
					processableJob := &processableJobs[a]

					processedJobs = append(processedJobs, processableJob.Job)

					processableJob.ResultOut <- processableJob.Job
				}

				go func() { processedJobsCh <- processedJobs }()

				return nil
			},
		},
		WithBatchCycle(time.Hour*9999), // Timer effectively disabled
		WithBatchSizeLimit(len(jobs)),  // Don't trigger until all jobs are in
	)

	// Do
	for a := range jobs {
		job := jobs[a]

		go func() {
			_, err := with.Process(context.Background(), job)
			require.NoError(t, err, fmt.Sprintf("process %v", job))
		}()
	}

	processedJobs := <-processedJobsCh
	sort.Ints(processedJobs)

	// Assert
	assert.Equal(t, jobs, processedJobs, "processedJobs")
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
