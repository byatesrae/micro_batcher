// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package microbatcher

import (
	"context"
	"sync"
)

// Ensure, that BatchProcessorMock does implement BatchProcessor.
// If this is not the case, regenerate this file with moq.
var _ BatchProcessor[any, any] = &BatchProcessorMock[any, any]{}

// BatchProcessorMock is a mock implementation of BatchProcessor.
//
//	func TestSomethingThatUsesBatchProcessor(t *testing.T) {
//
//		// make and configure a mocked BatchProcessor
//		mockedBatchProcessor := &BatchProcessorMock{
//			DoFunc: func(ctx context.Context, processableJobs []ProcessableJob[Job, JobResult]) error {
//				panic("mock out the Do method")
//			},
//		}
//
//		// use mockedBatchProcessor in code that requires BatchProcessor
//		// and then make assertions.
//
//	}
type BatchProcessorMock[Job any, JobResult any] struct {
	// DoFunc mocks the Do method.
	DoFunc func(ctx context.Context, processableJobs []ProcessableJob[Job, JobResult]) error

	// calls tracks calls to the methods.
	calls struct {
		// Do holds details about calls to the Do method.
		Do []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// ProcessableJobs is the processableJobs argument value.
			ProcessableJobs []ProcessableJob[Job, JobResult]
		}
	}
	lockDo sync.RWMutex
}

// Do calls DoFunc.
func (mock *BatchProcessorMock[Job, JobResult]) Do(ctx context.Context, processableJobs []ProcessableJob[Job, JobResult]) error {
	if mock.DoFunc == nil {
		panic("BatchProcessorMock.DoFunc: method is nil but BatchProcessor.Do was just called")
	}
	callInfo := struct {
		Ctx             context.Context
		ProcessableJobs []ProcessableJob[Job, JobResult]
	}{
		Ctx:             ctx,
		ProcessableJobs: processableJobs,
	}
	mock.lockDo.Lock()
	mock.calls.Do = append(mock.calls.Do, callInfo)
	mock.lockDo.Unlock()
	return mock.DoFunc(ctx, processableJobs)
}

// DoCalls gets all the calls that were made to Do.
// Check the length with:
//
//	len(mockedBatchProcessor.DoCalls())
func (mock *BatchProcessorMock[Job, JobResult]) DoCalls() []struct {
	Ctx             context.Context
	ProcessableJobs []ProcessableJob[Job, JobResult]
} {
	var calls []struct {
		Ctx             context.Context
		ProcessableJobs []ProcessableJob[Job, JobResult]
	}
	mock.lockDo.RLock()
	calls = mock.calls.Do
	mock.lockDo.RUnlock()
	return calls
}