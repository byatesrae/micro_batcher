package microbatcher

import "context"

// ProcessableJob is what [BatchProcessor] will process.
type ProcessableJob[Job any, JobResult any] struct {
	// JobCtx is the context applicable when processing the job.
	JobCtx context.Context

	// Job is what should be "processed".
	Job Job

	// ResultOut is the channel on which to send the result of processing Job.
	// Only one value will be read from this channel.
	ResultOut chan<- JobResult
}

// BatchProcessor should be able to "process" Job's to yield a JobResult for each.
type BatchProcessor[Job any, JobResult any] interface {
	// Do should process all processableJobs. See the fields of [ProcessableJob]
	// for a more detailed explanation of behaviour. An error should only be returned
	// from this method if this method should not be called anymore.
	Do(ctx context.Context, processableJobs []ProcessableJob[Job, JobResult]) error
}
