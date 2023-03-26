package microbatcher

import (
	"context"
	"fmt"
	"log"
)

type printJob = string

// printResult aggregates the return values of a call to fmt.Print().
type printJobResult struct {
	n   int
	err error
}

type printerProcessor struct{}

// printerProcessor is our BatchProcessor.
var _ BatchProcessor[printJob, printJobResult] = (*printerProcessor)(nil)

func (p *printerProcessor) Do(_ context.Context, processableJobs []ProcessableJob[printJob, printJobResult]) error {
	for a := range processableJobs {
		processableJob := &processableJobs[a]

		if processableJob.JobCtx.Err() != nil {
			processableJob.ResultOut <- printJobResult{err: fmt.Errorf("job ctx: %w", processableJob.JobCtx.Err())}

			continue
		}

		n, err := fmt.Print(processableJob.Job)
		processableJob.ResultOut <- printJobResult{err: err, n: n}
	}

	return nil
}

// addPrintJob will process s with service.
func addPrintJob(service *Service[printJob, printJobResult], s printJob) {
	res, err := service.Process(context.Background(), s)
	if err != nil {
		log.Printf("Failed to print \":%s\" with err %s", s, err)
	}

	log.Printf("Printed \":%s\" with number of bytes %v", s, res)
}

func ExampleService() {
	service := New[printJob, printJobResult](&printerProcessor{})

	go addPrintJob(service, "hello")
	go addPrintJob(service, "world")
}
