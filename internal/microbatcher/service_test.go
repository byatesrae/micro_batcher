package microbatcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceProcess(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		with        *Service[string, string]
		giveCtx     context.Context
		giveJob     string
		expected    string
		expectedErr string
	}{
		{
			name: "success",
			with: New[string, string](&BatchProcessorMock[string, string]{
				DoFunc: func(ctx context.Context, processableJobs []ProcessableJob[string, string]) error {
					for a := range processableJobs {
						processableJob := &processableJobs[a]

						processableJob.ResultOut <- processableJob.Job + "ABC"
					}

					return nil
				},
			}),
			giveCtx:  context.Background(),
			giveJob:  "Test123",
			expected: "Test123ABC",
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actual, actualErr := tc.with.Process(tc.giveCtx, tc.giveJob)

			assert.Equal(t, tc.expected, actual, "actual")

			if tc.expectedErr != "" {
				assert.EqualError(t, actualErr, tc.expectedErr, "actual err")
			} else {
				assert.NoError(t, actualErr, "actual err")
			}
		})
	}
}
