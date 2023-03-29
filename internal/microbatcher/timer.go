package microbatcher

import "time"

type timer interface {
	C() <-chan time.Time
	Stop() bool
}

type standardTimer struct {
	timer *time.Timer
}

func (t *standardTimer) C() <-chan time.Time {
	return t.timer.C
}

func (t *standardTimer) Stop() bool {
	return t.timer.Stop()
}
