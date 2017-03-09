package util

import (
	"sync"
)

type RoutineGroupMonitor struct {
	stopper Canceller
	stopf   CancelFunc
	wg      sync.WaitGroup
}

func NewRoutineGroupMonitor() *RoutineGroupMonitor {
	c := &RoutineGroupMonitor{}
	c.stopper, c.stopf = NewCanceller()
	c.wg = sync.WaitGroup{}
	return c
}

func (c *RoutineGroupMonitor) Done() {
	c.stopf()
	c.wg.Wait()
}

func (c *RoutineGroupMonitor) GoFunc(f func(Canceller)) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		f(c.stopper)
	}()
}
