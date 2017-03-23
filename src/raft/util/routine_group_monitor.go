package util

import (
	"sync"
)

type RoutineGroupMonitor struct {
	canceller Canceller
	cancelf   CancelFunc
	wg        sync.WaitGroup
}

func NewRoutineGroupMonitor() *RoutineGroupMonitor {
	c := &RoutineGroupMonitor{}
	c.canceller, c.cancelf = NewCanceller()
	c.wg = sync.WaitGroup{}
	return c
}

func (c *RoutineGroupMonitor) Done() {
	c.cancelf()
	c.wg.Wait()
}

func (c *RoutineGroupMonitor) GoFunc(f func(Canceller)) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		f(c.canceller)
	}()
}
