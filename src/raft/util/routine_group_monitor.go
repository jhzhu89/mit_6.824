package util

import (
	"sync"
)

type RoutineGroup struct {
	ctx     CancelContext
	cancelf CancelFunc
	wg      sync.WaitGroup
}

type DoneFunc func()

func NewRoutineGroup() (*RoutineGroup, DoneFunc) {
	c := &RoutineGroup{}
	c.ctx, c.cancelf = NewCancelContext()
	c.wg = sync.WaitGroup{}
	return c, func() {
		c.cancelf()
		c.wg.Wait()
	}
}

func (c *RoutineGroup) GoFunc(f func(CancelContext)) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		f(c.ctx)
	}()
}
