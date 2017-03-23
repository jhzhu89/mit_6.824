package util

type CancelContext interface {
	Done() <-chan struct{}
}

type CancelFunc func()

func NewCancelContext() (CancelContext, CancelFunc) {
	c := newCancelContex()
	return &c, func() { c.cancel() }
}

type cancelCtx struct {
	done chan struct{}
}

func newCancelContex() cancelCtx {
	return cancelCtx{make(chan struct{})}
}

func (c *cancelCtx) cancel() {
	close(c.done)
}

func (c *cancelCtx) Done() <-chan struct{} {
	return c.done
}
