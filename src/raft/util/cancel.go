package util

type Context interface {
	Done() <-chan struct{}
}

type CancelFunc func()

func NewCancelContext() (Context, CancelFunc) {
	c := newCancelContext()
	return &c, func() { c.cancel() }
}

type cancelCtx struct {
	done chan struct{}
}

func newCancelContext() cancelCtx {
	return cancelCtx{make(chan struct{})}
}

func (c *cancelCtx) cancel() {
	close(c.done)
}

func (c *cancelCtx) Done() <-chan struct{} {
	return c.done
}
