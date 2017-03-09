package util

type Canceller interface {
	Cancelled() <-chan struct{}
}

type CancelFunc func()

func NewCanceller() (Canceller, CancelFunc) {
	c := newCanceller()
	return &c, func() { c.cancel() }
}

type canceller struct {
	done chan struct{}
}

func newCanceller() canceller {
	return canceller{make(chan struct{})}
}

func (c *canceller) cancel() {
	close(c.done)
}

func (c *canceller) Cancelled() <-chan struct{} {
	return c.done
}
