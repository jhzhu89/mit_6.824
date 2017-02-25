package util

type Stopper interface {
	Stopped() <-chan struct{}
}

type StopFunc func()

func WithStop() (Stopper, StopFunc) {
	s := newStopper()
	return &s, func() { s.stop() }
}

type stopper struct {
	done chan struct{}
}

func newStopper() stopper {
	return stopper{make(chan struct{})}
}

func (s *stopper) stop() {
	close(s.done)
}

func (s *stopper) Stopped() <-chan struct{} {
	return s.done
}
