package util

type Signal interface {
	Received() <-chan struct{}
	Send()
}

type signal struct {
	sig chan struct{}
}

func NewSignal() Signal {
	return &signal{make(chan struct{})}
}

func (s *signal) Received() <-chan struct{} {
	return s.sig
}

func (s *signal) Send() {
	close(s.sig)
}
