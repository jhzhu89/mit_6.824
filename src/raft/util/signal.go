package util

import (
	"sync"
)

type Signal interface {
	Received() <-chan struct{}
	Send()
}

type signal struct {
	sig  chan struct{}
	once sync.Once
}

func NewSignal() Signal {
	return &signal{make(chan struct{}), sync.Once{}}
}

func (s *signal) Received() <-chan struct{} {
	return s.sig
}

func (s *signal) Send() {
	s.once.Do(func() {
		// Make sure only close the chan once.
		close(s.sig)
	})
}
