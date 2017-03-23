package util

import (
	"sync"
	"testing"
)

func TestWithStop(t *testing.T) {
	s, f := NewCancelContext()
	var wg sync.WaitGroup

	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(s CancelContext) {
			defer wg.Done()
			select {
			case <-s.Done():
				t.Logf("received stop signal from parrent...")
			}
		}(s)
	}

	f()

	wg.Wait()

	func(s CancelContext) {
		select {
		case <-s.Done():
			t.Logf("received stop signal from parrent...")
		}
	}(s)

}
