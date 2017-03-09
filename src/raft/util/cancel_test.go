package util

import (
	"sync"
	"testing"
)

func TestWithStop(t *testing.T) {
	s, f := NewCanceller()
	var wg sync.WaitGroup

	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(s Canceller) {
			defer wg.Done()
			select {
			case <-s.Cancelled():
				t.Logf("received stop signal from parrent...")
			}
		}(s)
	}

	f()

	wg.Wait()

	func(s Canceller) {
		select {
		case <-s.Cancelled():
			t.Logf("received stop signal from parrent...")
		}
	}(s)

}
