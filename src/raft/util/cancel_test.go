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
		go func(s Context) {
			defer wg.Done()
			select {
			case <-s.Done():
				t.Logf("received stop signal from parrent...")
			}
		}(s)
	}

	f()

	wg.Wait()

	func(s Context) {
		select {
		case <-s.Done():
			t.Logf("received stop signal from parrent...")
		}
	}(s)

}
