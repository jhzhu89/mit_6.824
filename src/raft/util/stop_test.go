package util

import (
	"sync"
	"testing"
)

func TestWithStop(t *testing.T) {
	s, f := WithStop()
	var wg sync.WaitGroup

	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func(s Stopper) {
			defer wg.Done()
			select {
			case <-s.Stopped():
				t.Logf("received stop signal from parrent...")
			}
		}(s)
	}

	f()

	wg.Wait()

	func(s Stopper) {
		select {
		case <-s.Stopped():
			t.Logf("received stop signal from parrent...")
		}
	}(s)

}
