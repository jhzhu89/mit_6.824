package raft

import (
	"strconv"
	"sync"
	"testing"
)

type testdata struct {
	name string
	data []*LogEntry
}

func TestAddLogs(t *testing.T) {
	tests := []testdata{
		{"tc1", []*LogEntry{{1, 1, "c1"}, {2, 1, "c2"}}},
		{"tc2", []*LogEntry{{3, 2, "c3"}, {4, 2, "c4"}, {5, 2, "c5"}}},
		{"tc3", []*LogEntry{{5, 2, "c6"}, {6, 2, "c7"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newCommitter(nil)
			c.addLogs(tc.data)
			if c.start != tc.data[0].Index {
				t.Fatalf("c.start should be %v, but got %v",
					tc.data[0].Index,
					c.start)
			}
			if c.end != tc.data[len(tc.data)-1].Index+1 {
				t.Fatalf("c.end should be %v, but got %v",
					c.logs[len(c.logs)-1].Index+1,
					tc.data[len(tc.data)-1].Index+1)
			}
		})
	}
}

func TestTryToCommitOne(t *testing.T) {
	tests := []testdata{
		{"tc1", []*LogEntry{{1, 2, "c1"}, {2, 2, "c2"}}},
		{"tc2", []*LogEntry{{3, 2, "c3"}, {4, 2, "c4"}, {5, 2, "c5"}}},
		{"tc3", []*LogEntry{{6, 2, "c6"}, {7, 2, "c7"}}},
	}
	c := newCommitter(nil)
	c.quoromSize = 3

	for _, tc := range tests {
		c.addLogs(tc.data)
	}

	t.Run("1_not_committed", func(t *testing.T) {
		c.tryCommitOne(1)
		if c.toCommit != 1 {
			t.Fatalf("toCommit should be %v, but got %v", 1, c.toCommit)
		}
	})

	t.Run("1_committed", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				c.tryCommitOne(1)
			})
		}
	})

	if c.toCommit != 2 {
		t.Fatalf("toCommit should be %v, but got %v", 2, c.toCommit)
	}
	_, hit := c.logs[1]
	if hit {
		t.Fatalf("the log %v should already be deleted", 1)
	}

	t.Run("3_not_committed", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				c.tryCommitOne(3)
			})
		}
	})
	if c.toCommit != 2 {
		t.Fatalf("toCommit should be %v, but got %v", 2, c.toCommit)
	}

	t.Run("2_committed", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				c.tryCommitOne(2)
			})
		}
	})
	if c.toCommit != 3 {
		t.Fatalf("toCommit should be %v, but got %v", 3, c.toCommit)
	}
	_, hit = c.logs[2]
	if hit {
		t.Fatalf("the log %v should already be deleted", 2)
	}

	if len(c.committedLogs) != 2 {
		t.Fatalf("2 logs should be committed, but got %v", len(c.committedLogs))
	}
}

func TestTryToCommitRange(t *testing.T) {
	tests := []testdata{
		{"td1", []*LogEntry{{2, 2, "c3"}, {3, 2, "c4"}, {4, 2, "c5"}}},
		{"td2", []*LogEntry{{5, 2, "c6"}, {6, 2, "c7"}}},
	}
	c := newCommitter(nil)
	c.quoromSize = 3
	for _, d := range tests {
		c.addLogs(d.data)
	}

	type pair struct{ s, e int }

	doCommitRange := func(t *testing.T, pairs ...pair) {
		var wg sync.WaitGroup
		for _, p := range pairs {
			wg.Add(1)
			go func(p pair) {
				defer wg.Done()
				e := c.tryCommitRange(p.s, p.e)
				if e != nil {
					t.Fatal(e)
				}
			}(p)
		}
		wg.Wait()
	}

	t.Run("commit_range_1", func(t *testing.T) {
		doCommitRange(t, pair{0, 3}, pair{1, 4})
		if c.toCommit != c.logs[4].Index {
			t.Fatalf("toCommit should be %v, but got %v", c.logs[4].Index, c.toCommit)
		}
		if len(c.committedLogs) != c.toCommit-c.start {
			t.Fatalf("%v logs should be committed, but got %v", c.toCommit-c.start, len(c.committedLogs))
		}
	})

	t.Run("commit_range_2", func(t *testing.T) {
		doCommitRange(t, pair{1, 5}, pair{1, 6})
		if c.toCommit != 6 {
			t.Fatalf("toCommit should be %v, but got %v", 6, c.toCommit)
		}
		if len(c.committedLogs) != 4 {
			t.Fatalf("4 logs should be committed, but got %v", len(c.committedLogs))
		}
	})

	t.Run("error_on_commit_range", func(t *testing.T) {
		e := c.tryCommitRange(1, 9)
		if e == nil {
			t.Errorf("should get an error")
		}
	})
}
