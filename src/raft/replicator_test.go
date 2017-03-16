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
		{"tc1", []*LogEntry{{0, 1, "c1"}, {1, 1, "c2"}}},
		{"tc2", []*LogEntry{{2, 2, "c3"}, {3, 2, "c4"}, {4, 2, "c5"}}},
		{"tc3", []*LogEntry{{5, 2, "c6"}, {6, 2, "c7"}}},
	}
	c := newCommitter(nil, 0)
	// test senquentially add
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c.addLogs(tc.data)
			if c.start != 0 {
				t.Fatalf("c.start should be 0, but got %v", c.start)
			}
			if c.end != tc.data[len(tc.data)-1].Index+1 {
				t.Fatalf("c.end should be %v, but got %v",
					c.logs[len(c.logs)-1].Index+1,
					tc.data[len(tc.data)-1].Index+1)
			}
		})
	}

	if len(c.logs) != 5 {
		t.Fatalf("should have 5 logs, but got %v", len(c.logs))
	}
}

func TestTryToCommitOne(t *testing.T) {
	tests := []testdata{
		{"tc1", []*LogEntry{{0, 1, "c1"}, {1, 1, "c2"}}},
		{"tc2", []*LogEntry{{2, 2, "c3"}, {3, 2, "c4"}, {4, 2, "c5"}}},
		{"tc3", []*LogEntry{{5, 2, "c6"}, {6, 2, "c7"}}},
	}
	c := newCommitter(nil, 0)
	c.quoromSize = 3

	for _, tc := range tests {
		c.addLogs(tc.data)
	}

	t.Run("0_not_committed", func(t *testing.T) {
		c.tryToCommitOne(0)
		if c.toCommit != 0 {
			t.Fatalf("toCommit should be %v, but got %v", 0, c.toCommit)
		}
	})

	t.Run("0_committed", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				c.tryToCommitOne(0)
			})
		}
	})

	if c.toCommit != 1 {
		t.Fatalf("toCommit should be %v, but got %v", 1, c.toCommit)
	}
	_, hit := c.logs[0]
	if hit {
		t.Fatalf("the log %v should already be deleted", 0)
	}

	t.Run("2_not_committed", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				c.tryToCommitOne(2)
			})
		}
	})
	if c.toCommit != 1 {
		t.Fatalf("toCommit should be %v, but got %v", 1, c.toCommit)
	}

	t.Run("1_committed", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			t.Run(strconv.Itoa(i), func(t *testing.T) {
				t.Parallel()
				c.tryToCommitOne(1)
			})
		}
	})
	if c.toCommit != 2 {
		t.Fatalf("toCommit should be %v, but got %v", 2, c.toCommit)
	}
	_, hit = c.logs[1]
	if hit {
		t.Fatalf("the log %v should already be deleted", 1)
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
	c := newCommitter(nil, 0)
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
				e := c.tryToCommitRange(p.s, p.e)
				if e != nil {
					t.Fatal(e)
				}
			}(p)
		}
		wg.Wait()
	}

	t.Run("commit_range_1", func(t *testing.T) {
		doCommitRange(t, pair{0, 3}, pair{1, 4})
		if c.toCommit != c.logs[3].Index {
			t.Fatalf("toCommit should be %v, but got %v", c.logs[3].Index, c.toCommit)
		}
		if len(c.committedLogs) != c.toCommit-c.start {
			t.Fatalf("%v logs should be committed, but got %v", c.toCommit-c.start, len(c.committedLogs))
		}
	})

	t.Run("commit_range_2", func(t *testing.T) {
		doCommitRange(t, pair{1, 5}, pair{1, 6})
		if c.toCommit != 5 {
			t.Fatalf("toCommit should be %v, but got %v", 5, c.toCommit)
		}
		if len(c.committedLogs) != 3 {
			t.Fatalf("1 logs should be committed, but got %v", len(c.committedLogs))
		}
	})

	t.Run("error_on_commit_range", func(t *testing.T) {
		e := c.tryToCommitRange(1, 9)
		if e == nil {
			t.Errorf("should get an error")
		}
	})
}
