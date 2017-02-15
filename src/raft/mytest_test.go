package raft

import (
	"testing"
	"time"
)

func TestMyTest(t *testing.T) {
	servers := 1
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	t.Logf("running...\n")
	<-time.After(time.Second * 10)
	t.Logf("exiting...\n")

}
