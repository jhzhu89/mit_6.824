package raft

import (
	"fmt"
	"log"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	crand "crypto/rand"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func init() {
	// Ensure we use a high-entropy seed for the psuedo-random generator
	rand.Seed(newSeed())
}

//
// Util funcs.
//
// returns an int64 from a crypto random source
// can be used to seed a source for a math/rand.
func newSeed() int64 {
	r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}
	return r.Int64()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) time.Duration {
	if minVal == 0 {
		return 0
	}
	extra := (time.Duration(rand.Int63()) % (minVal))
	return minVal + extra
}

// Run a child routine, wait it to exist.
func goChild(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}

func goFunc(arg interface{}) { go arg.(func())() }

type Int32 int32

func (i *Int32) AtomicGet() int32 {
	return atomic.LoadInt32((*int32)(i))
}

func (i *Int32) AtomicSet(v int32) {
	atomic.StoreInt32((*int32)(i), v)
}

func (i *Int32) AtomicAdd(d int32) {
	atomic.AddInt32((*int32)(i), d)
}
