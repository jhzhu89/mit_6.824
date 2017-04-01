package raftkv

import (
	"sync"
)

type kvstore struct {
	sync.RWMutex
	store map[string]string
}

func newKvstore() *kvstore {
	return &kvstore{store: make(map[string]string)}
}

func (k *kvstore) put(key, value string) {
	k.Lock()
	defer k.Unlock()
	k.store[key] = value
}

func (k *kvstore) append(key, value string) {
	k.Lock()
	defer k.Unlock()
	old := k.store[key]
	k.store[key] = old + value
}

func (k *kvstore) get(key string) string {
	k.RLock()
	defer k.RUnlock()
	return k.store[key]
}
