package raftkv

type kvstore struct {
	store map[string]string
}

func newKvstore() *kvstore {
	return &kvstore{store: make(map[string]string)}
}

func (k *kvstore) put(key, value string) {
	k.store[key] = value
}

func (k *kvstore) append(key, value string) {
	old := k.store[key]
	k.store[key] = old + value
}

func (k *kvstore) get(key string) string {
	return k.store[key]
}
