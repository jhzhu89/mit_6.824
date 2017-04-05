package raftkv

import (
	"github.com/satori/go.uuid"
)

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Uuid uuid.UUID
}

type PutAppendReply struct {
	WrongLeader bool
	Pending     bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Uuid uuid.UUID
}

type GetReply struct {
	WrongLeader bool
	Pending     bool
	Err         Err
	Value       string
}
