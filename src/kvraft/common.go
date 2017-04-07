package raftkv

import (
	"fmt"
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

func (r PutAppendReply) String() string {
	return fmt.Sprintf("{WrongLeader: %v, Pending: %v, Err: %v}", r.WrongLeader, r.Pending, r.Err)
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

func (r GetReply) String() string {
	return fmt.Sprintf("{WrongLeader: %v, Pending: %v, Err: %v, Value: %v}", r.WrongLeader, r.Pending, r.Err, r.Value)
}
