package store

import (
	"github.com/coreos/etcd/log"
	"github.com/coreos/go-raft"
	"time"
)

func init() {
	raft.RegisterCommand(&CreateCommand{})
}

// Create command
type CreateCommand struct {
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	ExpireTime time.Time `json:"expireTime"`
	Unique     bool      `json:"unique"`
}

// The name of the create command in the log
func (c *CreateCommand) CommandName() string {
	return "etcd:create"
}

// Create node
func (c *CreateCommand) Apply(server raft.Server) (interface{}, error) {
	s, _ := server.StateMachine().(Store)

	e, err := s.Create(c.Key, c.Value, c.Unique, c.ExpireTime, server.CommitIndex(), server.Term())

	if err != nil {
		log.Debug(err)
		return nil, err
	}

	return e, nil
}