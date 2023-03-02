package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var topology map[string][]string
	messages := NewSet[int]()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		type broadcastMsg struct {
			Kind    string `json:"type"`
			Message int    `json:"message"`
		}
		var body broadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// If we haven't seen this message before, share what we
		// learned to our friends.
		if messages.Add(body.Message) {
			for _, friend := range topology[n.ID()] {
				if friend == msg.Src {
					continue
				}
				_ = n.RPC(friend, body, NoopHandler)
			}
		}
		return n.Reply(Ack(msg))
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(Ack(msg, "messages", messages.Elements()))
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMsg struct {
			Topology map[string][]string
		}
		var body topologyMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology
		return n.Reply(Ack(msg))
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func NoopHandler(maelstrom.Message) error { return nil }

func Ack(msg maelstrom.Message, kvs ...any) (maelstrom.Message, any) {
	if len(kvs)%2 != 0 {
		panic("odd number of key/values")
	}
	type kindMsg struct {
		Kind string `json:"type"`
	}
	var body kindMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return msg, nil
	}
	resp := map[string]any{"type": body.Kind + "_ok"}
	for i := 0; i < len(kvs); i += 2 {
		key := kvs[i].(string)
		resp[key] = kvs[i+1]
	}
	return msg, resp
}

type Set[T comparable] struct {
	mu       sync.Mutex
	elements map[T]struct{}
}

func NewSet[T comparable]() Set[T] {
	return Set[T]{elements: make(map[T]struct{})}
}

func (s *Set[T]) Add(t T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.elements[t]; ok {
		return false
	}
	s.elements[t] = struct{}{}
	return true
}

func (s *Set[T]) Elements() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	elems := make([]T, 0, len(s.elements))
	for elem := range s.elements {
		elems = append(elems, elem)
	}
	return elems
}
