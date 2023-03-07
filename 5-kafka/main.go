package main

import (
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kafka := NewKafka()

	n.Handle("send", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Key     string `json:"key"`
			Message int    `json:"msg"`
		}](msg)
		if err != nil {
			return err
		}
		return n.Reply(Ack(msg, "offset", kafka.Append(body.Key, body.Message)))
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Offsets map[string]int `json:"offsets"`
		}](msg)
		if err != nil {
			return err
		}
		return n.Reply(Ack(msg, "msgs", kafka.Read(body.Offsets)))
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Offsets map[string]int `json:"offsets"`
		}](msg)
		if err != nil {
			return err
		}
		kafka.CommitOffsets(body.Offsets)
		return n.Reply(Ack(msg))
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Keys []string `json:"keys"`
		}](msg)
		if err != nil {
			return err
		}
		return n.Reply(Ack(msg, "offsets", kafka.Offsets(body.Keys...)))
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Kafka struct {
	mu               sync.Mutex
	messages         map[string][]int
	committedOffsets map[string]int
}

func NewKafka() Kafka {
	return Kafka{
		messages:         map[string][]int{},
		committedOffsets: map[string]int{},
	}
}

func (k *Kafka) Append(key string, value int) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.messages[key] = append(k.messages[key], value)
	return len(k.messages[key]) - 1
}

func (k *Kafka) CommitOffsets(newOffsets map[string]int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for key, val := range newOffsets {
		k.committedOffsets[key] = val
	}
}

func (k *Kafka) Offsets(keys ...string) map[string]int {
	k.mu.Lock()
	defer k.mu.Unlock()
	offsets := make(map[string]int, len(keys))
	for _, key := range keys {
		if _, ok := k.committedOffsets[key]; !ok {
			continue
		}
		offsets[key] = k.committedOffsets[key]
	}
	return offsets
}

func (k *Kafka) Read(keyOffsets map[string]int) map[string][][]int {
	k.mu.Lock()
	defer k.mu.Unlock()
	logs := make(map[string][][]int, len(keyOffsets))
	for key, offset := range keyOffsets {
		if offset >= len(k.messages[key]) {
			continue
		}
		vals := k.messages[key][offset:]
		keyVals := make([][]int, len(vals))
		for i := 0; i < len(keyVals); i++ {
			keyVals[i] = []int{offset + i, vals[i]}
		}
		logs[key] = keyVals
	}
	return logs
}
