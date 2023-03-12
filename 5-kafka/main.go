package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kafka := NewKafka(maelstrom.NewLinKV(n))

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

// Kafka is an interface to a kafka-like log service. It wraps a Linear KV
// store, using it as a map of "key" to messages ([]int) and all
// "commit-offsets" (map[string]int).
//
// Internally, it partitions messages into groups of 100 to minimize the amount
// of data we need to read and write back to the database.
type Kafka struct {
	partitions     map[string]int
	partitionsLock sync.Mutex
	kv             *maelstrom.KV
}

func NewKafka(kv *maelstrom.KV) Kafka {
	return Kafka{
		kv:         kv,
		partitions: make(map[string]int),
	}
}

func (k *Kafka) Append(key string, value int) int {
	var id int
	startPartition := k.getPartition(key)
	partition := startPartition
	Retry(context.Background(), func(ctx context.Context) error {
		key := fmt.Sprintf("%d-%s", partition, key)
		messages, err := k.readMessages(ctx, key)
		if err != nil {
			return err
		}
		if len(messages) >= 100 {
			partition++
			return fmt.Errorf("partition full")
		}
		id = 100*partition + len(messages)
		newMessages := append(messages, value)
		return k.CompareAndSwap(ctx, key, messages, newMessages)
	})
	if id%100 == 99 {
		// Pre-emptively increase the partition if we know the next one
		// will fail.
		partition++
	}
	if partition != startPartition {
		k.casPartition(key, startPartition, partition)
	}
	return id
}

func (k *Kafka) CommitOffsets(newOffsets map[string]int) {
	Retry(context.Background(), func(ctx context.Context) error {
		offsets, err := k.readOffsets(ctx)
		if err != nil {
			return err
		}
		for k, v := range offsets {
			// If the key is already present in newOffsets, take it
			// as the latest value.
			if _, ok := newOffsets[k]; ok {
				continue
			}
			// Otherwise, preserve the previous values.
			newOffsets[k] = v
		}
		return k.CompareAndSwap(ctx, "commit-offsets", offsets, newOffsets)
	})
}

func (k *Kafka) Offsets(keys ...string) map[string]int {
	offsets, _ := k.readOffsets(context.Background())
	for _, key := range keys {
		// Delete any keys that weren't requested.
		if _, ok := offsets[key]; ok {
			continue
		}
		delete(offsets, key)
	}
	return offsets
}

func (k *Kafka) Read(keyOffsets map[string]int) map[string][][]int {
	logs := make(map[string][][]int, len(keyOffsets))
	for key, offset := range keyOffsets {
		messages, _ := k.readMessages(context.Background(), fmt.Sprintf("%d-%s", offset/100, key))
		// No new messages.
		if offset%100 >= len(messages) {
			continue
		}
		// Read all messages after offset.
		vals := messages[offset%100:]
		// Convert to [offset, message] pairs.
		keyVals := make([][]int, len(vals))
		for i := 0; i < len(keyVals); i++ {
			keyVals[i] = []int{offset + i, vals[i]}
		}
		logs[key] = keyVals
	}
	return logs
}

func (k *Kafka) CompareAndSwap(ctx context.Context, key string, from, to any) error {
	return k.kv.CompareAndSwap(ctx, key, asJSON(from), asJSON(to), true)
}

func (k *Kafka) readMessages(ctx context.Context, key string) ([]int, error) {
	value, err := k.kv.Read(ctx, key)
	if doesNotExist(err) {
		return []int{}, nil
	}
	if err != nil {
		return nil, err
	}
	jsonString, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected JSON encoded value")
	}
	var messages []int
	if err := json.Unmarshal([]byte(jsonString), &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

func (k *Kafka) readOffsets(ctx context.Context) (map[string]int, error) {
	value, err := k.kv.Read(ctx, "commit-offsets")
	if doesNotExist(err) {
		return map[string]int{}, nil
	}
	if err != nil {
		return nil, err
	}
	jsonString, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected JSON encoded value")
	}
	var offsets map[string]int
	if err := json.Unmarshal([]byte(jsonString), &offsets); err != nil {
		return nil, err
	}
	return offsets, nil
}

func (k *Kafka) getPartition(key string) int {
	k.partitionsLock.Lock()
	defer k.partitionsLock.Unlock()
	return k.partitions[key]
}

func (k *Kafka) casPartition(key string, from, to int) {
	k.partitionsLock.Lock()
	defer k.partitionsLock.Unlock()
	if k.partitions[key] == from {
		k.partitions[key] = to
	}
}

func asJSON(v any) string {
	jsonBytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(jsonBytes)
}

func doesNotExist(err error) bool {
	if err == nil {
		return false
	}
	if err, ok := err.(*maelstrom.RPCError); ok {
		return err.Code == maelstrom.KeyDoesNotExist
	}
	return false
}
