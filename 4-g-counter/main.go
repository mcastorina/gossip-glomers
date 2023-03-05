package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	machineID uint64
	snowflake Snowflake
)

func main() {
	n := maelstrom.NewNode()
	kv := KV[State]{maelstrom.NewSeqKV(n)}

	n.Handle("init", func(msg maelstrom.Message) error {
		body, err := Unmarshal[struct {
			ID string `json:"node_id"`
		}](msg)
		if err != nil {
			return err
		}

		hash := sha256.Sum256([]byte(body.ID))
		machineID = binary.BigEndian.Uint64(hash[:])
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		body, err := Unmarshal[struct {
			Delta int `json:"delta"`
		}](msg)
		if err != nil {
			return err
		}

		go Retry(context.Background(), func(ctx context.Context) error {
			current, _ := kv.ReadDefault(ctx, "g-counter")
			return kv.CompareAndSwap(ctx, "g-counter", current, current.Add(body.Delta), true)
		})
		return n.Reply(Ack(msg))
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Read all values from the kv.
		Retry(context.Background(), func(ctx context.Context) error {
			value, _ := kv.ReadDefault(ctx, "g-counter")
			return kv.CompareAndSwap(ctx, "g-counter", value, value.Add(0), true)
		})
		value, _ := kv.ReadDefault(context.TODO(), "g-counter")
		return n.Reply(Ack(msg, "value", value.Value))
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func Retry(ctx context.Context, task func(context.Context) error) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Task should be context aware, which means it should return
		// when ctx is cancelled.
		if err := task(ctx); err == nil {
			return
		}
	}
}

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

func Unmarshal[T any](msg maelstrom.Message) (T, error) {
	var value T
	return value, json.Unmarshal(msg.Body, &value)
}

type State struct {
	ID    uint64 `json:"id"`
	Value int    `json:"value"`
}

func (s State) Add(delta int) State {
	return State{
		ID:    snowflake.Next(),
		Value: s.Value + delta,
	}
}

type KV[T any] struct{ *maelstrom.KV }

func (kv KV[T]) ReadDefault(ctx context.Context, key string) (T, error) {
	var defaultT T
	value, err := kv.KV.Read(ctx, key)
	if err == nil {
		return kv.mapToT(value.(map[string]any)), nil
	}
	if err, ok := err.(*maelstrom.RPCError); ok && err.Code == maelstrom.KeyDoesNotExist {
		return defaultT, nil
	}
	return defaultT, err
}

func (kv KV[T]) mapToT(m map[string]any) T {
	var t T
	b, _ := json.Marshal(m)
	json.Unmarshal(b, &t)
	return t
}

// Snowflake is an implementation of Twitter's Snowflake ID algorithm.
// https://en.wikipedia.org/wiki/Snowflake_ID
type Snowflake struct {
	lastTimestamp uint64
	overflow      uint64
}

func (s *Snowflake) Next() uint64 {
	ts := uint64(time.Now().UnixMilli())
	overflow := atomic.LoadUint64(&s.overflow)
	lastTimestamp := atomic.LoadUint64(&s.lastTimestamp)
	if ts == lastTimestamp {
		// Increase counter.
		overflow = atomic.AddUint64(&s.overflow, 1)
	} else {
		// Reset overflow and lastTimestamp.
		atomic.CompareAndSwapUint64(&s.overflow, overflow, 0)
		atomic.CompareAndSwapUint64(&s.lastTimestamp, lastTimestamp, ts)
		overflow = 0
	}
	// 41 bits of timestamp, 10 bits of machine ID, and 12 bits of overflow
	return (ts & 0x01ffffffffff) | ((machineID & 0x03ff) << 41) | ((overflow & 0x0fff) << 51)
}
