package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
		body, err := Parse[struct {
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
		body, err := Parse[struct {
			Delta int `json:"delta"`
		}](msg)
		if err != nil {
			return err
		}

		go Retry(context.Background(), func(ctx context.Context) error {
			current, _ := kv.ReadDefault(ctx, "g-counter")
			return kv.CompareAndSwap(ctx, "g-counter", current, current.Add(body.Delta))
		})
		return n.Reply(Ack(msg))
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Read all values from the kv.
		Retry(context.Background(), func(ctx context.Context) error {
			value, _ := kv.ReadDefault(ctx, "g-counter")
			return kv.CompareAndSwap(ctx, "g-counter", value, value.Add(0))
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

func Parse[T any](msg maelstrom.Message) (T, error) {
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

type KV[T any] struct{ kv *maelstrom.KV }

func (kv KV[T]) ReadDefault(ctx context.Context, key string) (T, error) {
	var defaultT T
	value, err := kv.Read(ctx, key)
	if err == nil {
		return value, nil
	}
	if err, ok := err.(*maelstrom.RPCError); ok && err.Code == maelstrom.KeyDoesNotExist {
		return defaultT, nil
	}
	return defaultT, err
}

func (kv KV[T]) Read(ctx context.Context, key string) (T, error) {
	var t T
	value, err := kv.kv.Read(ctx, key)
	if err != nil {
		return t, err
	}
	jsonString, ok := value.(string)
	if !ok {
		return t, fmt.Errorf("expected JSON encoded value")
	}
	return t, json.Unmarshal([]byte(jsonString), &t)
}

func (kv KV[T]) Write(ctx context.Context, key string, value T) error {
	return kv.kv.Write(ctx, key, kv.asJSON(value))
}

func (kv KV[T]) CompareAndSwap(ctx context.Context, key string, oldValue, newValue T) error {
	return kv.kv.CompareAndSwap(ctx, key, kv.asJSON(oldValue), kv.asJSON(newValue), true)
}

func (kv KV[T]) asJSON(t T) string {
	jsonBytes, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return string(jsonBytes)
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
