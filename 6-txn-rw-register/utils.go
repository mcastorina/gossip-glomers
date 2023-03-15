package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var snowflake Snowflake

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

func Parse[T any](msg maelstrom.Message) (T, error) {
	var value T
	return value, json.Unmarshal(msg.Body, &value)
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

// Snowflake is an implementation of Twitter's Snowflake ID algorithm.
// https://en.wikipedia.org/wiki/Snowflake_ID
type Snowflake struct {
	lastTimestamp uint64
	overflow      uint64
	machineID     uint64
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
	return (ts & 0x01ffffffffff) | ((s.machineID & 0x03ff) << 41) | ((overflow & 0x0fff) << 51)
}

func (s *Snowflake) Init(machineID string) {
	hash := sha256.Sum256([]byte(machineID))
	s.machineID = binary.BigEndian.Uint64(hash[:])
}
