package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var machineID uint64

func main() {
	n := maelstrom.NewNode()
	var generator Snowflake

	n.Handle("init", func(msg maelstrom.Message) error {
		type initBody struct {
			ID string `json:"node_id"`
		}
		var body initBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		hash := sha256.Sum256([]byte(body.ID))
		machineID = binary.BigEndian.Uint64(hash[:])
		return nil
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   generator.Next(),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
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
