package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Our neighbor we send messages to.
	var friend string

	// Total list of seen messages.
	seenMessages := NewSet[int]()

	// Cache to batch messages.
	cache := NewCache[int]()
	defer cache.Wait()
	// Context for stopping the cache.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Broadcast is a message received from a client.
		type broadcastMsg struct {
			Kind    string `json:"type"`
			Message int    `json:"message"`
		}
		var body broadcastMsg
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Record the message and add it to our cache of batch messages.
		seenMessages.Add(body.Message)
		cache.Add(body.Message)
		return n.Reply(Ack(msg))
	})

	n.Handle("batch", func(msg maelstrom.Message) error {
		// Batch is an internal batched broadcast sent between nodes.
		var body BatchMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, message := range body.Messages {
			// If it's a new message, add it to our batch to send to our friend.
			if seenMessages.Add(message) {
				cache.Add(message)
			}
		}

		return n.Reply(Ack(msg))
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		return n.Reply(Ack(msg, "messages", seenMessages.Elements()))
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		num, err := strconv.Atoi(n.ID()[1:])
		if err != nil {
			panic(err)
		}
		// Set friend globally to be used by other handles.
		friend = fmt.Sprintf("n%d", (num+1)%len(n.NodeIDs()))
		cache.SetFlush(ctx, 10*time.Millisecond, func(elems []int) {
			// When the cache is flushed, send all elements to our friend.
			go Retry(ctx, func(ctx context.Context) error {
				body := BatchMessage{
					Kind:     "batch",
					Messages: elems,
				}
				ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
				defer cancel()
				_, err := n.SyncRPC(ctx, friend, body)
				return err
			})
		})
		return n.Reply(Ack(msg))
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type BatchMessage struct {
	Kind     string `json:"type"`
	Messages []int  `json:"messages"`
}

func NoopHandler(maelstrom.Message) error { return nil }
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

func (s *Set[T]) Clear() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	elems := make([]T, 0, len(s.elements))
	for elem := range s.elements {
		elems = append(elems, elem)
	}
	s.elements = make(map[T]struct{})
	return elems
}

type Cache[T comparable] struct {
	elements Set[T]
	flush    func(elems []T)
	wg       sync.WaitGroup
}

func NewCache[T comparable]() Cache[T] {
	return Cache[T]{elements: NewSet[T]()}
}

func (c *Cache[T]) SetFlush(ctx context.Context, timeout time.Duration, flush func([]T)) {
	c.flush = flush
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.worker(ctx, timeout)
	}()
}

func (c *Cache[T]) Add(t T) {
	c.elements.Add(t)
}

func (c *Cache[T]) Flush() {
	if c.flush == nil {
		return
	}
	// Clear the set and call flush if there were any elements.
	if elems := c.elements.Clear(); len(elems) > 0 {
		c.flush(elems)
	}
}

func (c *Cache[T]) Wait() {
	c.wg.Wait()
}

func (c *Cache[T]) worker(ctx context.Context, timeout time.Duration) {
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.Flush()
			return
		case <-ticker.C:
			c.Flush()
		}
	}
}
