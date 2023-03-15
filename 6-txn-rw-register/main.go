package main

import (
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := NewKV()

	n.Handle("txn", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Txn Transaction `json:"txn"`
		}](msg)
		if err != nil {
			return err
		}
		return n.Reply(Ack(msg, "txn", kv.Commit(body.Txn)))
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type Transaction []Operation
type Operation [3]any

type KV struct {
	mu   sync.Mutex
	data map[int]int
}

func NewKV() KV { return KV{data: make(map[int]int)} }

// Commit applies the operations as a whole. No concurrent reads or writes may
// happen while the transaction is being committed.
func (kv *KV) Commit(tx Transaction) Transaction {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ret := make(Transaction, len(tx))
	for i, op := range tx {
		ret[i] = kv.handle(op)
	}
	return ret
}

// handle a single operation. The KV is expected to be locked when this
// function is called.
func (kv *KV) handle(op Operation) Operation {
	if op.IsRead() {
		key := op.Key()
		var val any
		if v, ok := kv.data[key]; ok {
			val = v
		}
		return [3]any{"r", key, val}
	}
	if op.IsWrite() {
		key := op.Key()
		val := op.Value()
		kv.data[key] = val
		return [3]any{"w", key, val}
	}
	panic("unrecognized operation")
}

func (o Operation) IsRead() bool {
	s, ok := o[0].(string)
	return ok && s == "r"
}

func (o Operation) IsWrite() bool {
	s, ok := o[0].(string)
	return ok && s == "w"
}

func (o Operation) Key() int {
	return int(o[1].(float64))
}

func (o Operation) Value() int {
	return int(o[2].(float64))
}
