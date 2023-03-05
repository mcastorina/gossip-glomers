package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Key     string `json:"key"`
			Message int    `json:"msg"`
		}](msg)
		if err != nil {
			return err
		}
		// TODO: Handle body.
		// TODO: Reply with offset.
		return n.Reply(Ack(msg))
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Offsets map[string]int `json:"offsets"`
		}](msg)
		if err != nil {
			return err
		}
		// TODO: Handle body.
		// TODO: Reply with messages.
		return n.Reply(Ack(msg))
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Offsets map[string]int `json:"offsets"`
		}](msg)
		if err != nil {
			return err
		}
		// TODO: Handle body.
		return n.Reply(Ack(msg))
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		body, err := Parse[struct {
			Keys []string `json:"keys"`
		}](msg)
		if err != nil {
			return err
		}
		// TODO: Handle body.
		// TODO: Reply with commit offsets.
		return n.Reply(Ack(msg))
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
