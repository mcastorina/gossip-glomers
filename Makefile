MAELSTROM_DIR := ./maelstrom
MAELSTROM     := PATH="/opt/homebrew/opt/openjdk/bin:$$PATH" ./maelstrom

.PHONY: debug
debug:
	cd $(MAELSTROM_DIR) && $(MAELSTROM) serve

1-echo/echo: $(shell find 1-echo/ -name '*.go')
	go build -C ./1-echo -o echo

.PHONY: echo
echo: 1-echo/echo

.PHONY: echo-test
test-echo: 1-echo/echo
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w echo --bin ../$< --node-count 1 --time-limit 10

2-unique-ids/unique-ids: $(shell find 2-unique-ids/ -name '*.go')
	go build -C ./2-unique-ids -o unique-ids

.PHONY: unique-ids
unique-ids: 2-unique-ids/unique-ids

.PHONY: unique-ids-test
test-unique-ids: 2-unique-ids/unique-ids
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w unique-ids --bin ../$< --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
