MAELSTROM_DIR := ./maelstrom
MAELSTROM     := PATH="/opt/homebrew/opt/openjdk/bin:$$PATH" ./maelstrom

# Use an impossible comparison to take advantage of top-level tab
# completion.
ifneq (0, 0)
NODES        = 1
TIME         = 10
RATE         = 10
LATENCY      = 10
CONCURRENCY  = 1n
endif

.PHONY: debug
debug:
	cd $(MAELSTROM_DIR) && $(MAELSTROM) serve

################################################################################

1-echo/echo: $(shell find 1-echo/ -name '*.go')
	go build -C ./1-echo -o echo

.PHONY: echo
echo: 1-echo/echo

.PHONY: echo-test
test-echo: 1-echo/echo
	$(eval NODES ?= 1)
	$(eval TIME  ?= 10)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w echo --bin ../$< \
		--node-count $(NODES) --time-limit $(TIME)

################################################################################

2-unique-ids/unique-ids: $(shell find 2-unique-ids/ -name '*.go')
	go build -C ./2-unique-ids -o unique-ids

.PHONY: unique-ids
unique-ids: 2-unique-ids/unique-ids

.PHONY: unique-ids-test
test-unique-ids: 2-unique-ids/unique-ids
	$(eval NODES ?= 3)
	$(eval TIME  ?= 30)
	$(eval RATE  ?= 1000)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w unique-ids --bin ../$< \
		--time-limit $(TIME) --rate $(RATE) --node-count $(NODES) \
		--availability total --nemesis partition

################################################################################

3-broadcast/broadcast: $(shell find 3-broadcast/ -name '*.go')
	go build -C ./3-broadcast -o broadcast

.PHONY: broadcast
broadcast: 3-broadcast/broadcast

.PHONY: broadcast-test
test-broadcast: 3-broadcast/broadcast
	$(eval NODES ?= 1)
	$(eval TIME  ?= 20)
	$(eval RATE  ?= 10)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w broadcast --bin ../$< \
		--node-count $(NODES) --time-limit $(TIME) --rate $(RATE)

.PHONY: broadcast-test-partition
test-broadcast-partition: 3-broadcast/broadcast
	$(eval NODES ?= 5)
	$(eval TIME  ?= 20)
	$(eval RATE  ?= 10)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w broadcast --bin ../$< \
		--node-count $(NODES) --time-limit $(TIME) --rate $(RATE) \
		--nemesis partition

.PHONY: broadcast-test-partition
test-broadcast-latency: 3-broadcast/broadcast
	$(eval NODES   ?= 25)
	$(eval TIME    ?= 20)
	$(eval RATE    ?= 100)
	$(eval LATENCY ?= 100)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w broadcast --bin ../$< \
		--node-count $(NODES) --time-limit $(TIME) --rate $(RATE) \
		--latency $(LATENCY)

################################################################################

4-g-counter/g-counter: $(shell find 4-g-counter/ -name '*.go')
	go build -C ./4-g-counter -o g-counter

.PHONY: g-counter
g-counter: 4-g-counter/g-counter

.PHONY: g-counter-test
test-g-counter: 4-g-counter/g-counter
	$(eval NODES ?= 3)
	$(eval TIME  ?= 20)
	$(eval RATE  ?= 100)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w g-counter --bin ../$< \
		--node-count $(NODES) --time-limit $(TIME) --rate $(RATE) \
		--nemesis partition

################################################################################

5-kafka/kafka: $(shell find 5-kafka/ -name '*.go')
	go build -C ./5-kafka -o kafka

.PHONY: kafka
kafka: 5-kafka/kafka

.PHONY: kafka-test
test-kafka: 5-kafka/kafka
	$(eval NODES ?= 1)
	$(eval TIME  ?= 20)
	$(eval RATE  ?= 1000)
	$(eval CONCURRENCY ?= 2n)
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w kafka --bin ../$< \
		--node-count $(NODES) --time-limit $(TIME) --rate $(RATE) \
		--concurrency $(CONCURRENCY)
