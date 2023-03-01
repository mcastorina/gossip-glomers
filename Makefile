MAELSTROM_DIR := ./maelstrom 
MAELSTROM     := PATH="/opt/homebrew/opt/openjdk/bin:$$PATH" ./maelstrom 

1-echo/echo: $(shell find 1-echo/ -name '*.go')
	go build -C ./1-echo -o echo

.PHONY: echo
echo: 1-echo/echo

.PHONY: echo-test
test-echo: 1-echo/echo
	cd $(MAELSTROM_DIR) && $(MAELSTROM) test -w echo --bin ../$< --node-count 1 --time-limit 10
