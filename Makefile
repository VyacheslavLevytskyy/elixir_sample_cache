.PHONY: all compile

all: compile

deps:
	@mix deps.get

compile: deps
	@mix compile

run: compile test
	@iex -S mix

test: compile
	@mix test

clean:
	@mix clean

dialyzer: compile
	@mix dialyzer
