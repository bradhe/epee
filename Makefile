SHELL							:= /bin/bash
TMP								:= /tmp

GO 								:= /usr/local/go/bin/go
VERSION          	:= $(shell grep Version version.go | sed -e 's/\"//g' -e 's/const Version = //')

PROTOC						:= /usr/local/bin/protoc
PROTOC_VERSION 		:= 2.5.0
PROTOC_PREFIX			:= /usr

build: clean generate_proto
	$(GO) build ./...

setup:
	@if [ ! -e $(PROTOC) ]; \
	then \
		cd $(TMP) && \
		wget https://protobuf.googlecode.com/files/protobuf-$(PROTOC_VERSION).tar.gz && \
		tar -xzvf protobuf-$(PROTOC_VERSION).tar.gz && \
		cd protobuf-$(PROTOC_VERSION) && \
			./configure --prefix=$(PROTOC_PREFIX) && \
			make && \
			sudo make install; \
	fi
	go get ./...

generate_proto:
	$(PROTOC) --go_out=. ./test_types.proto

clean:
	$(GO) clean
	rm -f *.pb.go

test: build
	$(GO) test
