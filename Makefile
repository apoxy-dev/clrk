SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

MODULE := github.com/apoxy-dev/clrk
GO_VERSION := 1.24
BUILDER_IMAGE := golang:$(GO_VERSION)-bookworm
BIN_DIR := $(CURDIR)/bin
LINUX_BIN_DIR := $(CURDIR)/bin/linux
DATA_DIR ?= $(HOME)/.clrk

# Image tags used by `clrk dev` and by docker-build.
CONTROLLER_IMAGE ?= ghcr.io/apoxy-dev/clrk-controller-manager:dev
WORKER_IMAGE     ?= ghcr.io/apoxy-dev/clrk-worker:dev
DEV_IMAGE        ?= ghcr.io/apoxy-dev/clrk-dev:latest

UNAME_ARCH := $(shell uname -m)
ifeq ($(UNAME_ARCH),x86_64)
  HOST_GOARCH := amd64
else ifeq ($(UNAME_ARCH),arm64)
  HOST_GOARCH := arm64
else ifeq ($(UNAME_ARCH),aarch64)
  HOST_GOARCH := arm64
else
  HOST_GOARCH := $(UNAME_ARCH)
endif

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: generate
generate: ## Run codegen (deepcopy, register, client, openapi)
	./codegen/update.sh

.PHONY: build
build: ## Build native binaries into bin/
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/clrk ./cmd/clrk
	go build -o $(BIN_DIR)/controller-manager ./cmd/controller-manager
	@if [ "$$(uname)" = "Linux" ]; then \
	  go build -o $(BIN_DIR)/worker ./cmd/worker; \
	else \
	  echo "skipping worker: linux-only"; \
	fi

.PHONY: build-linux
build-linux: ## Cross-compile linux binaries via a golang builder container (CGO enabled)
	mkdir -p $(LINUX_BIN_DIR)
	docker run --rm \
	  -v $(CURDIR):/src \
	  -w /src \
	  -e GOCACHE=/src/.gocache \
	  -e GOMODCACHE=/src/.gomodcache \
	  -e CGO_ENABLED=1 \
	  -e GOOS=linux \
	  -e GOARCH=$(HOST_GOARCH) \
	  $(BUILDER_IMAGE) \
	  bash -c 'apt-get update -qq && apt-get install -qq -y libseccomp-dev >/dev/null && \
	    go build -o bin/linux/controller-manager ./cmd/controller-manager && \
	    go build -o bin/linux/worker ./cmd/worker'

.PHONY: docker-build
docker-build: ## Build all docker images
	docker build -f build/docker/Dockerfile.controller-manager -t $(CONTROLLER_IMAGE) .
	docker build -f build/docker/Dockerfile.worker -t $(WORKER_IMAGE) .
	docker build -f build/docker/Dockerfile.dev -t $(DEV_IMAGE) build/docker/

.PHONY: dev
dev: build ## Run `clrk dev`
	$(BIN_DIR)/clrk dev

.PHONY: test
test: ## Run tests
	go test ./...

.PHONY: lint
lint: ## Run go vet
	go vet ./...

.PHONY: tidy
tidy: ## go mod tidy
	go mod tidy

.PHONY: clean
clean: ## Remove built artifacts
	rm -rf $(BIN_DIR) .gocache .gomodcache
