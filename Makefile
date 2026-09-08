.PHONY: build test lint clean release install

GO ?= go
VERSION ?= $(shell cat VERSION 2>/dev/null || echo "dev")
LDFLAGS := -X main.version=$(VERSION)

# Default target: build the plugin for the current platform (macOS → trae.dylib).
build:
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -ldflags "$(LDFLAGS)" -o trae.dylib .

# Run all tests with race detector.
test:
	$(GO) test -race -count=1 ./...

# Lint everything (gofmt + go vet; staticcheck optional).
lint:
	@test -z "$$($(GO)fmt -l .)" || ($(GO)fmt -l . && exit 1)
	$(GO) vet ./...
	@if command -v staticcheck >/dev/null 2>&1; then staticcheck ./...; fi

# Clean build artifacts.
clean:
	rm -f trae.dylib trae.h
	rm -rf dist/ coverage.out coverage.html

# Copy the built plugin into the CPA plugins dir.
# NOTE: does NOT restart the host — reload is a manual cutover step.
install: build
	@echo "built trae.dylib ($(VERSION)); copy to ~/cliproxyapi/plugins/ manually"

# Cross-compile release (linux only — darwin needs a native build).
release: clean
	@mkdir -p dist
	@for target in linux/amd64 linux/arm64; do \
	  os=$${target%/*}; arch=$${target#*/}; \
	  out=dist/trae_$(VERSION)_$${os}_$${arch}; \
	  mkdir -p $$out; \
	  GOOS=$$os GOARCH=$$arch CGO_ENABLED=1 $(GO) build -buildmode=c-shared -ldflags "$(LDFLAGS)" -o $$out/trae.so . 2>&1 | grep -v "cgo is not enabled" || true; \
	  if [ -f $$out/trae.so ]; then \
	    cp README.md $$out/ 2>/dev/null || true; \
	    echo "built $$out"; \
	  fi; \
	done

tag:
	git tag -a v$(VERSION) -m "v$(VERSION)"
