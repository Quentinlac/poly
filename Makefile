.PHONY: build run clean deps test

# Build the application
build:
	GOWORK=off go build -o polymarket-analyzer .

# Run the application
run:
	GOWORK=off go run main.go

# Install dependencies
deps:
	GOWORK=off go mod download
	GOWORK=off go mod tidy

# Clean build artifacts
clean:
	rm -f polymarket-analyzer

# Run tests (when tests are added)
test:
	GOWORK=off go test ./...

# Format code
fmt:
	GOWORK=off go fmt ./...

# Lint code (requires golangci-lint)
lint:
	golangci-lint run

