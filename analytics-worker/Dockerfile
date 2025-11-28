# Build stage
# NOTE: Qovery uses repo root as build context, so paths are relative to repo root
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the analytics worker binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o analytics-worker ./analytics-worker

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy binary from builder
COPY --from=builder /app/analytics-worker .

# Copy config
COPY --from=builder /app/config ./config

# Create data directory
RUN mkdir -p /app/data && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Run the analytics worker
CMD ["./analytics-worker"]
