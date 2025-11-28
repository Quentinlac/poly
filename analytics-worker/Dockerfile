# Build stage
# Build context is analytics-worker/ directory
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy all source code (mirrored from parent)
COPY api/ ./api/
COPY config/ ./config/
COPY models/ ./models/
COPY service/ ./service/
COPY storage/ ./storage/
COPY syncer/ ./syncer/
COPY analyzer/ ./analyzer/
COPY utils/ ./utils/
COPY main.go ./

# Build the analytics worker binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o worker .

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

# Copy binary from builder
COPY --from=builder /app/worker .

# Copy config files
COPY config/ ./config/

# Create data directory
RUN mkdir -p /app/data && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Run the worker
CMD ["./worker"]
