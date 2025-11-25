# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy parent go.mod and go.sum first
COPY go.mod go.sum ./

# Download parent module dependencies
RUN go mod download

# Copy everything else
COPY . .

# Build from analytics-worker directory
WORKDIR /app/analytics-worker

# Download analytics-worker specific dependencies
RUN go mod download

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o analytics-worker .

# Final stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/analytics-worker .

# Set timezone
ENV TZ=UTC

# Run the binary
CMD ["./analytics-worker"]
