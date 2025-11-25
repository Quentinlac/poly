# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod ./
COPY go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the analytics worker binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o analytics-worker ./cmd/analytics-worker

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy the binary
COPY --from=builder /app/analytics-worker .

# Set timezone
ENV TZ=UTC

# Run the binary
CMD ["./analytics-worker"]
