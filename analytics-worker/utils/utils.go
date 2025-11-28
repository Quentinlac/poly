// Package utils provides shared utility functions across the polymarket-analyzer project.
package utils

import (
	"strings"
)

// NormalizeAddress normalizes an Ethereum address to lowercase with trimmed spaces.
func NormalizeAddress(addr string) string {
	return strings.TrimSpace(strings.ToLower(addr))
}

// ShortAddress returns a truncated address for display (0x1234...5678).
func ShortAddress(addr string) string {
	if len(addr) <= 10 {
		return addr
	}
	return addr[:6] + "..." + addr[len(addr)-4:]
}

// NormalizeUserID is an alias for NormalizeAddress for backward compatibility.
func NormalizeUserID(id string) string {
	return NormalizeAddress(id)
}

// Min returns the minimum of two integers.
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Max returns the maximum of two integers.
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinFloat returns the minimum of two float64 values.
func MinFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// MaxFloat returns the maximum of two float64 values.
func MaxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
