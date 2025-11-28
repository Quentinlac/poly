package api

import "strings"

// Helper functions for outcome normalization

// isComplementOutcome checks if an outcome should be normalized (typically "No")
func isComplementOutcome(outcome string) bool {
	normalized := strings.ToLower(strings.TrimSpace(outcome))
	return normalized == "no"
}

// getComplementOutcome returns the complement of an outcome
func getComplementOutcome(outcome string) string {
	normalized := strings.ToLower(strings.TrimSpace(outcome))
	if normalized == "no" {
		return "Yes"
	}
	if normalized == "yes" {
		return "No"
	}
	// For non-binary markets, return as-is
	return outcome
}
