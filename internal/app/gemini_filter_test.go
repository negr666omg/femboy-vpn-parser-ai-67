package app

import (
	"testing"

	"github.com/negr666omg/subwatch/internal/sub"
)

func TestGeminiAvailableExportGate(t *testing.T) {
	tests := []struct {
		name       string
		status     string
		serviceErr string
		want       bool
	}{
		{name: "explicit available", status: "available", want: true},
		{name: "partial is not available", status: "partial"},
		{name: "unknown is not available", status: "unknown"},
		{name: "blocked is not available", status: "blocked"},
		{name: "failed is not available", status: "failed"},
		{name: "available with service error is rejected", status: "available", serviceErr: "response read error"},
		{name: "empty status is rejected"},
		{name: "case mismatch is rejected", status: "Available"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := sub.Node{GeminiStatus: test.status, ServiceError: test.serviceErr}
			if got := geminiAvailable(node); got != test.want {
				t.Fatalf("geminiAvailable(%q, %q) = %v, want %v", test.status, test.serviceErr, got, test.want)
			}
		})
	}
}
