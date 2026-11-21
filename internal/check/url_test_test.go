package check

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestURLTestAcceptsGenerate204StyleResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	passed, latency, err := URLTest(ctx, &http.Client{Timeout: time.Second}, server.URL)
	if err != nil {
		t.Fatalf("URLTest: %v", err)
	}
	if !passed {
		t.Fatal("HTTP 204 should pass URL test")
	}
	if latency < 0 {
		t.Fatalf("latency=%d, must not be negative", latency)
	}
}

func TestURLTestReportsHTTPFailureStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "blocked", http.StatusForbidden)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	passed, _, err := URLTest(ctx, &http.Client{Timeout: time.Second}, server.URL)
	if passed {
		t.Fatal("HTTP 403 must not pass URL test")
	}
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("expected diagnostic HTTP 403 error, got %v", err)
	}
}
