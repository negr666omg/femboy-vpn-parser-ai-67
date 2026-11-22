package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunNetworkProbeDirectHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	var output strings.Builder
	if err := RunNetworkProbe(server.URL, 3*time.Second, &output); err != nil {
		t.Fatalf("RunNetworkProbe() error = %v; output=%s", err, output.String())
	}
	for _, marker := range []string{"DNS OK:", "TCP OK:", "HTTP OK:"} {
		if !strings.Contains(output.String(), marker) {
			t.Fatalf("preflight output missing %q: %s", marker, output.String())
		}
	}
}

func TestRunNetworkProbeRejectsUnsuccessfulHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := RunNetworkProbe(server.URL, 3*time.Second, nil)
	if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable") {
		t.Fatalf("RunNetworkProbe() error = %v, want HTTP 503", err)
	}
}

func TestRunNetworkProbeRejectsInvalidURL(t *testing.T) {
	if err := RunNetworkProbe("not-a-url", time.Second, nil); err == nil {
		t.Fatal("RunNetworkProbe() accepted an invalid URL")
	}
}
