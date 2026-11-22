package core

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunNetworkTCPProbeReachableEndpoint(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()

	var output strings.Builder
	if err := RunNetworkTCPProbe(listener.Addr().String(), 3*time.Second, &output); err != nil {
		t.Fatalf("RunNetworkTCPProbe() error = %v; output=%s", err, output.String())
	}
	if !strings.Contains(output.String(), "TCP OK:") || !strings.Contains(output.String(), "reachable proxy endpoints: 1/1") {
		t.Fatalf("unexpected preflight output: %s", output.String())
	}
}

func TestRunNetworkTCPProbeRejectsUnreachableEndpoint(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	var output strings.Builder
	err = RunNetworkTCPProbe(address, 2*time.Second, &output)
	if err == nil || !strings.Contains(err.Error(), "none of 1 sampled proxy endpoints") {
		t.Fatalf("RunNetworkTCPProbe() error = %v, want unreachable endpoint error", err)
	}
	if !strings.Contains(output.String(), "TCP FAIL:") {
		t.Fatalf("preflight output missing failure details: %s", output.String())
	}
}

func TestRunNetworkTCPProbeRejectsInvalidEndpoint(t *testing.T) {
	if err := RunNetworkTCPProbe("not-an-endpoint", time.Second, nil); err == nil {
		t.Fatal("RunNetworkTCPProbe() accepted an invalid endpoint")
	}
}

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
