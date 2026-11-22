package check

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloudflareNetworkQualityTestCollectsIdleAndLoadedMetrics(t *testing.T) {
	var idleSequence atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server-Timing", "cfReqDuration;dur=0.1")
		switch r.Method {
		case http.MethodGet:
			bytesValue, _ := strconv.Atoi(r.URL.Query().Get("bytes"))
			if bytesValue == 0 {
				delay := time.Duration(3+idleSequence.Add(1)%3) * time.Millisecond
				time.Sleep(delay)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(bytesValue))
			w.WriteHeader(http.StatusOK)
			remaining := bytesValue
			chunk := make([]byte, 4096)
			for remaining > 0 {
				n := len(chunk)
				if remaining < n {
					n = remaining
				}
				if _, err := w.Write(chunk[:n]); err != nil {
					return
				}
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				remaining -= n
				time.Sleep(8 * time.Millisecond)
			}
		case http.MethodPost:
			buffer := make([]byte, 4096)
			for {
				n, err := r.Body.Read(buffer)
				if n > 0 {
					time.Sleep(8 * time.Millisecond)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return
				}
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client := server.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	quality, err := CloudflareNetworkQualityTest(
		ctx, client,
		server.URL+"/__down?bytes={bytes}",
		server.URL+"/__up",
		128*1024, 128*1024,
		4, 20*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("CloudflareNetworkQualityTest() error = %v", err)
	}
	if quality.LatencyMS <= 0 {
		t.Errorf("unloaded latency = %.2f ms, want > 0", quality.LatencyMS)
	}
	if quality.JitterMS <= 0 {
		t.Errorf("unloaded jitter = %.2f ms, want > 0", quality.JitterMS)
	}
	if quality.DownloadLoadedLatencyMS <= 0 || quality.DownloadLoadedJitterMS <= 0 {
		t.Errorf("download-loaded metrics are incomplete: %+v", quality)
	}
	if quality.UploadLoadedLatencyMS <= 0 || quality.UploadLoadedJitterMS <= 0 {
		t.Errorf("upload-loaded metrics are incomplete: %+v", quality)
	}
}

func TestCloudflareServerDurationParsesRequestAndSpeedTiming(t *testing.T) {
	cases := []struct {
		header string
		want   float64
	}{
		{header: "cfReqDuration;dur=4.2", want: 4.2},
		{header: "cfSpeed;dur=1.5, cfSpeedWorker;dur=2.5", want: 4},
		{header: "unknown;dur=9", want: 0},
	}
	for _, tc := range cases {
		if got := cloudflareServerDuration(tc.header); got != tc.want {
			t.Errorf("cloudflareServerDuration(%q) = %.2f, want %.2f", tc.header, got, tc.want)
		}
	}
}

func TestCloudflareNetworkQualityRejectsInvalidSettings(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	_, err := CloudflareNetworkQualityTest(
		context.Background(), client,
		"https://speed.cloudflare.com/__down?bytes={bytes}",
		"https://speed.cloudflare.com/__up",
		64*1024, 64*1024, 2, 100*time.Millisecond,
	)
	if err == nil || !strings.Contains(err.Error(), "latency_samples") {
		t.Fatalf("invalid sample count error = %v", err)
	}
}
