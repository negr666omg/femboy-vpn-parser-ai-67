package check

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOoklaSpeedTestAutomaticallySelectsFastestReachableServer(t *testing.T) {
	var fastDownloads, fastUploads, slowDownloads, slowUploads atomic.Int32
	payload := strings.Repeat("x", 64*1024)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/js/servers":
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `[
				{"url":%q,"name":"Slow City","country":"Testland","sponsor":"Slow ISP","id":"1"},
				{"url":%q,"name":"Fast City","country":"Testland","sponsor":"Fast ISP","id":"2"}
			]`, "http://"+r.Host+"/slow/upload.php", "http://"+r.Host+"/fast/upload.php")
		case "/slow/latency.txt":
			time.Sleep(70 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		case "/fast/latency.txt":
			w.WriteHeader(http.StatusOK)
		case "/slow/random1000x1000.jpg":
			slowDownloads.Add(1)
			_, _ = io.WriteString(w, payload)
		case "/fast/random1000x1000.jpg":
			fastDownloads.Add(1)
			_, _ = io.WriteString(w, payload)
		case "/slow/upload.php":
			slowUploads.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusNoContent)
		case "/fast/upload.php":
			fastUploads.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var events []SpeedProgress
	down, up, err := ooklaSpeedTestWithEndpoint(context.Background(), server.Client(), server.URL+"/api/js/servers", 120*time.Millisecond, func(event SpeedProgress) {
		events = append(events, event)
	})
	if err != nil {
		t.Fatalf("Ookla speed test failed: %v", err)
	}
	if down <= 0 || up <= 0 {
		t.Fatalf("expected positive throughput, got download=%f upload=%f", down, up)
	}
	if fastDownloads.Load() == 0 || fastUploads.Load() == 0 {
		t.Fatalf("automatically selected server was not used for both directions: fast downloads=%d uploads=%d", fastDownloads.Load(), fastUploads.Load())
	}
	if slowDownloads.Load() != 0 || slowUploads.Load() != 0 {
		t.Fatalf("slower server should only be probed, not selected: downloads=%d uploads=%d", slowDownloads.Load(), slowUploads.Load())
	}
	seenDownload, seenUpload := false, false
	for _, event := range events {
		if event.Phase == "download" && event.DownloadMbps > 0 {
			seenDownload = true
		}
		if event.Phase == "upload" && event.UploadMbps > 0 {
			seenUpload = true
		}
	}
	if !seenDownload || !seenUpload {
		t.Fatalf("expected final progress events with throughput, got %#v", events)
	}
}

func TestOoklaSpeedTestRejectsEmptyServerList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]")
	}))
	defer server.Close()

	_, _, err := ooklaSpeedTestWithEndpoint(context.Background(), server.Client(), server.URL, 100*time.Millisecond, nil)
	if err == nil || !strings.Contains(err.Error(), "empty server list") {
		t.Fatalf("expected empty server-list error, got %v", err)
	}
}
