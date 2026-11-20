package check

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestSpeedTestWithProgressReportsBothDirections(t *testing.T) {
	const payloadSize = 512 * 1024
	payload := strings.Repeat("x", payloadSize)
	var uploadMu sync.Mutex
	uploaded := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download":
			w.Header().Set("Content-Length", "524288")
			_, _ = io.WriteString(w, payload)
		case "/upload":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			uploadMu.Lock()
			uploaded = len(body)
			uploadMu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var mu sync.Mutex
	var events []SpeedProgress
	down, up, err := SpeedTestWithProgress(
		context.Background(), server.Client(),
		server.URL+"/download?bytes={bytes}", server.URL+"/upload",
		payloadSize, payloadSize,
		func(event SpeedProgress) {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	)
	if err != nil {
		t.Fatalf("SpeedTestWithProgress: %v", err)
	}
	if down <= 0 || up <= 0 {
		t.Fatalf("expected positive speeds, down=%f up=%f", down, up)
	}
	uploadMu.Lock()
	gotUploaded := uploaded
	uploadMu.Unlock()
	if gotUploaded != payloadSize {
		t.Fatalf("uploaded %d bytes, want %d", gotUploaded, payloadSize)
	}

	mu.Lock()
	defer mu.Unlock()
	seenDownload, seenUpload := false, false
	for _, event := range events {
		if strings.HasPrefix(event.Phase, "download") && event.CompletedBytes > 0 {
			seenDownload = true
		}
		if strings.HasPrefix(event.Phase, "upload") && event.CompletedBytes > 0 {
			seenUpload = true
		}
	}
	if !seenDownload || !seenUpload {
		t.Fatalf("expected live progress for download and upload, got %#v", events)
	}
}
