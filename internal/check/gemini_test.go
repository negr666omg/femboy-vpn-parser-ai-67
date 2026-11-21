package check

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestClassifyGemini(t *testing.T) {
	cases := []struct {
		name, url, body, want string
		code                  int
	}{
		{"full flag escaped marker", "https://gemini.google.com/app?hl=en", `x45631641\":true y`, "available", 200},
		{"full flag plain JSON marker", "https://gemini.google.com/app?hl=en", `{"45631641":true}`, "available", 200},
		{"full flag comma marker", "https://gemini.google.com/app?hl=en", `45631641,null,true`, "available", 200},
		{"partial flag escaped marker", "https://gemini.google.com/app?hl=en", `x45631641,null,false y`, "partial", 200},
		{"partial flag plain JSON marker", "https://gemini.google.com/app?hl=en", `{"45631641":false}`, "partial", 200},
		{"nearby marker is not a match", "https://gemini.google.com/app?hl=en", `{"145631641":true}`, "unknown", 200},
        {"nearby false marker is not a match", "https://gemini.google.com/app?hl=en", `{"145631641":false}`, "unknown", 200},
		{"consent", "https://consent.google.com/m?continue=gemini", `45631641":true`, "unknown", 200},
		{"blocked 403", "https://gemini.google.com/app", ``, "blocked", 403},
		{"rate limited", "https://gemini.google.com/app", ``, "blocked", 429},
		{"missing flag", "https://gemini.google.com/app", `ordinary HTML`, "unknown", 200},
		{"unexpected host", "https://accounts.google.com/", `45631641":true`, "unknown", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, _, _ := ClassifyGemini(tc.code, tc.url, []byte(tc.body))
			if status != tc.want {
				t.Fatalf("got %q, want %q", status, tc.want)
			}
		})
	}
}

func TestSpeedTestUsesConfiguredEndpointsAndExactPayloadSizes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download":
			n, err := strconv.Atoi(r.URL.Query().Get("bytes"))
			if err != nil || n != 256*1024 {
				http.Error(w, "bad bytes query", http.StatusBadRequest)
				return
			}
			_, _ = w.Write(make([]byte, n))
		case "/upload":
			n, err := io.Copy(io.Discard, r.Body)
			if err != nil || n != 256*1024 {
				http.Error(w, "bad upload size", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := server.Client()
	down, up, err := SpeedTest(context.Background(), client,
		server.URL+"/download?bytes={bytes}", server.URL+"/upload", 256*1024, 256*1024)
	if err != nil {
		t.Fatal(err)
	}
	if down <= 0 || up <= 0 {
		t.Fatalf("expected positive rates, got download=%f upload=%f", down, up)
	}
}

func TestSpeedTestRejectsShortDownload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/download" {
			_, _ = w.Write(make([]byte, 1024))
			return
		}
		t.Errorf("upload must not start after incomplete download")
	}))
	defer server.Close()

	_, _, err := SpeedTest(context.Background(), server.Client(), server.URL+"/download", server.URL+"/upload", 256*1024, 256*1024)
	if err == nil || !strings.Contains(err.Error(), "received 1024/262144 bytes") {
		t.Fatalf("expected incomplete-download error, got %v", err)
	}
}
