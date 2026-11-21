package check

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"net/http"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

func TestReadGeminiBodyDecodesBrowserEncodings(t *testing.T) {
	payload := []byte(`{"45631641":true,"ok":"Gemini page"}`)

	encode := func(t *testing.T, writer func(*bytes.Buffer) (io.WriteCloser, error)) []byte {
		t.Helper()
		var buffer bytes.Buffer
		compressor, err := writer(&buffer)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := compressor.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := compressor.Close(); err != nil {
			t.Fatal(err)
		}
		return buffer.Bytes()
	}
	zstdBytes := func(t *testing.T) []byte {
		t.Helper()
		var buffer bytes.Buffer
		compressor, err := zstd.NewWriter(&buffer, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := compressor.Write(payload); err != nil {
			t.Fatal(err)
		}
		compressor.Close()
		return buffer.Bytes()
	}

	tests := []struct {
		name     string
		encoding string
		body     []byte
	}{
		{name: "identity", encoding: "identity", body: payload},
		{name: "gzip", encoding: "gzip", body: encode(t, func(b *bytes.Buffer) (io.WriteCloser, error) { return gzip.NewWriter(b), nil })},
		{name: "deflate", encoding: "deflate", body: encode(t, func(b *bytes.Buffer) (io.WriteCloser, error) { return zlib.NewWriter(b), nil })},
		{name: "raw deflate", encoding: "deflate", body: encode(t, func(b *bytes.Buffer) (io.WriteCloser, error) { return flate.NewWriter(b, flate.DefaultCompression) })},
		{name: "brotli", encoding: "br", body: encode(t, func(b *bytes.Buffer) (io.WriteCloser, error) { return brotli.NewWriter(b), nil })},
		{name: "zstd", encoding: "zstd", body: zstdBytes(t)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := &http.Response{
				Header: http.Header{"Content-Encoding": []string{test.encoding}},
				Body:   io.NopCloser(bytes.NewReader(test.body)),
			}
			got, err := readGeminiBody(response, 1024)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("decoded body = %q, want %q", got, payload)
			}
		})
	}
}

func TestReadGeminiBodyRejectsOversizedDecodedResponse(t *testing.T) {
	response := &http.Response{
		Header: http.Header{"Content-Encoding": []string{"identity"}},
		Body:   io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), 32))),
	}
	if _, err := readGeminiBody(response, 16); err == nil {
		t.Fatal("expected oversized response to be rejected")
	}
}
