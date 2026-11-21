package check

import (
	"bufio"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
	"github.com/metacubex/utls"
	"golang.org/x/net/http2"
)

// NewGeminiClient creates the browser-facing transport used by Throne's
// service checker, without changing the regular URL/speed-test transport.
// The HTTP CONNECT tunnel is provided by this node's sing-box mixed inbound.
func NewGeminiClient(proxyURL string, timeout time.Duration) (*http.Client, error) {
	proxy, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("parse Gemini proxy URL: %w", err)
	}
	if proxy.Scheme != "http" || proxy.Host == "" || (proxy.Path != "" && proxy.Path != "/") || proxy.RawQuery != "" || proxy.Fragment != "" {
		return nil, fmt.Errorf("Gemini proxy must be an HTTP proxy URL")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("create Gemini cookie jar: %w", err)
	}
	transport := &geminiTransport{
		proxy:   proxy,
		h2:      &http2.Transport{DisableCompression: true},
		h2Conns: make(map[string]*http2.ClientConn),
	}
	return &http.Client{
		Transport: transport,
		Jar:       jar,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}, nil
}

type geminiTransport struct {
	proxy   *url.URL
	h2      *http2.Transport
	mu      sync.Mutex
	h2Conns map[string]*http2.ClientConn
	conns   []net.Conn
	closed  bool
}

func (t *geminiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil || req.URL.Scheme != "https" {
		return nil, fmt.Errorf("Gemini browser transport only supports HTTPS")
	}
	host := strings.ToLower(strings.TrimSuffix(req.URL.Hostname(), "."))
	if host == "" {
		return nil, fmt.Errorf("Gemini request has no hostname")
	}
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	address := net.JoinHostPort(host, port)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, fmt.Errorf("Gemini transport is closed")
	}
	if cc := t.h2Conns[host]; cc != nil {
		if cc.CanTakeNewRequest() {
			if response, err := cc.RoundTrip(req); err == nil {
				return response, nil
			}
		}
		_ = cc.Close()
		delete(t.h2Conns, host)
	}

	conn, err := t.dialTLS(req.Context(), host, address)
	if err != nil {
		return nil, err
	}
	t.conns = append(t.conns, conn)
	if conn.ConnectionState().NegotiatedProtocol == "h2" {
		// HTTP/2 manages per-stream deadlines through request contexts; a
		// socket-wide deadline would incorrectly kill later requests on this
		// reusable connection.
		_ = conn.SetDeadline(time.Time{})
		cc, err := t.h2.NewClientConn(conn)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("create HTTP/2 connection: %w", err)
		}
		t.h2Conns[host] = cc
		response, err := cc.RoundTrip(req)
		if err != nil {
			_ = cc.Close()
			delete(t.h2Conns, host)
			return nil, err
		}
		return response, nil
	}

	req.Close = true
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write HTTPS request: %w", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read HTTPS response: %w", err)
	}
	return response, nil
}

func (t *geminiTransport) dialTLS(ctx context.Context, host, address string) (*utls.UConn, error) {
	proxyAddress := t.proxy.Host
	if _, _, err := net.SplitHostPort(proxyAddress); err != nil {
		proxyPort := t.proxy.Port()
		if proxyPort == "" {
			proxyPort = "80"
		}
		proxyAddress = net.JoinHostPort(t.proxy.Hostname(), proxyPort)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, fmt.Errorf("connect to node's HTTP proxy: %w", err)
	}
	fail := func(err error) (*utls.UConn, error) {
		_ = conn.Close()
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	var connect strings.Builder
	fmt.Fprintf(&connect, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Connection: Keep-Alive\r\n", address, address)
	if t.proxy.User != nil {
		password, _ := t.proxy.User.Password()
		credentials := t.proxy.User.Username() + ":" + password
		fmt.Fprintf(&connect, "Proxy-Authorization: Basic %s\r\n", base64.StdEncoding.EncodeToString([]byte(credentials)))
	}
	connect.WriteString("\r\n")
	if _, err := io.WriteString(conn, connect.String()); err != nil {
		return fail(fmt.Errorf("send HTTP CONNECT: %w", err))
	}

	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return fail(fmt.Errorf("read HTTP CONNECT response: %w", err))
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		_ = response.Body.Close()
		return fail(fmt.Errorf("HTTP CONNECT rejected: %s: %s", response.Status, strings.TrimSpace(string(body))))
	}
	_ = response.Body.Close()

	var tunnel net.Conn = conn
	if reader.Buffered() > 0 {
		tunnel = &bufferedConn{Conn: conn, reader: reader}
	}
	tlsConn := utls.UClient(tunnel, &utls.Config{
		ServerName: host,
		NextProtos: []string{"h2", "http/1.1"},
	}, utls.HelloChrome_Auto)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fail(fmt.Errorf("Chrome TLS handshake: %w", err))
	}
	return tlsConn, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (t *geminiTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	for _, cc := range t.h2Conns {
		_ = cc.Close()
	}
	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.h2Conns = nil
	t.conns = nil
}

func (t *geminiTransport) CloseIdleConnections() {
	t.Close()
}

func readGeminiBody(response *http.Response, maxBytes int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("empty Gemini response body")
	}
	var reader io.Reader = response.Body
	closeReader := func() {}
	encoding := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Encoding")))

	switch encoding {
	case "", "identity":
	case "gzip", "x-gzip":
		decoder, err := gzip.NewReader(response.Body)
		if err != nil {
			return nil, fmt.Errorf("decode gzip response: %w", err)
		}
		reader = decoder
		closeReader = func() { _ = decoder.Close() }
	case "br":
		reader = brotli.NewReader(response.Body)
	case "zstd":
		decoder, err := zstd.NewReader(response.Body, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxWindow(8<<20))
		if err != nil {
			return nil, fmt.Errorf("decode zstd response: %w", err)
		}
		reader = decoder
		closeReader = decoder.Close
	case "deflate":
		buffered := bufio.NewReader(response.Body)
		header, _ := buffered.Peek(2)
		if len(header) == 2 && header[0]&0x0f == 8 && (uint16(header[0])<<8|uint16(header[1]))%31 == 0 {
			decoder, err := zlib.NewReader(buffered)
			if err != nil {
				return nil, fmt.Errorf("decode deflate response: %w", err)
			}
			reader = decoder
			closeReader = func() { _ = decoder.Close() }
		} else {
			decoder := flate.NewReader(buffered)
			reader = decoder
			closeReader = func() { _ = decoder.Close() }
		}
	default:
		return nil, fmt.Errorf("unsupported Gemini content encoding %q", encoding)
	}
	defer closeReader()

	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Gemini response: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("Gemini response exceeds %d decoded bytes", maxBytes)
	}
	return body, nil
}
