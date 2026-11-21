package core

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	coreVersion   = "1.14.4"
	// Keep local process readiness independent from URL-test timeout without
	// turning a broken outbound into a 20-second delay for every node.
	startupTimeout = 8 * time.Second
)

type Instance struct {
	ProxyURL string
	Dialer   *net.Dialer
	cmd      *exec.Cmd
	done     chan error
	tmpDir   string
}

func FindOrDownload(binary string) (string, error) {
	return FindOrDownloadProgressContext(context.Background(), binary, io.Discard)
}

func FindOrDownloadProgress(binary string, progress io.Writer) (string, error) {
	return FindOrDownloadProgressContext(context.Background(), binary, progress)
}

// FindOrDownloadProgressContext prepares sing-box while respecting cancellation.
// A completed binary is installed with an atomic rename so concurrent runs
// cannot observe a partially extracted executable.
func FindOrDownloadProgressContext(ctx context.Context, binary string, progress io.Writer) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if progress == nil {
		progress = io.Discard
	}
	if binary != "" {
		p, err := exec.LookPath(binary)
		if err != nil {
			return "", err
		}
		return p, nil
	}
	if p, err := exec.LookPath("sing-box"); err == nil {
		fmt.Fprintf(progress, "[prepare] found sing-box in PATH: %s\n", p)
		return p, nil
	}
	if runtime.GOOS != "linux" {
		return "", fmt.Errorf("sing-box not found; automatic download currently supports Linux only")
	}
	arch := runtime.GOARCH
	asset, hash := map[string]string{"amd64": "sing-box-1.14.4-linux-amd64.tar.gz", "arm64": "sing-box-1.14.4-linux-arm64.tar.gz"}[arch], map[string]string{"amd64": "699a1b510aa4396b2176d32d1181920399ebf1e267835da6f3b598d0f7c24968", "arm64": "436441f76ee2f7fe21b5b2d426d3cb261a480455a704865789ad533e4273ec26"}[arch]
	if asset == "" {
		return "", fmt.Errorf("unsupported architecture %s; install sing-box manually", arch)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(cache, "subwatch", "sing-box-"+coreVersion)
	bin := filepath.Join(dir, "sing-box")
	if st, err := os.Stat(bin); err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0111 != 0 {
		check := exec.CommandContext(ctx, bin, "version")
		if e := check.Run(); e == nil {
			fmt.Fprintf(progress, "[prepare] using cached sing-box: %s\n", bin)
			return bin, nil
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		_ = os.Remove(bin)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	url := "https://github.com/SagerNet/sing-box/releases/download/v" + coreVersion + "/" + asset
	fmt.Fprintf(progress, "[prepare] downloading sing-box %s for linux/%s...\n", coreVersion, arch)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "SubWatch/0.1")
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("download sing-box: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download sing-box: HTTP %s", resp.Status)
	}
	tmp, err := os.CreateTemp(dir, "sing-box-*.tgz")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	if _, err = io.Copy(io.MultiWriter(tmp, h), &progressReader{reader: io.LimitReader(resp.Body, 80<<20), writer: progress, next: 2 * (1024 * 1024)}); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("download sing-box: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	fmt.Fprintln(progress, "[prepare] download complete; verifying SHA-256...")
	if got := hex.EncodeToString(h.Sum(nil)); got != hash {
		return "", fmt.Errorf("sing-box checksum mismatch: got %s", got)
	}
	f, err := os.Open(tmpName)
	if err != nil {
		return "", err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	found := false
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		hdr, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return "", e
		}
		if filepath.Base(hdr.Name) != "sing-box" || hdr.Typeflag != tar.TypeReg {
			continue
		}
		if hdr.Size <= 0 || hdr.Size > 100<<20 {
			return "", fmt.Errorf("sing-box archive entry has invalid size: %d bytes", hdr.Size)
		}
		out, e := os.CreateTemp(dir, "sing-box-bin-*.tmp")
		if e != nil {
			return "", e
		}
		outName := out.Name()
		defer os.Remove(outName)
		if e = out.Chmod(0700); e != nil {
			_ = out.Close()
			return "", e
		}
		written, copyErr := io.CopyN(out, tr, hdr.Size)
		if copyErr != nil {
			_ = out.Close()
			return "", copyErr
		}
		if written != hdr.Size {
			_ = out.Close()
			return "", fmt.Errorf("incomplete sing-box archive entry: %d/%d bytes", written, hdr.Size)
		}
		if e = out.Sync(); e != nil {
			_ = out.Close()
			return "", e
		}
		if e = out.Close(); e != nil {
			return "", e
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := os.Rename(outName, bin); err != nil {
			return "", err
		}
		found = true
		break
	}
	if !found {
		return "", errors.New("sing-box binary not found in release archive")
	}
	fmt.Fprintln(progress, "[prepare] sing-box extracted and ready")
	return bin, nil
}

type progressReader struct {
	reader io.Reader
	writer io.Writer
	read   int64
	next   int64
}

func (p *progressReader) Read(buf []byte) (int, error) {
	n, err := p.reader.Read(buf)
	p.read += int64(n)
	if p.read >= p.next && n > 0 {
		fmt.Fprintf(p.writer, "[prepare] downloaded %.1f MiB\n", float64(p.read)/(1024*1024))
		p.next = p.read + 2*(1024*1024)
	}
	return n, err
}

type cappedLog struct {
	buffer bytes.Buffer
	limit  int
}

func (l *cappedLog) Write(p []byte) (int, error) {
	original := len(p)
	remaining := l.limit - l.buffer.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = l.buffer.Write(p)
	}
	return original, nil
}

func (l *cappedLog) String() string {
	return strings.TrimSpace(l.buffer.String())
}

func Start(ctx context.Context, binary string, outbound map[string]any, iface string, namespace ...string) (*Instance, error) {
	return startWithRetries(ctx, binary, outbound, iface, namespace, 0)
}

func startWithRetries(ctx context.Context, binary string, outbound map[string]any, iface string, namespace []string, portRetry int) (*Instance, error) {
	proxyBin, err := FindOrDownloadProgressContext(ctx, binary, io.Discard)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	listenAddr := "127.0.0.1"
	namespaceName, proxyInterface := "", ""
	if len(namespace) > 0 {
		namespaceName = namespace[0]
		if namespaceName != "" {
			listenAddr = "10.254.0.2"
		}
	}
	if len(namespace) > 1 {
		proxyInterface = namespace[1]
	}
	var proxyDialer *net.Dialer
	if namespaceName != "" {
		proxyDialer = namespaceProxyDialer(proxyInterface)
	}
	out := make(map[string]any, len(outbound)+1)
	for k, v := range outbound {
		if k != "_name" {
			out[k] = v
		}
	}
	out["tag"] = "proxy"
	if namespaceName == "" && iface != "" && iface != "auto" {
		it, lookupErr := net.InterfaceByName(iface)
		if lookupErr != nil {
			return nil, fmt.Errorf("network interface %q not found: %w", iface, lookupErr)
		}
		if it.Flags&net.FlagUp == 0 {
			return nil, fmt.Errorf("network interface %q exists but is DOWN", iface)
		}
		out["bind_interface"] = iface
	}
	cfg := map[string]any{"log": map[string]any{"level": "error"}, "inbounds": []any{map[string]any{"type": "mixed", "tag": "mixed-in", "listen": listenAddr, "listen_port": port}}, "outbounds": []any{out, map[string]any{"type": "direct", "tag": "direct"}}, "route": map[string]any{"final": "proxy"}}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	tmpDir, err := os.MkdirTemp("", "subwatch-node-")
	if err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(tmpDir, "config.json")
	if err = os.WriteFile(cfgPath, data, 0600); err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	check := exec.CommandContext(ctx, proxyBin, "check", "-c", cfgPath)
	if output, e := check.CombinedOutput(); e != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("sing-box config: %w: %s", e, strings.TrimSpace(string(output)))
	}
	var cmd *exec.Cmd
	if namespaceName != "" { cmd = (&NetworkNamespace{Name: namespaceName}).Command(ctx, proxyBin, "run", "-c", cfgPath) } else { cmd = exec.CommandContext(ctx, proxyBin, "run", "-c", cfgPath) }
	cmd.Stdout = io.Discard
	stderr := &cappedLog{limit: 32 * 1024}
	cmd.Stderr = stderr
	if err = cmd.Start(); err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	proxyAddress := fmt.Sprintf("%s:%d", listenAddr, port)
	inst := &Instance{ProxyURL: "http://" + proxyAddress, Dialer: proxyDialer, cmd: cmd, done: make(chan error, 1), tmpDir: tmpDir}
	go func() { inst.done <- cmd.Wait() }()
	deadline := time.NewTimer(startupTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastDialErr error
	for {
		var conn net.Conn
		var e error
		if proxyDialer != nil {
			conn, e = proxyDialer.DialContext(ctx, "tcp", proxyAddress)
		} else {
			conn, e = net.DialTimeout("tcp", proxyAddress, 200*time.Millisecond)
		}
		if e == nil {
			conn.Close()
			return inst, nil
		}
		lastDialErr = e
		select {
		case e := <-inst.done:
			os.RemoveAll(tmpDir)
			if e == nil {
				e = errors.New("sing-box exited before proxy became ready")
			}
			detail := stderr.String()
			if strings.Contains(detail, "address already in use") && portRetry < 3 {
				return startWithRetries(ctx, binary, outbound, iface, namespace, portRetry+1)
			}
			if detail != "" {
				return nil, fmt.Errorf("%w: %s", e, detail)
			}
			return nil, e
		case <-ctx.Done():
			inst.Close()
			if detail := stderr.String(); detail != "" {
				return nil, fmt.Errorf("%w (sing-box: %s)", ctx.Err(), detail)
			}
			return nil, ctx.Err()
		case <-deadline.C:
			inst.Close()
			reason := fmt.Sprintf("sing-box mixed inbound did not start within %s (check node protocol and interface %q)", startupTimeout, iface)
			if lastDialErr != nil {
				reason += fmt.Sprintf("; local proxy connection via %q failed: %v", proxyInterface, lastDialErr)
			}
			if detail := stderr.String(); detail != "" {
				reason += ": " + detail
			}
			return nil, errors.New(reason)
		case <-ticker.C:
		}
	}
}

func (i *Instance) Close() {
	if i == nil {
		return
	}
	if i.cmd != nil && i.cmd.Process != nil {
		_ = i.cmd.Process.Kill()
		select {
		case <-i.done:
		case <-time.After(time.Second):
		}
	}
	if i.tmpDir != "" {
		_ = os.RemoveAll(i.tmpDir)
	}
}
