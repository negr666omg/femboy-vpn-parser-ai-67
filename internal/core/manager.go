package core

import (
	"archive/tar"
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

const coreVersion = "1.14.4"

type Instance struct {
	ProxyURL string
	cmd      *exec.Cmd
	done     chan error
	tmpDir   string
}

func FindOrDownload(binary string) (string, error) {
	return FindOrDownloadProgress(binary, io.Discard)
}

func FindOrDownloadProgress(binary string, progress io.Writer) (string, error) {
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
		if e := exec.Command(bin, "version").Run(); e == nil {
			fmt.Fprintf(progress, "[prepare] using cached sing-box: %s\n", bin)
			return bin, nil
		}
		_ = os.Remove(bin)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	url := "https://github.com/SagerNet/sing-box/releases/download/v" + coreVersion + "/" + asset
	fmt.Fprintf(progress, "[prepare] downloading sing-box %s for linux/%s...\n", coreVersion, arch)
	req, err := http.NewRequest(http.MethodGet, url, nil)
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
	if resp.StatusCode != 200 {
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
		tmp.Close()
		return "", err
	}
	if err = tmp.Close(); err != nil {
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
		out, e := os.OpenFile(bin, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0700)
		if e != nil {
			return "", e
		}
		_, copyErr := io.Copy(out, io.LimitReader(tr, 100<<20))
		closeErr := out.Close()
		if copyErr != nil {
			return "", copyErr
		}
		if closeErr != nil {
			return "", closeErr
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

func Start(ctx context.Context, binary string, outbound map[string]any, iface string) (*Instance, error) {
	proxyBin, err := FindOrDownload(binary)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	out := make(map[string]any, len(outbound)+1)
	for k, v := range outbound {
		if k != "_name" {
			out[k] = v
		}
	}
	out["tag"] = "proxy"
	if iface != "" && iface != "auto" {
		it, lookupErr := net.InterfaceByName(iface)
		if lookupErr != nil {
			return nil, fmt.Errorf("network interface %q not found: %w", iface, lookupErr)
		}
		if it.Flags&net.FlagUp == 0 {
			return nil, fmt.Errorf("network interface %q exists but is DOWN", iface)
		}
		out["bind_interface"] = iface
	}
	cfg := map[string]any{"log": map[string]any{"level": "error"}, "inbounds": []any{map[string]any{"type": "mixed", "tag": "mixed-in", "listen": "127.0.0.1", "listen_port": port}}, "outbounds": []any{out, map[string]any{"type": "direct", "tag": "direct"}}, "route": map[string]any{"final": "proxy"}}
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
	cmd := exec.Command(proxyBin, "run", "-c", cfgPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err = cmd.Start(); err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	inst := &Instance{ProxyURL: fmt.Sprintf("http://127.0.0.1:%d", port), cmd: cmd, done: make(chan error, 1), tmpDir: tmpDir}
	go func() { inst.done <- cmd.Wait() }()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		conn, e := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if e == nil {
			conn.Close()
			return inst, nil
		}
		select {
		case e := <-inst.done:
			os.RemoveAll(tmpDir)
			if e == nil {
				e = errors.New("sing-box exited before proxy became ready")
			}
			return nil, e
		case <-ctx.Done():
			inst.Close()
			return nil, ctx.Err()
		case <-deadline.C:
			inst.Close()
			return nil, fmt.Errorf("sing-box mixed inbound did not start (check node protocol and interface %q)", iface)
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
