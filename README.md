# SubWatch

Linux-first Go CLI for merging VLESS/VMess/Trojan/Shadowsocks/Hysteria2/TUIC/SOCKS/HTTP subscription links, removing duplicates, checking nodes through sing-box, and exporting filtered subscriptions.

## Source

The complete source tree is in [subwatch-source.tar.gz](./subwatch-source.tar.gz). Extract it, then run:

```bash
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o subwatch ./cmd/subwatch
```

## Quick start

```bash
chmod +x subwatch
./subwatch init
# edit sources.txt: one subscription URL, local file path or share URI per line
./subwatch interfaces
./subwatch run --interface wlan0
# or: ./subwatch run --interface throne-tun
```

The run pipeline merges sources, deduplicates outbound configs, performs an HTTP URL test, then Cloudflare download/upload tests, then Throne-style public Gemini Service Check classification. Only Gemini `available` results appear in `gemini-full.txt`; `partial`, `unknown`, `blocked` and `failed` do not pass. This is only a public network-level page check and does not authenticate or test account eligibility.

SubWatch supports `--concurrency`, `--timeout`, `--url`, `--min-download`, `--min-upload`, `--download-bytes`, `--upload-bytes`, `--interface`, `--out`, and `--core-path`. Default minimum upload and download rates are 5 Mbps. See README inside the source archive for details and known limitations.

On Linux amd64/arm64, if sing-box is not installed, the binary downloads the pinned v1.14.4 archive and verifies its SHA-256 before use. The current Gemini HTTP client matches Throne's URL, cookie, headers and page-state rules but uses Go's standard TLS stack instead of Throne's Chrome uTLS fingerprint; uncertain responses are never promoted to available.
