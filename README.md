# SubWatch

SubWatch is a Linux-first Go CLI that merges subscription sources, removes duplicate nodes, runs staged connectivity and throughput checks through sing-box, and writes two filtered subscription files.

## What it does

1. Reads subscription URLs and/or local subscription files listed in `sources.txt`.
2. Parses supported share links and removes duplicate configurations.
3. Runs an HTTP GET test through each node. Concurrency, timeout, target URL and network interface are configurable.
4. Speed-tests only nodes that passed the URL test. Download and upload minimums are independent; either rate below its threshold rejects the node.
5. Checks Gemini web and public API-discovery endpoints only for nodes that passed the speed test.
6. Writes `speed-passed.txt` and `gemini-full.txt`, plus a CSV report.

Supported share links in this first version: VLESS, VMess, Trojan, Shadowsocks, Hysteria2, TUIC, SOCKS5 and HTTP(S) proxy links. Unknown formats are skipped and included in the invalid count.

## Quick start (Linux)

```bash
chmod +x subwatch
./subwatch init
$EDITOR sources.txt
./subwatch interfaces
./subwatch run --interface wlan0
```

Choose `--interface throne-tun`, `--interface wlan0`, `--interface auto`, or any interface shown by `./subwatch interfaces`.

If `sing-box` is installed and is on `PATH`, SubWatch uses it. Otherwise, on Linux amd64/arm64 it downloads the pinned official sing-box v1.14.4 archive on first run and verifies its SHA-256 before extracting the executable to the user's cache directory. You can also pass `--core-path /path/to/sing-box`.

## Sources file

One entry per line. Each entry can be an HTTP(S) subscription URL, a local file path, or a single share link. Blank lines and lines starting with `#` are ignored. Plain-text and Base64-wrapped subscription bodies are accepted.

## Important options

```text
--interface auto|wlan0|throne-tun
--concurrency 8
--timeout 10s
--url https://www.gstatic.com/generate_204
--min-download 5
--min-upload 5
--download-bytes 10000000
--upload-bytes 5000000
--out out
```

Speed checks use Cloudflare's public `speed.cloudflare.com` test endpoints. They transfer the configured amount of data for every node that passed the URL test, so adjust the byte counts and concurrency if your subscription contains many nodes or your connection has a data cap.

## Output files

- `out/merged-deduped.txt` — merged subscription after de-duplication.
- `out/speed-passed.txt` — nodes that passed URL and both speed thresholds.
- `out/gemini-full.txt` — nodes that passed both previous stages and returned HTTP 200 from both Gemini web and public API-discovery endpoints.
- `out/report.csv` — per-node status, latency, throughput, HTTP status and errors.

**Gemini check limitation:** this is a network-level check only. HTTP 200 from those public endpoints does not verify Google account eligibility, age/region eligibility, login state, or whether every Gemini feature is usable.

## Interface selection notes

SubWatch passes the selected Linux interface to sing-box as `bind_interface` for the outbound connection. `auto` leaves interface selection to the operating system. The named interface must exist; use `subwatch interfaces` to inspect names. Some TUN implementations may have special routing rules, so a successful interface selection does not guarantee the operating system's routes/firewall allow the test traffic.

## Build from source

```bash
go build -trimpath -ldflags="-s -w" -o subwatch ./cmd/subwatch
```

Go 1.22+ is required. The application itself uses only the Go standard library; sing-box is the external proxy engine used for node testing.

## Security and privacy

Subscription links often contain credentials. Keep `sources.txt`, source backups and generated subscription files private. The CSV report includes complete proxy URIs to make it easy to identify rows, so do not publish it. SubWatch does not upload subscriptions or reports to its own service.
