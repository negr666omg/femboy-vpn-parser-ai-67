# SubWatch 0.3.8

Linux-first Go CLI for parsing and deduplicating VLESS/VMess/Trojan/Shadowsocks/Hysteria2/TUIC/SOCKS/HTTP subscription links, then testing them through sing-box.

## Build from source

```bash
tar -xzf subwatch-source-0.3.8.tar.gz -C subwatch-source
cd subwatch-source
go test ./...
go vet ./...
go build -trimpath -ldflags='-s -w' -o sw ./cmd/subwatch
```

## First run

```bash
chmod +x sw
./sw run
```

When `config.json` does not exist, `sw run` creates it automatically with editable defaults. A starter `sources.txt` is also created if it is missing. Put subscription URLs, local file paths, or individual share links into `sources.txt`, one per line. Existing files are never overwritten. `sw init` remains available for explicit initialization.

## Processing pipeline — one pass per stage

1. Parse every source and write `all.txt` (all parsed proxy URIs, including duplicates).
2. Deduplicate in memory and apply the enabled-protocol filter. These are internal working sets and are not exported.
3. URL-test every selected unique node. As soon as a node passes, SubWatch atomically updates `url-test-finished.txt` and the corresponding protocol file (`vless.txt`, `ss.txt`, `hysteria2.txt`, etc.). Protocol files therefore contain only URL-test-passed nodes and grow during the URL-test stage; they are never prefilled with untested nodes.
4. After the complete URL-test barrier, speed-test only URL successes. Both download and upload must meet their configured minimums. Write `speedtest-finished.txt`.
5. After the complete speed-test barrier, check Gemini only for speed-test successes. Write `gemini-available.txt` with only nodes classified as `available`.
6. At the end of the run, print each source subscription and how many unique URL-passed nodes it contributed, sorted from highest count to lowest. If a passing node occurs in multiple sources, it is credited to each of those sources while being tested only once globally.

All output lists are plain-text URI subscriptions, one URI per line. Failed-stage lists, temporary `selected`/`unique` lists, duplicate `tested` lists, service diagnostics and CSV reports are not exported. The output directory is cleaned of SubWatch-generated artifacts at the start of each run so stale files from older versions do not remain. Each invocation still performs one pass only; continuous/infinite cycling is not enabled.

## Live terminal status

Each testing stage uses one replaceable terminal status line instead of printing a separate line for every proxy. The status is fitted to the current terminal width; the config name and stage details share the available space, while the `ping: N ms` field is kept at the far right and always remains the final field. Long details are shortened to prevent wrapping, and ping is shown as `— ms` until the URL test succeeds. It shows the current config name and protocol. During URL-test it displays the measured ping and pass/fail result; during speed-test it displays ping plus live download/upload throughput and byte progress; during Gemini check it keeps ping and speed figures visible and adds the Gemini status and region when available. With parallel workers, the display follows the most recently started config; completion of an older parallel task cannot replace that current display. The test counters remain on the same line.

## Settings (`config.json`)

See [`config.example.json`](./config.example.json). All settings are optional; defaults are used for omitted values.

- `network.interface`: `auto`, `eno1`, `throne-tun`, or another local interface name.
- `sources.file`: source list path. Relative paths are resolved relative to the config file.
- `protocols.enabled`: protocols to test (`vless`, `vmess`, `trojan`, `ss`, `hysteria2`, `tuic`, `socks`, `http`). Aliases `shadowsocks`, `hy2`, and `socks5` are accepted.
- `url_test`: URL used for the reachability check, concurrency, and timeout.
- `speed_test`: separate concurrency and timeout, minimum download/upload Mbps, payload sizes in bytes, and custom download/upload HTTP URLs. The download URL may contain `{bytes}`, replaced with the configured download size. The upload URL must accept a raw HTTP `POST` body and return a 2xx response.
- `service_check`: concurrency, timeout, and optional exit-IP lookup.
- `output`: output directory. The five result groups are always written as URI-only `.txt` files; protocol-specific working lists are always enabled.

The default speed test uses 2 MiB download and 2 MiB upload payloads, a concurrency of 2, and minimums of 8 Mbps in both directions. **8 Mbps is approximately 1 MB/s.** Speed-test concurrency is deliberately lower than URL-test concurrency because simultaneous throughput tests share the machine's internet connection and can reduce measured rates. Set it to `1` for the least contention or increase it when throughput is ample.

Stage timeouts are independently configurable. CLI flags override the config file; for example:

```bash
./sw run --interface eno1
./sw run --url-concurrency 12 --speed-concurrency 1
./sw run --url 'https://www.gstatic.com/generate_204'
./sw run --download-url 'https://speed.cloudflare.com/__down?bytes={bytes}' --upload-url 'https://speed.cloudflare.com/__up'
./sw run --min-download 8 --min-upload 8 --speed-timeout 30s
```

For compatibility, an old `config.json` containing a `final_check` object is accepted, but that obsolete setting is ignored. The duplicate stage no longer exists.

## Output files

The output directory contains only these result groups:

- `all.txt` — every parsed proxy URI from all sources (duplicates included).
- `vless.txt`, `vmess.txt`, `trojan.txt`, `ss.txt`, `hysteria2.txt`, `tuic.txt`, `socks.txt`, `http.txt` — only nodes that passed URL-test, grouped by protocol. A protocol file appears only when it has at least one passing node and is updated as URL tests finish.
- `url-test-finished.txt` — all URL-test-passed nodes.
- `speedtest-finished.txt` — nodes that passed both download and upload thresholds.
- `gemini-available.txt` — nodes classified as Gemini `available`.

`selected` means the internal list after deduplication and filtering by `protocols.enabled`; it does not mean URL-test-passed or working. It is an intermediate implementation detail and is no longer written to disk. Failed lists, `unique.txt`, `tested.txt`, `final.txt`, service-check dumps and `report.csv` are intentionally omitted to keep the output directory minimal.

The Gemini check classifies a public signed-out page response only. It does not authenticate or test account-specific eligibility. A proxy can still stop responding immediately after its one-time test, and the destination client may route traffic differently from SubWatch.


### Subscription download failures (0.3.8)

A timeout or HTTP/read error while downloading one subscription now marks only that source as skipped. SubWatch logs the source error and continues loading the rest of `sources.txt`. The run is still interrupted by an explicit Ctrl+C/SIGTERM, or by a fatal global error such as an unreadable source-list file.

## Remote SFTP upload

SubWatch can upload the `.txt` result files from `out/` to a remote directory after each completed cycle. Configure `remote_upload` in `config.json`:

```json
"remote_upload": {
  "enabled": true,
  "method": "sftp",
  "host": "79.72.16.241",
  "port": 22,
  "username": "ubuntu",
  "auth": "private_key",
  "private_key": "~/.ssh/id_ed25519",
  "password": "",
  "remote_directory": "/home/ubuntu/subwatch/out"
}
```

`enabled` is the only upload switch. There is no separate `upload_after_run`: when enabled, the upload happens after each completed test cycle. `auth` accepts `private_key` or `password`. Key authentication uses the local OpenSSH `ssh`/`sftp` commands. For password authentication, install `sshpass` on the machine running SubWatch and set `password` in the config; protect `config.json` because it contains a secret. SSH key authentication is recommended.

On the first run, SubWatch asks whether to repeat complete test cycles continuously. Answer `y` for continuous mode or press Enter / answer `n` for one cycle. In continuous mode, SFTP upload runs after every completed cycle. Ctrl+C stops the loop cleanly.
