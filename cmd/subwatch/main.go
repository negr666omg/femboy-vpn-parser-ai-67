package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/negr666omg/subwatch/internal/app"
)

const version = "0.5.2"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	command := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	switch command {
	case "version":
		fmt.Printf("SubWatch %s (%s/%s)\n", version, runtime.GOOS, runtime.GOARCH)
		return nil
	case "init":
		fs := flag.NewFlagSet("init", flag.ContinueOnError)
		dir := fs.String("dir", ".", "directory where sources.txt and config.json will be created")
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if err := app.Init(*dir); err != nil {
			return err
		}
		fmt.Printf("Initialized %s (sources.txt, config.json)\n", strings.TrimRight(*dir, "/"))
		return nil
	case "__fetch-source":
		fs := flag.NewFlagSet("__fetch-source", flag.ContinueOnError)
		timeout := fs.Duration("timeout", 30*time.Second, "subscription download timeout")
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil
			}
			return err
		}
		if fs.NArg() != 0 {
			return fmt.Errorf("__fetch-source does not accept positional arguments")
		}
		return app.FetchSourceCommand(*timeout)
	case "interfaces":
		if len(args) > 0 {
			return fmt.Errorf("interfaces does not accept options")
		}
		return app.Interfaces(os.Stdout)
	case "run":
		return runCommand(args)
	default:
		return fmt.Errorf("unknown command %q (use run, init, interfaces, version)", command)
	}
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	config := fs.String("config", "config.json", "JSON settings file")
	sources := fs.String("sources", "", "override source list file")
	out := fs.String("out", "", "override output directory")
	iface := fs.String("interface", "", "override outbound interface: auto, eno1, throne-tun, etc.")
	corePath := fs.String("core-path", "", "path to sing-box executable")
	protocols := fs.String("protocols", "", "comma-separated enabled protocols")
	concurrency := fs.Int("concurrency", 0, "override concurrency for all three test stages")
	urlConcurrency := fs.Int("url-concurrency", 0, "URL-test concurrency")
	speedConcurrency := fs.Int("speed-concurrency", 0, "speed-test concurrency")
	serviceConcurrency := fs.Int("service-concurrency", 0, "service-check concurrency")
	timeout := fs.Duration("timeout", 0, "override timeout for all three stages")
	urlTimeout := fs.Duration("url-timeout", 0, "URL-test timeout")
	speedTimeout := fs.Duration("speed-timeout", 0, "speed-test timeout")
	serviceTimeout := fs.Duration("service-timeout", 0, "service-check timeout")
	sourceTimeout := fs.Duration("source-timeout", 0, "subscription download timeout")
	testURL := fs.String("url", "", "override URL-test target")
	downloadURL := fs.String("download-url", "", "override speed-test download URL (supports {bytes})")
	uploadURL := fs.String("upload-url", "", "override speed-test upload URL")
	minDown := fs.Float64("min-download", -1, "minimum download rate in decimal Mbps")
	minUp := fs.Float64("min-upload", -1, "minimum upload rate in decimal Mbps")
	downBytes := fs.Int64("download-bytes", 0, "download test size")
	upBytes := fs.Int64("upload-bytes", 0, "upload test size")
	split := fs.Bool("split-by-protocol", false, "write protocol-specific URI subscription files")
	lookupIP := fs.Bool("lookup-ip", false, "also collect exit IP metadata during service check")
	noText := fs.Bool("no-subscription-text", false, "do not write the final URI subscription file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	configMissing := false
	if _, statErr := os.Stat(*config); errors.Is(statErr, os.ErrNotExist) {
		configMissing = true
	}
	opts, err := app.LoadOptions(*config)
	if err != nil {
		failureDir := strings.TrimSpace(*out)
		if failureDir == "" {
			failureDir = opts.OutDir
		}
		if logErr := app.RecordStartupFailure(failureDir, "CONFIG", err); logErr != nil {
			fmt.Fprintf(os.Stderr, "failed to write failure log: %v\n", logErr)
		}
		return err
	}
	if configMissing {
		fmt.Fprintf(os.Stdout, "[config] created %s with editable defaults\n", *config)
		fmt.Fprintf(os.Stdout, "[config] created source list %s when missing\n", opts.Sources)
	}
	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })
	if visited["sources"] {
		opts.Sources = *sources
	}
	if visited["out"] {
		opts.OutDir = *out
	}
	if visited["interface"] {
		opts.Interface = *iface
	}
	if visited["core-path"] {
		opts.CorePath = *corePath
	}
	if visited["protocols"] {
		opts.Protocols = strings.Split(*protocols, ",")
	}
	if visited["concurrency"] {
		opts.URLConcurrency, opts.SpeedConcurrency, opts.ServiceConcurrency = *concurrency, *concurrency, *concurrency
	}
	if visited["url-concurrency"] {
		opts.URLConcurrency = *urlConcurrency
	}
	if visited["speed-concurrency"] {
		opts.SpeedConcurrency = *speedConcurrency
	}
	if visited["service-concurrency"] {
		opts.ServiceConcurrency = *serviceConcurrency
	}
	if visited["timeout"] {
		opts.URLTimeout, opts.SpeedTimeout, opts.ServiceTimeout = *timeout, *timeout, *timeout
	}
	if visited["url-timeout"] {
		opts.URLTimeout = *urlTimeout
	}
	if visited["speed-timeout"] {
		opts.SpeedTimeout = *speedTimeout
	}
	if visited["service-timeout"] {
		opts.ServiceTimeout = *serviceTimeout
	}
	if visited["source-timeout"] {
		opts.SourceTimeout = *sourceTimeout
	}
	if visited["url"] {
		opts.URL = *testURL
	}
	if visited["download-url"] {
		opts.SpeedDownloadURL = *downloadURL
	}
	if visited["upload-url"] {
		opts.SpeedUploadURL = *uploadURL
	}
	if visited["min-download"] {
		opts.MinDownload = *minDown
	}
	if visited["min-upload"] {
		opts.MinUpload = *minUp
	}
	if visited["download-bytes"] {
		opts.DownloadBytes = *downBytes
	}
	if visited["upload-bytes"] {
		opts.UploadBytes = *upBytes
	}
	if visited["split-by-protocol"] {
		opts.SplitByProtocol = *split
	}
	if visited["lookup-ip"] {
		opts.LookupIP = *lookupIP
	}
	if visited["no-subscription-text"] {
		opts.WriteText = !*noText
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Fprint(os.Stdout, "Run continuously (repeat full test cycles)? [y/N]: ")
	answer, readErr := reader.ReadString('\n')
	if readErr != nil && readErr != io.EOF {
		return readErr
	}
	continuous := strings.EqualFold(strings.TrimSpace(answer), "y") || strings.EqualFold(strings.TrimSpace(answer), "yes")
	stability := app.NewStabilityTracker()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cycle := 0
	for {
		cycle++
		if continuous {
			fmt.Fprintf(os.Stdout, "\n========== CYCLE %d ==========\n", cycle)
		}
		err := app.RunWithStability(ctx, opts, os.Stdout, os.Stderr, stability)
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stdout, "\n[stop] Interrupted; exiting cleanly")
			return nil
		}
		if err != nil {
			return err
		}
		if !continuous {
			return nil
		}
		if opts.CycleInterval > 0 {
			fmt.Fprintf(os.Stdout, "\n[loop] waiting %s before next cycle (Ctrl+C to stop)\n", opts.CycleInterval)
		}
		if !waitForNextCycle(ctx, opts.CycleInterval) {
			fmt.Fprintln(os.Stdout, "\n[stop] Interrupted; exiting cleanly")
			return nil
		}
	}
}

func waitForNextCycle(ctx context.Context, interval time.Duration) bool {
	if interval <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}
