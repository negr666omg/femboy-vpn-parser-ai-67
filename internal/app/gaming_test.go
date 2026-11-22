package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/negr666omg/subwatch/internal/check"
)

func TestGamingQualityDecisionChecksIdleAndBothLoadedDirections(t *testing.T) {
	limits := GamingQualityOptions{
		MaxLatencyMS: 50, MaxJitterMS: 10,
		MaxLoadedLatencyMS: 80, MaxLoadedJitterMS: 15,
	}
	good := check.NetworkQuality{
		LatencyMS: 35, JitterMS: 4,
		DownloadLoadedLatencyMS: 54, DownloadLoadedJitterMS: 8,
		UploadLoadedLatencyMS: 62, UploadLoadedJitterMS: 9,
	}
	if passed, reason := gamingQualityDecision(good, limits); !passed || reason != "" {
		t.Fatalf("good quality decision = (%v, %q), want (true, empty)", passed, reason)
	}

	badUpload := good
	badUpload.UploadLoadedLatencyMS = 95
	if passed, reason := gamingQualityDecision(badUpload, limits); passed || !strings.Contains(reason, "upload-loaded latency") {
		t.Fatalf("bad upload-loaded latency decision = (%v, %q), want a rejection", passed, reason)
	}

	badJitter := good
	badJitter.DownloadLoadedJitterMS = 25
	if passed, reason := gamingQualityDecision(badJitter, limits); passed || !strings.Contains(reason, "download-loaded jitter") {
		t.Fatalf("bad loaded jitter decision = (%v, %q), want a rejection", passed, reason)
	}
}

func TestLoadOptionsEnablesExperimentalGamingQuality(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	config := strings.Replace(ExampleConfig(), `"gaming_test": {\n    "enabled": false,`, `"gaming_test": {\n    "enabled": true,`, 1)
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	opts, err := LoadOptions(path)
	if err != nil {
		t.Fatalf("LoadOptions() error = %v", err)
	}
	if !opts.GamingQuality.Enabled {
		t.Fatal("gaming_test.enabled=true did not enable experimental quality stage")
	}
	if opts.GamingQuality.LatencySamples != 12 || opts.GamingQuality.Concurrency != 1 {
		t.Fatalf("unexpected gaming test defaults: %+v", opts.GamingQuality)
	}
	if opts.GamingQuality.DownloadBytes != 32<<20 || opts.GamingQuality.UploadBytes != 16<<20 {
		t.Fatalf("unexpected gaming payload defaults: %d/%d", opts.GamingQuality.DownloadBytes, opts.GamingQuality.UploadBytes)
	}
}
