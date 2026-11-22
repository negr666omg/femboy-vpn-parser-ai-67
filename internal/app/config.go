package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Options struct {
	Sources            string
	SourceTimeout      time.Duration
	OutDir             string
	GeminiFile         bool
	Interface          string
	CycleInterval      time.Duration
	IsolateSourceDownloads bool
	NetworkNamespace   string
	NetworkNamespaceInterface string
	CorePath           string
	Protocols          []string
	URLConcurrency     int
	SpeedConcurrency   int
	SpeedProvider      string
	CloudflareFallback bool
	ServiceConcurrency int
	URLTimeout         time.Duration
	SpeedTimeout       time.Duration
	FirstByteTimeout   time.Duration
	MaxSpeedDuration   time.Duration
	SpeedSampleInterval time.Duration
	RenameNodes        bool
	GeoIPURL           string
	NameFormat         string
	CountryFormat      string
	SpeedDecimals      int
	ServiceTimeout     time.Duration
	URL                string
	SpeedDownloadURL   string
	SpeedUploadURL     string
	DownloadBytes      int64
	UploadBytes        int64
	MinDownload        float64
	MinUpload          float64
	SplitByProtocol    bool
	WriteText          bool
	LookupIP           bool
	RemoteUpload       RemoteUploadOptions
	GamingQuality      GamingQualityOptions
}

type GamingQualityOptions struct {
	Enabled                 bool
	Concurrency             int
	Timeout                 time.Duration
	LatencySamples          int
	SampleInterval          time.Duration
	DownloadBytes           int64
	UploadBytes             int64
	MaxLatencyMS            float64
	MaxJitterMS             float64
	MaxLoadedLatencyMS      float64
	MaxLoadedJitterMS       float64
}

type RemoteUploadOptions struct {
	Enabled         bool
	Method          string
	Host            string
	Port            int
	Username        string
	Auth            string
	PrivateKey      string
	Password        string
	RemoteDirectory string
	Interface       string
}

type fileConfig struct {
	Network struct {
		Interface              string `json:"interface"`
		IsolateSourceDownloads bool   `json:"isolate_source_downloads"`
	} `json:"network"`
	Loop struct {
		Interval string `json:"interval"`
	} `json:"loop"`
	Sources struct {
		File    string `json:"file"`
		Timeout string `json:"timeout"`
	} `json:"sources"`
	Protocols struct {
		Enabled []string `json:"enabled"`
	} `json:"protocols"`
	URLTest struct {
		URL         string `json:"url"`
		Concurrency int    `json:"concurrency"`
		Timeout     string `json:"timeout"`
	} `json:"url_test"`
	SpeedTest struct {
		Concurrency   int      `json:"concurrency"`
		Provider      string   `json:"provider"`
		CloudflareFallback struct {
			Enabled bool `json:"enabled"`
		} `json:"cloudflare_fallback"`
		Timeout       string   `json:"timeout"`
		MinDownload   *float64 `json:"min_download_mbps"`
		MinUpload     *float64 `json:"min_upload_mbps"`
		DownloadBytes int64    `json:"download_bytes"`
		UploadBytes   int64    `json:"upload_bytes"`
		DownloadURL   string   `json:"download_url"`
		UploadURL     string   `json:"upload_url"`
		FirstByteTimeout string `json:"first_byte_timeout"`
		MaxDuration string `json:"max_duration"`
		SampleInterval string `json:"sample_interval"`
	} `json:"speed_test"`
	GamingTest struct {
		Enabled            bool     `json:"enabled"`
		Concurrency        int      `json:"concurrency"`
		Timeout            string   `json:"timeout"`
		LatencySamples     int      `json:"latency_samples"`
		SampleInterval     string   `json:"sample_interval"`
		DownloadBytes      int64    `json:"download_bytes"`
		UploadBytes        int64    `json:"upload_bytes"`
		MaxLatencyMS       *float64 `json:"max_latency_ms"`
		MaxJitterMS        *float64 `json:"max_jitter_ms"`
		MaxLoadedLatencyMS *float64 `json:"max_loaded_latency_ms"`
		MaxLoadedJitterMS  *float64 `json:"max_loaded_jitter_ms"`
	} `json:"gaming_test"`
	ServiceCheck struct {
		Concurrency int    `json:"concurrency"`
		Timeout     string `json:"timeout"`
		LookupIP    *bool  `json:"lookup_ip"`
		RenameNodes *bool `json:"rename_nodes"`
		GeoIPURL string `json:"geoip_url"`
		NameFormat string `json:"name_format"`
		CountryFormat string `json:"country_format"`
		SpeedDecimals *int `json:"speed_decimals"`
	} `json:"service_check"`
	RemoteUpload struct {
		Enabled         bool   `json:"enabled"`
		Method          string `json:"method"`
		Host            string `json:"host"`
		Port            int    `json:"port"`
		Username        string `json:"username"`
		Auth            string `json:"auth"`
		PrivateKey      string `json:"private_key"`
		Password        string `json:"password"`
		RemoteDirectory string `json:"remote_directory"`
		Interface       string `json:"interface"`
	} `json:"remote_upload"`
	Output struct {
		Directory        string `json:"directory"`
		SplitByProtocol  bool   `json:"split_by_protocol"`
		SubscriptionText *bool  `json:"subscription_text"`
		GeminiFile       *bool  `json:"gemini_file"`
	} `json:"output"`
}

func DefaultOptions() Options {
	return Options{
		Sources: "sources.txt", SourceTimeout: 30 * time.Second, OutDir: "out", Interface: "auto", GeminiFile: false,
		Protocols:      []string{"vless", "vmess", "trojan", "ss", "hysteria", "hysteria2", "tuic", "socks", "http"},
		URLConcurrency: 8, SpeedConcurrency: 2, ServiceConcurrency: 4, SpeedProvider: "cloudflare",
		URLTimeout: 12 * time.Second, SpeedTimeout: 85 * time.Second,
		FirstByteTimeout: 10 * time.Second, MaxSpeedDuration: 30 * time.Second, SpeedSampleInterval: 500 * time.Millisecond, RenameNodes: true,
		GeoIPURL: "https://ipwho.is/", NameFormat: "{flag} {country} {download}↓{upload}↑", CountryFormat: "full", SpeedDecimals: 1,
		ServiceTimeout:   25 * time.Second,
		URL:              "https://www.gstatic.com/generate_204",
		SpeedDownloadURL: "https://speed.cloudflare.com/__down?bytes={bytes}",
		SpeedUploadURL:   "https://speed.cloudflare.com/__up",
		DownloadBytes:    2 << 20, UploadBytes: 2 << 20,
		MinDownload: 8, MinUpload: 8, WriteText: true,
		GamingQuality: GamingQualityOptions{
			Enabled: false, Concurrency: 1, Timeout: 90 * time.Second,
			LatencySamples: 12, SampleInterval: 250 * time.Millisecond,
			DownloadBytes: 16 << 20, UploadBytes: 8 << 20,
			MaxLatencyMS: 50, MaxJitterMS: 10,
			MaxLoadedLatencyMS: 80, MaxLoadedJitterMS: 15,
		},
	}
}

func LoadOptions(path string) (Options, error) {
	opts := DefaultOptions()
	if strings.TrimSpace(path) == "" {
		return opts, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := writeDefaultIfMissing(path, ExampleConfig()); err != nil {
			return opts, fmt.Errorf("create config %s: %w", path, err)
		}
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return opts, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg fileConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return opts, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Network.Interface != "" {
		opts.Interface = cfg.Network.Interface
	}
	opts.IsolateSourceDownloads = cfg.Network.IsolateSourceDownloads
	if cfg.Loop.Interval != "" {
		d, e := time.ParseDuration(strings.TrimSpace(cfg.Loop.Interval))
		if e != nil || d < 0 {
			return opts, fmt.Errorf("loop.interval must be a non-negative duration: %q", cfg.Loop.Interval)
		}
		opts.CycleInterval = d
	}
	if cfg.Sources.File != "" {
		opts.Sources = cfg.Sources.File
	}
	if cfg.Sources.Timeout != "" {
		d, e := time.ParseDuration(cfg.Sources.Timeout)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("sources.timeout must be a positive duration: %q", cfg.Sources.Timeout)
		}
		opts.SourceTimeout = d
	}
	if !filepath.IsAbs(opts.Sources) {
		opts.Sources = filepath.Join(filepath.Dir(path), opts.Sources)
	}
	if len(cfg.Protocols.Enabled) > 0 {
		opts.Protocols = normalizeProtocols(cfg.Protocols.Enabled)
	}
	if cfg.URLTest.URL != "" {
		opts.URL = cfg.URLTest.URL
	}
	if cfg.URLTest.Concurrency > 0 {
		opts.URLConcurrency = cfg.URLTest.Concurrency
	}
	if cfg.URLTest.Timeout != "" {
		d, e := time.ParseDuration(cfg.URLTest.Timeout)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("url_test.timeout must be a positive duration: %q", cfg.URLTest.Timeout)
		}
		opts.URLTimeout = d
	}
	if cfg.SpeedTest.Provider != "" {
		opts.SpeedProvider = strings.ToLower(strings.TrimSpace(cfg.SpeedTest.Provider))
	}
	opts.CloudflareFallback = cfg.SpeedTest.CloudflareFallback.Enabled
	if opts.SpeedProvider != "cloudflare" && opts.SpeedProvider != "ookla" {
		return opts, fmt.Errorf("speed_test.provider must be cloudflare or ookla")
	}
	if opts.CloudflareFallback && opts.SpeedProvider != "ookla" {
		return opts, fmt.Errorf("speed_test.cloudflare_fallback.enabled requires speed_test.provider=ookla")
	}
	if cfg.SpeedTest.Concurrency > 0 {
		opts.SpeedConcurrency = cfg.SpeedTest.Concurrency
	}
	if cfg.SpeedTest.Timeout != "" {
		d, e := time.ParseDuration(cfg.SpeedTest.Timeout)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("speed_test.timeout must be a positive duration: %q", cfg.SpeedTest.Timeout)
		}
		opts.SpeedTimeout = d
	}
	if cfg.SpeedTest.MaxDuration != "" {
		d, e := time.ParseDuration(cfg.SpeedTest.MaxDuration)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("speed_test.max_duration must be a positive duration: %q", cfg.SpeedTest.MaxDuration)
		}
		opts.MaxSpeedDuration = d
	}
	if cfg.SpeedTest.MinDownload != nil {
		opts.MinDownload = *cfg.SpeedTest.MinDownload
	}
	if cfg.SpeedTest.MinUpload != nil {
		opts.MinUpload = *cfg.SpeedTest.MinUpload
	}

	if opts.SpeedProvider == "cloudflare" || opts.CloudflareFallback {
		for _, item := range []struct {
			name   string
			value  string
			target *time.Duration
		}{
			{"speed_test.first_byte_timeout", cfg.SpeedTest.FirstByteTimeout, &opts.FirstByteTimeout},
			{"speed_test.sample_interval", cfg.SpeedTest.SampleInterval, &opts.SpeedSampleInterval},
		} {
			if item.value == "" {
				continue
			}
			d, e := time.ParseDuration(item.value)
			if e != nil || d <= 0 {
				return opts, fmt.Errorf("%s must be a positive duration: %q", item.name, item.value)
			}
			*item.target = d
		}
		if cfg.SpeedTest.DownloadBytes > 0 {
			opts.DownloadBytes = cfg.SpeedTest.DownloadBytes
		}
		if cfg.SpeedTest.UploadBytes > 0 {
			opts.UploadBytes = cfg.SpeedTest.UploadBytes
		}
		if cfg.SpeedTest.DownloadURL != "" {
			opts.SpeedDownloadURL = cfg.SpeedTest.DownloadURL
		}
		if cfg.SpeedTest.UploadURL != "" {
			opts.SpeedUploadURL = cfg.SpeedTest.UploadURL
		}
	} else {
		// Ookla owns its server selection and transfer sizes. Cloudflare-only
		// settings are intentionally ignored, even when present in old configs.
		opts.FirstByteTimeout = 0
		opts.SpeedSampleInterval = 0
		opts.DownloadBytes = 0
		opts.UploadBytes = 0
		opts.SpeedDownloadURL = ""
		opts.SpeedUploadURL = ""
	}
	// Experimental Cloudflare gaming-quality measurements are independent of
	// the primary speed provider and its fallback policy.
	opts.GamingQuality.Enabled = cfg.GamingTest.Enabled
	if cfg.GamingTest.Concurrency > 0 {
		opts.GamingQuality.Concurrency = cfg.GamingTest.Concurrency
	}
	if cfg.GamingTest.Timeout != "" {
		d, e := time.ParseDuration(cfg.GamingTest.Timeout)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("gaming_test.timeout must be a positive duration: %q", cfg.GamingTest.Timeout)
		}
		opts.GamingQuality.Timeout = d
	}
	if cfg.GamingTest.LatencySamples > 0 {
		opts.GamingQuality.LatencySamples = cfg.GamingTest.LatencySamples
	}
	if cfg.GamingTest.SampleInterval != "" {
		d, e := time.ParseDuration(cfg.GamingTest.SampleInterval)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("gaming_test.sample_interval must be a positive duration: %q", cfg.GamingTest.SampleInterval)
		}
		opts.GamingQuality.SampleInterval = d
	}
	if cfg.GamingTest.DownloadBytes > 0 {
		opts.GamingQuality.DownloadBytes = cfg.GamingTest.DownloadBytes
	}
	if cfg.GamingTest.UploadBytes > 0 {
		opts.GamingQuality.UploadBytes = cfg.GamingTest.UploadBytes
	}
	for _, item := range []struct {
		name string
		value *float64
		target *float64
	}{
		{"max_latency_ms", cfg.GamingTest.MaxLatencyMS, &opts.GamingQuality.MaxLatencyMS},
		{"max_jitter_ms", cfg.GamingTest.MaxJitterMS, &opts.GamingQuality.MaxJitterMS},
		{"max_loaded_latency_ms", cfg.GamingTest.MaxLoadedLatencyMS, &opts.GamingQuality.MaxLoadedLatencyMS},
		{"max_loaded_jitter_ms", cfg.GamingTest.MaxLoadedJitterMS, &opts.GamingQuality.MaxLoadedJitterMS},
	} {
		if item.value != nil {
			if *item.value < 0 {
				return opts, fmt.Errorf("gaming_test.%s cannot be negative", item.name)
			}
			*item.target = *item.value
		}
	}
	if opts.GamingQuality.Concurrency < 1 || opts.GamingQuality.Concurrency > 16 {
		return opts, fmt.Errorf("gaming_test.concurrency must be between 1 and 16")
	}
	if opts.GamingQuality.LatencySamples < 3 || opts.GamingQuality.LatencySamples > 100 {
		return opts, fmt.Errorf("gaming_test.latency_samples must be between 3 and 100")
	}
	if opts.GamingQuality.DownloadBytes < 64*1024 || opts.GamingQuality.DownloadBytes > 100<<20 ||
		opts.GamingQuality.UploadBytes < 64*1024 || opts.GamingQuality.UploadBytes > 100<<20 {
		return opts, fmt.Errorf("gaming_test download_bytes and upload_bytes must be between 64 KiB and 100 MiB")
	}
	if cfg.ServiceCheck.Concurrency > 0 {
		opts.ServiceConcurrency = cfg.ServiceCheck.Concurrency
	}
	if cfg.ServiceCheck.Timeout != "" {
		d, e := time.ParseDuration(cfg.ServiceCheck.Timeout)
		if e != nil || d <= 0 {
			return opts, fmt.Errorf("service_check.timeout must be a positive duration: %q", cfg.ServiceCheck.Timeout)
		}
		opts.ServiceTimeout = d
	}
	if cfg.ServiceCheck.LookupIP != nil { opts.LookupIP = *cfg.ServiceCheck.LookupIP }
	if cfg.ServiceCheck.RenameNodes != nil { opts.RenameNodes = *cfg.ServiceCheck.RenameNodes }
	if cfg.ServiceCheck.GeoIPURL != "" { opts.GeoIPURL = cfg.ServiceCheck.GeoIPURL }
	if cfg.ServiceCheck.NameFormat != "" { opts.NameFormat = cfg.ServiceCheck.NameFormat }
	if cfg.ServiceCheck.CountryFormat != "" {
		format := strings.ToLower(strings.TrimSpace(cfg.ServiceCheck.CountryFormat))
		if format != "full" && format != "code" && format != "none" {
			return opts, fmt.Errorf("service_check.country_format must be full, code, or none: %q", cfg.ServiceCheck.CountryFormat)
		}
		opts.CountryFormat = format
	}
	if opts.NameFormat != "" && !strings.Contains(opts.NameFormat, "{flag}") && !strings.Contains(opts.NameFormat, "{country}") && !strings.Contains(opts.NameFormat, "{download}") && !strings.Contains(opts.NameFormat, "{upload}") { return opts, fmt.Errorf("service_check.name_format must include {flag}, {country}, {download}, or {upload}") }
	if cfg.ServiceCheck.SpeedDecimals != nil { if *cfg.ServiceCheck.SpeedDecimals < 0 || *cfg.ServiceCheck.SpeedDecimals > 3 { return opts, fmt.Errorf("service_check.speed_decimals must be between 0 and 3") }; opts.SpeedDecimals = *cfg.ServiceCheck.SpeedDecimals }
	if cfg.RemoteUpload.Enabled {
		r := cfg.RemoteUpload
		if r.Method == "" {
			r.Method = "sftp"
		}
		if r.Port == 0 {
			r.Port = 22
		}
		if r.Auth == "" {
			r.Auth = "private_key"
		}
		if r.Host == "" || r.Username == "" || r.RemoteDirectory == "" {
			return opts, fmt.Errorf("remote_upload requires host, username and remote_directory")
		}
		if r.Method != "sftp" {
			return opts, fmt.Errorf("remote_upload.method must be sftp")
		}
		if r.Auth != "private_key" && r.Auth != "password" {
			return opts, fmt.Errorf("remote_upload.auth must be private_key or password")
		}
		if r.Auth == "private_key" && r.PrivateKey == "" {
			r.PrivateKey = "~/.ssh/id_ed25519"
		}
		if r.Auth == "password" && r.Password == "" {
			return opts, fmt.Errorf("remote_upload.password is required when auth=password")
		}
		if strings.HasPrefix(r.PrivateKey, "~/") {
			if home, e := os.UserHomeDir(); e == nil {
				r.PrivateKey = filepath.Join(home, r.PrivateKey[2:])
			}
		}
		opts.RemoteUpload = RemoteUploadOptions{Enabled: r.Enabled, Method: r.Method, Host: r.Host, Port: r.Port, Username: r.Username, Auth: r.Auth, PrivateKey: r.PrivateKey, Password: r.Password, RemoteDirectory: r.RemoteDirectory, Interface: strings.TrimSpace(r.Interface)}
	}
	if cfg.Output.Directory != "" {
		opts.OutDir = cfg.Output.Directory
	}
	opts.SplitByProtocol = cfg.Output.SplitByProtocol
	if cfg.Output.SubscriptionText != nil {
		opts.WriteText = *cfg.Output.SubscriptionText
	}
	if cfg.Output.GeminiFile != nil {
		opts.GeminiFile = *cfg.Output.GeminiFile
	}

	// Make a starter source list on first run so the next error is actionable.
	if err := writeDefaultIfMissing(opts.Sources, "# One subscription URL, local file path, or single share link per line.\n# https://example.net/subscription\n# ./my-subscription.txt\n"); err != nil {
		return opts, fmt.Errorf("create sources file %s: %w", opts.Sources, err)
	}
	return opts, nil
}

func writeDefaultIfMissing(path, content string) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		_ = os.Remove(path)
		return err
	}
	return f.Close()
}

func normalizeProtocols(protocols []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(protocols))
	for _, p := range protocols {
		p = strings.ToLower(strings.TrimSpace(p))
		if p == "shadowsocks" {
			p = "ss"
		}
		if p == "hy2" {
			p = "hysteria2"
		}
		if p == "socks5" {
			p = "socks"
		}
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func ExampleConfig() string {
	return `{
  "network": {
    "interface": "auto",
    "isolate_source_downloads": false
  },
  "loop": {
    "interval": "0s"
  },
  "sources": {
    "file": "sources.txt",
    "timeout": "30s"
  },
  "protocols": {
    "enabled": ["vless", "vmess", "trojan", "ss", "hysteria", "hysteria2", "tuic", "socks", "http"]
  },
  "url_test": {
    "url": "https://www.gstatic.com/generate_204",
    "concurrency": 8,
    "timeout": "12s"
  },
  "speed_test": {
    "provider": "cloudflare",
    "cloudflare_fallback": {
      "enabled": false
    },
    "concurrency": 2,
    "timeout": "85s",
    "first_byte_timeout": "10s",
    "max_duration": "30s",
    "sample_interval": "500ms",
    "min_download_mbps": 8,
    "min_upload_mbps": 8,
    "download_bytes": 2097152,
    "upload_bytes": 2097152,
    "download_url": "https://speed.cloudflare.com/__down?bytes={bytes}",
    "upload_url": "https://speed.cloudflare.com/__up"
  },
  "gaming_test": {
    "enabled": false,
    "concurrency": 1,
    "timeout": "90s",
    "latency_samples": 12,
    "sample_interval": "250ms",
    "download_bytes": 16777216,
    "upload_bytes": 8388608,
    "max_latency_ms": 50,
    "max_jitter_ms": 10,
    "max_loaded_latency_ms": 80,
    "max_loaded_jitter_ms": 15
  },
  "service_check": {
    "concurrency": 4,
    "timeout": "25s",
    "lookup_ip": false,
    "rename_nodes": true,
    "geoip_url": "https://ipwho.is/",
    "name_format": "{flag} {country} {download}↓{upload}↑",
    "country_format": "full",
    "speed_decimals": 1
  },
  "output": {
    "directory": "out",
    "split_by_protocol": true,
    "subscription_text": true,
    "gemini_file": false
  },
  "remote_upload": {
    "enabled": false,
    "method": "sftp",
    "host": "79.72.16.241",
    "port": 22,
    "username": "ubuntu",
    "auth": "private_key",
    "private_key": "~/.ssh/id_ed25519",
    "password": "",
    "remote_directory": "/home/ubuntu/oklol/pidarasy",
    "interface": ""
  }
}
`
}
