package app

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/negr666omg/subwatch/internal/sub"
)

// runFailureLog gathers errors from parsing and every test stage in memory.
// It writes one consolidated file at run completion instead of causing one
// disk write per failed config.
type runFailureLog struct {
	mu      sync.Mutex
	started time.Time
	entries []string
}

func newRunFailureLog() *runFailureLog {
	return &runFailureLog{started: time.Now()}
}

// RecordStartupFailure writes failures that happen before Run can start,
// such as an invalid config file, to the same consolidated output location.
func RecordStartupFailure(dir, stage string, cause error) error {
	if cause == nil {
		return nil
	}
	if strings.TrimSpace(dir) == "" {
		dir = "out"
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create failure-log directory: %w", err)
	}
	log := newRunFailureLog()
	log.Add(stage, cause.Error())
	return log.Write(filepath.Join(dir, "failed.txt"))
}

func (l *runFailureLog) Add(stage, message string) {
	if l == nil {
		return
	}
	message = cleanLiveText(message)
	if message == "" {
		return
	}
	l.mu.Lock()
	l.entries = append(l.entries, fmt.Sprintf("[%s] %s", strings.ToUpper(strings.TrimSpace(stage)), message))
	l.mu.Unlock()
}

func (l *runFailureLog) AddNode(stage string, node sub.Node, reason string) {
	name := cleanLiveText(node.Name)
	if name == "" {
		name = "unnamed config"
	}
	host := ""
	if parsed, err := url.Parse(node.URI); err == nil {
		host = parsed.Hostname()
	}
	identity := fmt.Sprintf("%s (%s)", name, protocolOf(node))
	if host != "" {
		identity += " @ " + host
	}
	if strings.TrimSpace(reason) == "" {
		reason = "no error details provided"
	}
	l.Add(stage, identity+": "+reason)
}

func (l *runFailureLog) Count() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func (l *runFailureLog) Write(path string) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	entries := append([]string(nil), l.entries...)
	started := l.started
	l.mu.Unlock()

	var output strings.Builder
	fmt.Fprintln(&output, "SubWatch failure log")
	fmt.Fprintf(&output, "Started: %s\n", started.Format(time.RFC3339))
	fmt.Fprintf(&output, "Entries: %d\n\n", len(entries))
	if len(entries) == 0 {
		fmt.Fprintln(&output, "No errors recorded.")
	} else {
		for index, entry := range entries {
			fmt.Fprintf(&output, "%04d %s\n", index+1, entry)
		}
	}
	return writeAtomic(filepath.Clean(path), []byte(output.String()))
}

// redactFailureInput removes inline credentials and query tokens before an
// invalid source line is copied into the human-readable error log.
func redactFailureInput(line string) string {
	line = strings.TrimSpace(line)
	parsed, err := url.Parse(line)
	if err == nil && parsed.Scheme != "" {
		if parsed.Host != "" {
			parsed.User = nil
			parsed.RawQuery = ""
			parsed.ForceQuery = false
			parsed.Fragment = ""
			return parsed.String()
		}
		return strings.ToLower(parsed.Scheme) + "://<redacted>"
	}
	return truncate(cleanLiveText(line), 120)
}
