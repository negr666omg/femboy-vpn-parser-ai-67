package app

import (
	"context"
	"encoding/base64"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type liveMouseTarget struct {
	row      int
	startCol int
	endCol   int
	copyText string
}

// liveTerminalMouse enables xterm SGR mouse reporting only while SubWatch is
// running in an interactive terminal. Ctrl-C remains a terminal signal because
// ICANON and ECHO are disabled without clearing ISIG.
type liveTerminalMouse struct {
	stdout *os.File
	stdin  *os.File
	term   unix.Termios

	targetsMu sync.RWMutex
	targets   []liveMouseTarget

	cursorRows   chan int
	copyRequests chan string
	done         chan struct{}
	wg           sync.WaitGroup
	closeOnce    sync.Once
	queryMu      sync.Mutex
}

func newLiveTerminalMouse(w io.Writer) *liveTerminalMouse {
	stdout, ok := w.(*os.File)
	if !ok || stdout != os.Stdout || os.Stdin == nil {
		return nil
	}

	stdinFD := int(os.Stdin.Fd())
	stdoutFD := int(stdout.Fd())
	original, err := unix.IoctlGetTermios(stdinFD, unix.TCGETS)
	if err != nil {
		return nil
	}
	if _, err := unix.IoctlGetTermios(stdoutFD, unix.TCGETS); err != nil {
		return nil
	}
	size, err := unix.IoctlGetWinsize(stdoutFD, unix.TIOCGWINSZ)
	if err != nil || size == nil || size.Col == 0 {
		return nil
	}

	interactive := *original
	interactive.Lflag &^= unix.ICANON | unix.ECHO | unix.ECHONL
	interactive.Cc[unix.VMIN] = 0
	interactive.Cc[unix.VTIME] = 1
	if err := unix.IoctlSetTermios(stdinFD, unix.TCSETS, &interactive); err != nil {
		return nil
	}

	mouse := &liveTerminalMouse{
		stdout:       stdout,
		stdin:        os.Stdin,
		term:         *original,
		cursorRows:   make(chan int, 4),
		copyRequests: make(chan string, 8),
		done:         make(chan struct{}),
	}
	if _, err := stdout.Write([]byte("\x1b[?1000h\x1b[?1006h")); err != nil {
		_ = unix.IoctlSetTermios(stdinFD, unix.TCSETS, original)
		return nil
	}

	mouse.wg.Add(2)
	go mouse.readLoop()
	go mouse.copyLoop()
	return mouse
}

func (m *liveTerminalMouse) Close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		close(m.done)
		m.setTargets(nil)
		_, _ = m.stdout.Write([]byte("\x1b[?1006l\x1b[?1000l"))
		// Let the non-canonical reader observe done before restoring canonical
		// input; otherwise Read could wait forever for a newline.
		m.wg.Wait()
		_ = unix.IoctlSetTermios(int(m.stdin.Fd()), unix.TCSETS, &m.term)
	})
}

func (m *liveTerminalMouse) setTargets(targets []liveMouseTarget) {
	if m == nil {
		return
	}
	valid := make([]liveMouseTarget, 0, len(targets))
	for _, target := range targets {
		if target.row > 0 && target.startCol > 0 && target.endCol >= target.startCol && target.copyText != "" {
			valid = append(valid, target)
		}
	}
	m.targetsMu.Lock()
	m.targets = valid
	m.targetsMu.Unlock()
}

func (m *liveTerminalMouse) cursorRow() (int, bool) {
	if m == nil {
		return 0, false
	}
	m.queryMu.Lock()
	defer m.queryMu.Unlock()

	for {
		select {
		case <-m.cursorRows:
		default:
			goto drained
		}
	}

drained:
	select {
	case <-m.done:
		return 0, false
	default:
	}
	if _, err := m.stdout.Write([]byte("\x1b[6n")); err != nil {
		return 0, false
	}
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case row := <-m.cursorRows:
		return row, true
	case <-timer.C:
		return 0, false
	case <-m.done:
		return 0, false
	}
}

func (m *liveTerminalMouse) readLoop() {
	defer m.wg.Done()
	buffer := make([]byte, 256)
	var pending []byte
	for {
		select {
		case <-m.done:
			return
		default:
		}

		n, err := m.stdin.Read(buffer)
		if err != nil {
			select {
			case <-m.done:
				return
			default:
				continue
			}
		}
		if n == 0 {
			continue
		}
		pending = append(pending, buffer[:n]...)
		pending = m.consumeInput(pending)
	}
}

func (m *liveTerminalMouse) consumeInput(pending []byte) []byte {
	for len(pending) > 0 {
		start := strings.IndexByte(string(pending), '\x1b')
		if start < 0 {
			return nil
		}
		if start > 0 {
			pending = pending[start:]
		}
		if len(pending) < 3 {
			return pending
		}
		if pending[1] != '[' {
			pending = pending[2:]
			continue
		}

		finalIndex := -1
		for i := 2; i < len(pending); i++ {
			if pending[i] >= 0x40 && pending[i] <= 0x7e {
				finalIndex = i
				break
			}
		}
		if finalIndex < 0 {
			return pending
		}

		sequence := string(pending[2:finalIndex])
		switch pending[finalIndex] {
		case 'R':
			if row, ok := parseCursorPosition(sequence); ok {
				select {
				case m.cursorRows <- row:
				default:
				}
			}
		case 'M', 'm':
			if button, col, row, pressed, ok := parseSGRMouseSequence(sequence, pending[finalIndex]); ok &&
				pressed && button&3 == 0 && button&32 == 0 {
				m.handleClick(col, row)
			}
		}
		pending = pending[finalIndex+1:]
	}
	return nil
}

func parseCursorPosition(sequence string) (int, bool) {
	parts := strings.Split(sequence, ";")
	if len(parts) != 2 {
		return 0, false
	}
	row, rowErr := strconv.Atoi(parts[0])
	col, colErr := strconv.Atoi(parts[1])
	return row, rowErr == nil && colErr == nil && row > 0 && col > 0
}

func parseSGRMouseSequence(sequence string, final byte) (button, col, row int, pressed, ok bool) {
	if !strings.HasPrefix(sequence, "<") || (final != 'M' && final != 'm') {
		return 0, 0, 0, false, false
	}
	parts := strings.Split(sequence[1:], ";")
	if len(parts) != 3 {
		return 0, 0, 0, false, false
	}
	button, buttonErr := strconv.Atoi(parts[0])
	col, colErr := strconv.Atoi(parts[1])
	row, rowErr := strconv.Atoi(parts[2])
	if buttonErr != nil || colErr != nil || rowErr != nil || col < 1 || row < 1 {
		return 0, 0, 0, false, false
	}
	return button, col, row, final == 'M', true
}

func (m *liveTerminalMouse) handleClick(col, row int) {
	m.targetsMu.RLock()
	var copyText string
	for _, target := range m.targets {
		if target.row == row && col >= target.startCol && col <= target.endCol {
			copyText = target.copyText
			break
		}
	}
	m.targetsMu.RUnlock()
	if copyText == "" {
		return
	}
	select {
	case m.copyRequests <- copyText:
	default:
	}
}

func (m *liveTerminalMouse) copyLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.done:
			return
		case value := <-m.copyRequests:
			if copyLiveTextToClipboard(m.stdout, value) {
				_, _ = m.stdout.Write([]byte("\a"))
			}
		}
	}
}

func copyLiveTextToClipboard(stdout *os.File, value string) bool {
	if value == "" {
		return false
	}
	commands := []struct {
		name string
		args []string
	}{
		{name: "wl-copy"},
		{name: "xclip", args: []string{"-selection", "clipboard", "-in"}},
		{name: "xsel", args: []string{"--clipboard", "--input"}},
	}
	for _, item := range commands {
		ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
		cmd := exec.CommandContext(ctx, item.name, item.args...)
		cmd.Stdin = strings.NewReader(value)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		err := cmd.Run()
		cancel()
		if err == nil {
			return true
		}
	}

	if stdout == nil {
		return false
	}
	payload := base64.StdEncoding.EncodeToString([]byte(value))
	_, err := stdout.Write([]byte("\x1b]52;c;" + payload + "\a"))
	return err == nil
}

// liveTitleClickRange returns one-based terminal columns for the visible title
// following the supplied row header. The whole original value is copied even
// when the displayed title is shortened to fit the terminal.
func liveTitleClickRange(line, head string) (int, int, bool) {
	prefix := head + " | "
	offset := strings.Index(line, prefix)
	if offset < 0 {
		return 0, 0, false
	}
	startByte := offset + len(prefix)
	rest := line[startByte:]
	if separator := strings.Index(rest, " | "); separator >= 0 {
		rest = rest[:separator]
	} else if ping := strings.LastIndex(rest, "ping:"); ping >= 0 {
		rest = rest[:ping]
	}
	visible := strings.TrimRight(rest, " ")
	if visible == "" {
		return 0, 0, false
	}
	startCol := displayWidth(line[:startByte]) + 1
	endCol := startCol + displayWidth(visible) - 1
	return startCol, endCol, endCol >= startCol
}
