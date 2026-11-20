package app

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// uploadOutput sends the current user-facing output files over OpenSSH SFTP.
// Password authentication uses sshpass when installed; key authentication uses
// the system OpenSSH client and never copies private-key material to the server.
func uploadOutput(ctx context.Context, opts RemoteUploadOptions, outDir string, stdout, stderr interface{ Write([]byte) (int, error) }) error {
	if !opts.Enabled {
		return nil
	}
	fmt.Fprintf(stdout, "[UPLOAD] Enabled; method=SFTP, auth=%s, target=%s@%s:%d, remote_dir=%s\n", opts.Auth, opts.Username, opts.Host, opts.Port, opts.RemoteDirectory)
	for _, name := range []string{"ssh", "sftp"} {
		if path, lookErr := exec.LookPath(name); lookErr != nil {
			return fmt.Errorf("required command %q not found in PATH: %w", name, lookErr)
		} else {
			fmt.Fprintf(stdout, "[UPLOAD] Found %s: %s\n", name, path)
		}
	}
	if opts.Auth == "password" {
		if path, lookErr := exec.LookPath("sshpass"); lookErr != nil {
			return fmt.Errorf("password authentication requires sshpass, but it was not found in PATH: %w", lookErr)
		} else {
			fmt.Fprintf(stdout, "[UPLOAD] Found sshpass: %s\n", path)
		}
	} else if opts.Auth == "private_key" {
		info, statErr := os.Stat(opts.PrivateKey)
		if statErr != nil {
			return fmt.Errorf("cannot access SSH private key %q: %w", opts.PrivateKey, statErr)
		}
		if info.IsDir() {
			return fmt.Errorf("SSH private key path %q is a directory", opts.PrivateKey)
		}
		fmt.Fprintf(stdout, "[UPLOAD] SSH private key found: %s\n", opts.PrivateKey)
	}
	files, err := filepath.Glob(filepath.Join(outDir, "*.txt"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	if len(files) == 0 {
		fmt.Fprintln(stdout, "[UPLOAD] No .txt output files to upload")
		return nil
	}

	target := opts.Username + "@" + opts.Host
	port := fmt.Sprint(opts.Port)
	sshArgs := []string{"-p", port, "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=2"}
	sftpArgs := []string{"-P", port, "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15", "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=2"}
	if strings.TrimSpace(opts.Interface) != "" && opts.Interface != "auto" {
		it, lookupErr := net.InterfaceByName(opts.Interface)
		if lookupErr != nil {
			return fmt.Errorf("SFTP bind interface %q not found: %w", opts.Interface, lookupErr)
		}
		if it.Flags&net.FlagUp == 0 {
			return fmt.Errorf("SFTP bind interface %q exists but is DOWN", opts.Interface)
		}
		sshArgs = append(sshArgs, "-o", "BindInterface="+opts.Interface)
		sftpArgs = append(sftpArgs, "-o", "BindInterface="+opts.Interface)
		fmt.Fprintf(stdout, "[UPLOAD] Binding SSH/SFTP to interface: %s\n", opts.Interface)
	} else {
		fmt.Fprintln(stdout, "[UPLOAD] SSH/SFTP interface: automatic (system routing)")
	}
	if opts.Auth == "private_key" {
		sshArgs = append(sshArgs, "-i", opts.PrivateKey, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes")
		sftpArgs = append(sftpArgs, "-i", opts.PrivateKey, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes")
	}
	sshArgs = append(sshArgs, target, "mkdir -p -- "+shellQuote(opts.RemoteDirectory))
	sftpArgs = append(sftpArgs, "-b", "-", target)

	run := func(name string, args []string, input string) error {
		cmd := exec.CommandContext(ctx, name, args...)
		if input != "" {
			cmd.Stdin = strings.NewReader(input)
		}
		if opts.Auth == "password" {
			cmd.Env = append(os.Environ(), "SSHPASS="+opts.Password)
			cmd = exec.CommandContext(ctx, "sshpass", append([]string{"-e", name}, args...)...)
			cmd.Env = append(os.Environ(), "SSHPASS="+opts.Password)
			if input != "" {
				cmd.Stdin = strings.NewReader(input)
			}
		}
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s failed: %s: %w", name, strings.TrimSpace(string(output)), err)
		}
		return nil
	}

	fmt.Fprintf(stdout, "[UPLOAD] Preparing remote directory and connecting to %s:%s...\n", opts.Host, port)
	if err := run("ssh", sshArgs, ""); err != nil {
		return fmt.Errorf("prepare remote directory: %w", err)
	}
	var batch strings.Builder
	for _, file := range files {
		destination := strings.TrimRight(opts.RemoteDirectory, "/") + "/" + filepath.Base(file)
		fmt.Fprintf(stdout, "[UPLOAD] Queueing %s -> %s\n", filepath.Base(file), destination)
		fmt.Fprintf(&batch, "put %s %s\n", sftpQuote(file), sftpQuote(destination))
	}
	if err := run("sftp", sftpArgs, batch.String()); err != nil {
		return fmt.Errorf("SFTP upload: %w", err)
	}
	fmt.Fprintf(stdout, "[UPLOAD] Uploaded %d files to %s:%s\n", len(files), opts.Host, opts.RemoteDirectory)
	return nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func sftpQuote(s string) string {
	return "\"" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "\"", "\\\"") + "\""
}
