package app

import (
	"context"
	"fmt"
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
	sshArgs := []string{"-p", port, "-o", "StrictHostKeyChecking=accept-new"}
	sftpArgs := []string{"-P", port, "-o", "StrictHostKeyChecking=accept-new"}
	if opts.Auth == "private_key" {
		sshArgs = append(sshArgs, "-i", opts.PrivateKey, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes")
		sftpArgs = append(sftpArgs, "-i", opts.PrivateKey, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes")
	}
	sshArgs = append(sshArgs, target, "mkdir -p -- "+shellQuote(opts.RemoteDirectory))
	sftpArgs = append(sftpArgs, "-b", "-")

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

	fmt.Fprintf(stdout, "[UPLOAD] Connecting to %s:%s via SFTP...\n", opts.Host, port)
	if err := run("ssh", sshArgs, ""); err != nil {
		return fmt.Errorf("prepare remote directory: %w", err)
	}
	var batch strings.Builder
	for _, file := range files {
		fmt.Fprintf(&batch, "put %s %s\n", sftpQuote(file), sftpQuote(strings.TrimRight(opts.RemoteDirectory, "/")+"/"+filepath.Base(file)))
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
