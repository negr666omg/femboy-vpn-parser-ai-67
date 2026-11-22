//go:build !linux

package core

import (
	"fmt"
	"io"
)

func CleanupNetworkIsolation(output io.Writer) error {
	return fmt.Errorf("network cleanup is supported on Linux only")
}
