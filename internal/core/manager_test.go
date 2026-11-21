package core

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestFindOrDownloadProgressContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	path, err := FindOrDownloadProgressContext(ctx, "", io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got path=%q err=%v", path, err)
	}
}
