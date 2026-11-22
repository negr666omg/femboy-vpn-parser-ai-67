package main

import (
	"context"
	"testing"
	"time"
)

func TestWaitForNextCycleSkipsDisabledInterval(t *testing.T) {
	if !waitForNextCycle(context.Background(), 0) {
		t.Fatal("zero interval should continue immediately")
	}
}

func TestWaitForNextCycleCanBeInterrupted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if waitForNextCycle(ctx, time.Hour) {
		t.Fatal("wait should stop when the context is canceled")
	}
}

func TestWaitForNextCycleCompletesInterval(t *testing.T) {
	if !waitForNextCycle(context.Background(), 5*time.Millisecond) {
		t.Fatal("wait should continue after the configured interval")
	}
}
