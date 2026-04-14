package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	sdk "github.com/bubustack/bubu-sdk-go"
	lkimp "github.com/bubustack/livekit-webhook-impulse/pkg/impulse"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := sdk.RunImpulse(ctx, lkimp.New()); err != nil {
		log.Fatalf("livekit-webhook impulse failed: %v", err)
	}
}
