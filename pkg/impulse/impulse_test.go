package impulse

import (
	"context"
	"testing"

	sdkengram "github.com/bubustack/bubu-sdk-go/engram"

	cfgpkg "github.com/bubustack/livekit-webhook-impulse/pkg/config"
)

func TestInitRejectsNilSecrets(t *testing.T) {
	t.Parallel()

	imp := New()
	err := imp.Init(context.Background(), cfgpkg.Config{}, nil)
	if err == nil {
		t.Fatalf("expected error when secrets are nil")
	}
}

func TestInitRejectsMissingCredentialKeys(t *testing.T) {
	t.Parallel()

	imp := New()
	secrets := sdkengram.NewSecrets(context.Background(), map[string]string{
		"API_KEY": "key-only",
	})

	err := imp.Init(context.Background(), cfgpkg.Config{}, secrets)
	if err == nil {
		t.Fatalf("expected error when API_SECRET is missing")
	}
}
