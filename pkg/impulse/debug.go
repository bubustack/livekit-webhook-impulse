package impulse

import (
	"context"
	"log/slog"
	"strings"

	sdk "github.com/bubustack/bubu-sdk-go"
	cfgpkg "github.com/bubustack/livekit-webhook-impulse/pkg/config"
	livekit "github.com/livekit/protocol/livekit"
)

func (i *LiveKitImpulse) debugEnabled(ctx context.Context, logger *slog.Logger) bool {
	if sdk.DebugModeEnabled() {
		return true
	}
	if logger == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return logger.Enabled(ctx, slog.LevelDebug)
}

func (i *LiveKitImpulse) logWebhookEventDebug(
	ctx context.Context,
	logger *slog.Logger,
	event *livekit.WebhookEvent,
	sessionKey string,
	payload map[string]any,
) {
	if !i.debugEnabled(ctx, logger) || event == nil {
		return
	}
	attrs := []slog.Attr{
		slog.String("event", strings.TrimSpace(event.GetEvent())),
		slog.String("eventID", strings.TrimSpace(event.GetId())),
	}
	if sessionKey != "" {
		attrs = append(attrs, slog.String("sessionKey", sessionKey))
	}
	if room := event.GetRoom(); room != nil {
		if name := strings.TrimSpace(room.GetName()); name != "" {
			attrs = append(attrs, slog.String("room", name))
		}
		if sid := strings.TrimSpace(room.GetSid()); sid != "" {
			attrs = append(attrs, slog.String("roomSID", sid))
		}
	}
	if participant := event.GetParticipant(); participant != nil {
		if id := strings.TrimSpace(participant.GetIdentity()); id != "" {
			attrs = append(attrs, slog.String("participant", id))
		}
		if sid := strings.TrimSpace(participant.GetSid()); sid != "" {
			attrs = append(attrs, slog.String("participantSID", sid))
		}
	}
	if track := event.GetTrack(); track != nil {
		if name := strings.TrimSpace(track.GetName()); name != "" {
			attrs = append(attrs, slog.String("track", name))
		}
		if sid := strings.TrimSpace(track.GetSid()); sid != "" {
			attrs = append(attrs, slog.String("trackSID", sid))
		}
	}
	if len(payload) > 0 {
		attrs = append(attrs, slog.Any("payload", payload))
	}
	logger.Debug("LiveKit webhook payload", attrsToArgs(attrs)...)
}

func (i *LiveKitImpulse) logPolicyMatchDebug(
	ctx context.Context,
	logger *slog.Logger,
	eventType, sessionKey string,
	policies []cfgpkg.Policy,
) {
	if !i.debugEnabled(ctx, logger) {
		return
	}
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		if policy.Name != "" {
			names = append(names, policy.Name)
		}
	}
	logger.Debug("LiveKit policy matches",
		slog.String("event", eventType),
		slog.String("sessionKey", sessionKey),
		slog.Any("policies", names),
	)
}
