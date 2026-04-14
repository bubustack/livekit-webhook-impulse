// package impulse implements the core logic for the LiveKit webhook impulse.
// This component acts as a bridge between the LiveKit server's event stream
// and the bobrapet workflow engine. Its sole purpose is to receive, authenticate,
// and filter webhook events before translating them into durable StoryTrigger requests.
//
// As per the architectural principles of bobrapet, this impulse is designed
// to be completely stateless, allowing it to be scaled horizontally for high
// availability and throughput. All logic is self-contained, and it does not
// depend on any external state stores.
package impulse

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"

	sdk "github.com/bubustack/bubu-sdk-go"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	sdkk8s "github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/livekit/protocol/auth"
	livekit "github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/encoding/protojson"

	cfgpkg "github.com/bubustack/livekit-webhook-impulse/pkg/config"
)

// LiveKitImpulse implements the sdk.Impulse interface for LiveKit webhooks.
// It holds the impulse's configuration, secrets, and the LiveKit key provider
// necessary for authenticating incoming webhooks.
type LiveKitImpulse struct {
	cfg         *cfgpkg.Config
	secrets     *sdkengram.Secrets
	keys        auth.KeyProvider
	dispatcher  *sdk.StoryDispatcher
	startEvents map[string]struct{}
	endEvents   map[string]struct{}
	logger      *slog.Logger
}

func New() *LiveKitImpulse {
	return &LiveKitImpulse{
		dispatcher: sdk.NewStoryDispatcher(),
	}
}

func (i *LiveKitImpulse) loggerWithContext(ctx context.Context) *slog.Logger {
	if ctx != nil {
		if l := sdk.LoggerFromContext(ctx); l != nil {
			return l.With("component", "livekit-webhook-impulse")
		}
	}
	if i.logger != nil {
		return i.logger
	}
	return slog.Default()
}

func normalizeEventList(events []string) []string {
	set := make(map[string]struct{})
	for _, ev := range events {
		norm := strings.ToLower(strings.TrimSpace(ev))
		if norm == "" {
			continue
		}
		set[norm] = struct{}{}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for ev := range set {
		out = append(out, ev)
	}
	sort.Strings(out)
	return out
}

func toEventSet(events []string) map[string]struct{} {
	set := make(map[string]struct{}, len(events))
	for _, ev := range events {
		set[ev] = struct{}{}
	}
	return set
}

func mergeAllowlist(base []string, extras ...[]string) []string {
	set := make(map[string]struct{})
	for _, raw := range base {
		if norm := strings.ToLower(strings.TrimSpace(raw)); norm != "" {
			set[norm] = struct{}{}
		}
	}
	for _, group := range extras {
		for _, raw := range group {
			if norm := strings.ToLower(strings.TrimSpace(raw)); norm != "" {
				set[norm] = struct{}{}
			}
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for ev := range set {
		out = append(out, ev)
	}
	sort.Strings(out)
	return out
}

func (i *LiveKitImpulse) isStartEvent(eventType string) bool {
	if len(i.startEvents) == 0 {
		return false
	}
	_, ok := i.startEvents[strings.ToLower(strings.TrimSpace(eventType))]
	return ok
}

func (i *LiveKitImpulse) isEndEvent(eventType string) bool {
	if len(i.endEvents) == 0 {
		return false
	}
	_, ok := i.endEvents[strings.ToLower(strings.TrimSpace(eventType))]
	return ok
}

func sessionKeyFromEvent(event *livekit.WebhookEvent) string {
	if event == nil {
		return ""
	}
	if room := event.GetRoom(); room != nil {
		if sid := strings.TrimSpace(room.GetSid()); sid != "" {
			return sid
		}
		if name := strings.TrimSpace(room.GetName()); name != "" {
			return name
		}
	}
	if id := strings.TrimSpace(event.GetId()); id != "" {
		return id
	}
	return ""
}

func (i *LiveKitImpulse) handleRoomEnded(ctx context.Context, sessionKey string) {
	logger := i.loggerWithContext(ctx)
	if sessionKey == "" {
		logger.Warn("Room end event missing room identifier; skipping StoryRun shutdown")
		return
	}
	if i.dispatcher == nil {
		logger.Warn("Dispatcher not initialized; cannot stop story", slog.String("room", sessionKey))
		return
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	session, err := i.dispatcher.Stop(shutdownCtx, sessionKey)
	switch {
	case err == nil:
		if session != nil {
			logger.Info("Ended story for room",
				slog.String("story", session.StoryName),
				slog.String("room", sessionKey),
				slog.String("storyRunNamespace", session.Namespace),
				slog.String("storyRun", session.StoryRun),
			)
		} else {
			logger.Info("Ended story for room", slog.String("room", sessionKey))
		}
	case errors.Is(err, sdk.ErrImpulseSessionNotFound):
		logger.Info("No active StoryRun recorded; skipping shutdown", slog.String("room", sessionKey))
	case errors.Is(err, sdk.ErrStoryRunNotFound):
		logger.Info("StoryRun already gone; clearing session", slog.String("room", sessionKey))
	default:
		if session != nil {
			logger.Error("Failed to stop StoryRun",
				slog.String("namespace", session.Namespace),
				slog.String("storyRun", session.StoryRun),
				slog.String("room", sessionKey),
				slog.Any("error", err),
			)
		} else {
			logger.Error("Failed to stop StoryRun for room",
				slog.String("room", sessionKey),
				slog.Any("error", err),
			)
		}
	}
}

func (i *LiveKitImpulse) Init(ctx context.Context, cfg cfgpkg.Config, secrets *sdkengram.Secrets) error {
	logger := i.loggerWithContext(ctx)
	if i.dispatcher == nil {
		i.dispatcher = sdk.NewStoryDispatcher()
	}
	// Apply safe defaults so a minimal YAML works out of the box.
	if cfg.EventsAllowlist == nil {
		if derived := collectPolicyEvents(cfg.Policies); len(derived) > 0 {
			cfg.EventsAllowlist = derived
		} else {
			cfg.EventsAllowlist = []string{
				"participant_joined",
				"track_published",
				"room_started",
				"room_finished",
				"room_ended",
				"participant_left",
				"track_unpublished",
			}
		}
	}
	if cfg.Path == "" {
		cfg.Path = "/webhook"
	}
	if len(cfg.SelfIdentities) == 0 {
		cfg.SelfIdentities = []string{"bubu-agent"}
	}
	if cfg.StartEvents == nil {
		cfg.StartEvents = []string{"participant_joined", "track_published"}
	}
	if cfg.EndEvents == nil {
		cfg.EndEvents = []string{"room_finished", "room_ended", "participant_left", "track_unpublished"}
	}

	cfg.StartEvents = normalizeEventList(cfg.StartEvents)
	cfg.EndEvents = normalizeEventList(cfg.EndEvents)
	cfg.EventsAllowlist = mergeAllowlist(cfg.EventsAllowlist, cfg.StartEvents, cfg.EndEvents)
	i.startEvents = toEventSet(cfg.StartEvents)
	i.endEvents = toEventSet(cfg.EndEvents)
	i.cfg = &cfg
	i.secrets = secrets
	if secrets == nil {
		return fmt.Errorf("secret with keys 'API_KEY' and 'API_SECRET' must be provided")
	}
	// Secrets must be provided by the operator via the secrets object.
	// This removes the dependency on specific environment variable names in the code
	// and makes the component more secure and testable.
	apiKey, apiKeyOk := secrets.Get("API_KEY")
	apiSecret, apiSecretOk := secrets.Get("API_SECRET")
	if !apiKeyOk || !apiSecretOk {
		return fmt.Errorf("secret with keys 'API_KEY' and 'API_SECRET' must be provided")
	}
	i.keys = auth.NewSimpleKeyProvider(apiKey, apiSecret)
	if logger != nil {
		keyHash := fmt.Sprintf("%x", sha256.Sum256([]byte(apiKey)))
		logger.Info("Loaded LiveKit webhook credentials",
			slog.String("apiKeyHash", keyHash[:8]),
			slog.Int("secretCount", len(secrets.GetAll())),
		)
	}
	return nil
}

// Run is the main entry point for the impulse's long-running process, called by the SDK.
// It starts an HTTP server to listen for incoming webhook events from LiveKit.
func (i *LiveKitImpulse) Run(ctx context.Context, k8sClient *sdkk8s.Client) error {
	logger := i.loggerWithContext(ctx)
	i.logger = logger
	mux := http.NewServeMux()
	mux.HandleFunc(i.cfg.Path, i.webhookHandler(k8sClient))

	// A small HTTP server keeps the impulse always-on.
	// The server will be gracefully shut down when the context is canceled.
	srv := &http.Server{Addr: ":8080", Handler: mux}
	logger.Info("LiveKit webhook impulse listening", slog.String("addr", srv.Addr), slog.String("path", i.cfg.Path))

	go func() {
		<-ctx.Done()
		// Allow a grace period for the server to shut down.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("HTTP server exited unexpectedly", slog.Any("error", err))
		return fmt.Errorf("http server error: %w", err)
	}
	return nil
}

// webhookHandler is the core logic for processing a single webhook event.
func (i *LiveKitImpulse) webhookHandler(_ *sdkk8s.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger := i.loggerWithContext(r.Context())
		if !validWebhookRequest(w, r) {
			return
		}
		event, ok := i.receiveWebhookEvent(r, logger, w)
		if !ok {
			return
		}
		evType := strings.TrimSpace(event.GetEvent())
		sessionKey := sessionKeyFromEvent(event)
		if i.handleNonStartingEvent(r.Context(), w, logger, event, evType, sessionKey) {
			return
		}

		inputs, eventPayload, roomName, participantIdentity := i.buildEventInputs(event)
		i.logWebhookEventDebug(r.Context(), logger, event, sessionKey, eventPayload)
		matchingPolicies := i.matchPolicies(evType, roomName, participantIdentity)
		i.logPolicyMatchDebug(r.Context(), logger, evType, sessionKey, matchingPolicies)
		if err := i.dispatchMatchingStories(
			r.Context(),
			logger,
			evType,
			sessionKey,
			roomName,
			participantIdentity,
			inputs,
			matchingPolicies,
		); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func validWebhookRequest(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if ctype := r.Header.Get("Content-Type"); !strings.HasPrefix(ctype, "application/webhook+json") {
		http.Error(w, "unsupported content-type", http.StatusUnsupportedMediaType)
		return false
	}
	return true
}

func (i *LiveKitImpulse) receiveWebhookEvent(
	r *http.Request,
	logger *slog.Logger,
	w http.ResponseWriter,
) (*livekit.WebhookEvent, bool) {
	event, err := webhook.ReceiveWebhookEvent(r, i.keys)
	if err != nil {
		logger.Warn("Webhook event receive failed",
			slog.Any("error", err),
			slog.Bool("hasAuthHeader", r.Header.Get("Authorization") != ""),
			slog.Bool("hasSignatureHeader", r.Header.Get("X-Webhook-Signature") != ""),
		)
		if twirpErr, ok := err.(twirp.Error); ok {
			http.Error(w, twirpErr.Msg(), http.StatusBadRequest)
		} else {
			http.Error(w, "error receiving webhook", http.StatusBadRequest)
		}
		return nil, false
	}
	logger.Info("Received LiveKit webhook",
		slog.String("event", event.GetEvent()),
		slog.String("eventID", event.GetId()),
	)
	return event, true
}

func (i *LiveKitImpulse) handleNonStartingEvent(
	ctx context.Context,
	w http.ResponseWriter,
	logger *slog.Logger,
	event *livekit.WebhookEvent,
	evType, sessionKey string,
) bool {
	participant := event.GetParticipant()
	if i.isEndEvent(evType) {
		if strings.EqualFold(evType, "participant_left") && participant != nil && i.isSelf(participant.GetIdentity()) {
			logger.Info("Ignoring participant_left end event from self identity",
				slog.String("participant", participant.GetIdentity()),
				slog.String("room", sessionKey),
			)
			w.WriteHeader(http.StatusAccepted)
			return true
		}
		i.handleRoomEnded(ctx, sessionKey)
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	if !i.isEventAllowed(evType) {
		logger.Info("Ignoring event not in allowlist", slog.String("event", evType))
		writeTextResponse(w, http.StatusOK, fmt.Sprintf("Ignoring event %s", evType))
		return true
	}
	if participant != nil && i.isSelf(participant.GetIdentity()) {
		logger.Info("Ignoring event from self identity",
			slog.String("event", evType),
			slog.String("participant", participant.GetIdentity()),
		)
		writeTextResponse(w, http.StatusOK, "Ignoring self participant")
		return true
	}
	if !i.isStartEvent(evType) {
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	if sessionKey == "" {
		logger.Warn("Start event missing room identifier", slog.String("event", evType))
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	if i.dispatcher != nil && i.dispatcher.HasSession(sessionKey) {
		logger.Info("Story already active for room; ignoring start event",
			slog.String("room", sessionKey),
			slog.String("event", evType),
		)
		w.WriteHeader(http.StatusAccepted)
		return true
	}
	return false
}

func (i *LiveKitImpulse) buildEventInputs(
	event *livekit.WebhookEvent,
) (map[string]any, map[string]any, string, string) {
	room := event.GetRoom()
	participant := event.GetParticipant()
	eventPayload := map[string]any{
		"type":      strings.TrimSpace(event.GetEvent()),
		"id":        event.GetId(),
		"createdAt": event.GetCreatedAt(),
	}
	if i.cfg.IncludeRawEvent {
		if rawEvent, err := protojson.Marshal(event); err == nil {
			var decoded map[string]any
			if err := json.Unmarshal(rawEvent, &decoded); err == nil {
				eventPayload["raw"] = decoded
			}
		}
	} else {
		addEventContext(eventPayload, room, participant)
	}
	inputs := map[string]any{"event": eventPayload}
	roomName, participantIdentity := addStoryInputs(inputs, room, participant)
	return inputs, eventPayload, roomName, participantIdentity
}

func addEventContext(
	eventPayload map[string]any,
	room *livekit.Room,
	participant *livekit.ParticipantInfo,
) {
	if room != nil {
		eventPayload["room"] = map[string]any{"name": room.GetName(), "sid": room.GetSid()}
	}
	if participant != nil {
		eventPayload["participant"] = map[string]any{
			"identity": participant.GetIdentity(),
			"sid":      participant.GetSid(),
		}
	}
}

func addStoryInputs(
	inputs map[string]any,
	room *livekit.Room,
	participant *livekit.ParticipantInfo,
) (string, string) {
	roomName := ""
	if room != nil {
		roomName = room.GetName()
		inputs["room"] = map[string]any{"name": roomName, "sid": room.GetSid()}
	}
	participantIdentity := ""
	if participant != nil {
		participantIdentity = participant.GetIdentity()
		inputs["participant"] = map[string]any{
			"identity": participantIdentity,
			"sid":      participant.GetSid(),
		}
	}
	return roomName, participantIdentity
}

func (i *LiveKitImpulse) dispatchMatchingStories(
	ctx context.Context,
	logger *slog.Logger,
	evType, sessionKey, roomName, participantIdentity string,
	inputs map[string]any,
	matchingPolicies []cfgpkg.Policy,
) error {
	if len(matchingPolicies) == 0 {
		logger.Info("Starting default story for event",
			slog.String("event", evType),
			slog.String("room", roomName),
		)
		if err := i.dispatchStory(ctx, logger, sessionKey, "", inputs, nil); err != nil {
			logger.Error("Failed to trigger default story",
				slog.String("event", evType),
				slog.String("room", roomName),
				slog.Any("error", err),
			)
			return fmt.Errorf("failed to trigger story")
		}
		return nil
	}
	for _, policy := range matchingPolicies {
		storyName := policy.StoryName
		logger.Info("Starting story for policy match",
			slog.String("story", storyName),
			slog.String("policy", policy.Name),
			slog.String("event", evType),
			slog.String("room", roomName),
			slog.String("participant", participantIdentity),
		)
		if err := i.dispatchStory(ctx, logger, sessionKey, storyName, inputs, &policy); err != nil {
			logger.Error("Failed to trigger story for policy",
				slog.String("story", storyName),
				slog.String("policy", policy.Name),
				slog.Any("error", err),
			)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to trigger one or more stories")
}

func writeTextResponse(w http.ResponseWriter, status int, body string) {
	w.WriteHeader(status)
	_, _ = fmt.Fprint(w, body)
}

func (i *LiveKitImpulse) dispatchStory(
	ctx context.Context,
	logger *slog.Logger,
	sessionKey, storyName string,
	baseInputs map[string]any,
	policy *cfgpkg.Policy,
) error {
	if logger == nil {
		logger = i.loggerWithContext(ctx)
	}
	payload := maps.Clone(baseInputs)
	if policy != nil {
		policyBlock := map[string]any{
			"name": policy.Name,
		}
		if policy.Description != "" {
			policyBlock["description"] = policy.Description
		}
		if len(policy.Metadata) > 0 {
			policyBlock["metadata"] = policy.Metadata
		}
		if policy.StoryInputs != nil {
			policyInputs := maps.Clone(policy.StoryInputs)
			policyBlock["inputs"] = policyInputs
			payload["policyInputs"] = policyInputs
		}
		payload["policy"] = policyBlock
	}
	if i.dispatcher == nil {
		i.dispatcher = sdk.NewStoryDispatcher()
	}

	req := sdk.StoryTriggerRequest{
		Key:       sessionKey,
		StoryName: storyName,
		Inputs:    payload,
	}
	if policy != nil && policy.Name != "" {
		req.Metadata = map[string]string{"policy": policy.Name}
	}

	result, err := i.dispatcher.Trigger(ctx, req)
	if err != nil {
		if errors.Is(err, sdk.ErrImpulseSessionExists) {
			logger.Info("Story already active; skipping duplicate start", slog.String("sessionKey", sessionKey))
			return nil
		}
		return err
	}
	if result.Session != nil {
		logger.Info("Started story for LiveKit session",
			slog.String("story", storyName),
			slog.String("sessionKey", sessionKey),
			slog.String("storyRunNamespace", result.Session.Namespace),
			slog.String("storyRun", result.Session.StoryRun),
		)
	}
	return err
}

func (i *LiveKitImpulse) matchPolicies(eventType, roomName, participantIdentity string) []cfgpkg.Policy {
	if len(i.cfg.Policies) == 0 {
		return nil
	}
	matches := make([]cfgpkg.Policy, 0, len(i.cfg.Policies))
	for _, policy := range i.cfg.Policies {
		if policyMatches(&policy, eventType, roomName, participantIdentity) {
			matches = append(matches, policy)
		}
	}
	return matches
}

func policyMatches(policy *cfgpkg.Policy, eventType, roomName, participant string) bool {
	if policy == nil {
		return false
	}
	return matchDimension(policy.Events, eventType) &&
		matchDimension(policy.Rooms, roomName) &&
		matchDimension(policy.Participants, participant)
}

func matchDimension(filters []string, candidate string) bool {
	if len(filters) == 0 {
		return true
	}
	candidate = strings.ToLower(candidate)
	for _, raw := range filters {
		filter := strings.ToLower(strings.TrimSpace(raw))
		switch {
		case filter == "":
			continue
		case filter == "*":
			return true
		case strings.HasSuffix(filter, "*"):
			prefix := strings.TrimSuffix(filter, "*")
			if strings.HasPrefix(candidate, prefix) {
				return true
			}
		default:
			if candidate == filter {
				return true
			}
		}
	}
	return false
}

func attrsToArgs(attrs []slog.Attr) []any {
	if len(attrs) == 0 {
		return nil
	}
	args := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		args = append(args, attr)
	}
	return args
}

func collectPolicyEvents(policies []cfgpkg.Policy) []string {
	set := make(map[string]struct{})
	for _, policy := range policies {
		for _, ev := range policy.Events {
			val := strings.TrimSpace(ev)
			if val == "" || val == "*" {
				return nil
			}
			set[val] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	events := make([]string, 0, len(set))
	for ev := range set {
		events = append(events, ev)
	}
	sort.Strings(events)
	return events
}

// isEventAllowed checks if an event name is in the configured allowlist.
func (i *LiveKitImpulse) isEventAllowed(ev string) bool {
	if len(i.cfg.EventsAllowlist) == 0 {
		return true
	}
	event := strings.ToLower(ev)
	for _, raw := range i.cfg.EventsAllowlist {
		allowed := strings.ToLower(strings.TrimSpace(raw))
		if allowed == "" {
			continue
		}
		if allowed == "*" || allowed == event {
			return true
		}
	}
	return false
}

// isSelf filters out events emitted by the agent itself to avoid self-trigger loops.
// Supports wildcard patterns (e.g., "bubu-agent*") for flexible matching.
func (i *LiveKitImpulse) isSelf(identity string) bool {
	if identity == "" {
		return false
	}
	return matchDimension(i.cfg.SelfIdentities, identity)
}
