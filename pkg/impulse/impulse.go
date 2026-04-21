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
	"os"
	"sort"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	sdk "github.com/bubustack/bubu-sdk-go"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	sdkk8s "github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/core/contracts"
	"github.com/livekit/protocol/auth"
	livekit "github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"
	"github.com/twitchtv/twirp"
	"google.golang.org/protobuf/encoding/protojson"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

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

type storyRunInputs struct {
	Room *storyRunRoom `json:"room,omitempty"`
}

type storyRunRoom struct {
	Name string `json:"name,omitempty"`
	Sid  string `json:"sid,omitempty"`
}

func uniqueNonEmpty(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, raw := range values {
		if value := strings.TrimSpace(raw); value != "" {
			return value
		}
	}
	return ""
}

func sessionKeysFromEvent(event *livekit.WebhookEvent) []string {
	if event == nil {
		return nil
	}
	if room := event.GetRoom(); room != nil {
		return uniqueNonEmpty(room.GetSid(), room.GetName(), event.GetId())
	}
	return uniqueNonEmpty(event.GetId())
}

func sessionKeyFromEvent(event *livekit.WebhookEvent) string {
	keys := sessionKeysFromEvent(event)
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func triggerTokenFromEvent(event *livekit.WebhookEvent) string {
	if event == nil {
		return ""
	}
	if room := event.GetRoom(); room != nil {
		// Use room SID when available so each room lifecycle gets a stable
		// cluster-side dedupe key even if the room name is reused later.
		return firstNonEmpty(room.GetSid(), room.GetName(), event.GetId())
	}
	return strings.TrimSpace(event.GetId())
}

func storyRunSessionKeys(run *runsv1alpha1.StoryRun) []string {
	if run == nil {
		return nil
	}
	keys := make([]string, 0, 3)
	if token := strings.TrimSpace(run.GetAnnotations()[runsidentity.StoryRunTriggerTokenAnnotation]); token != "" {
		keys = append(keys, token)
	}
	if run.Spec.Inputs == nil || len(run.Spec.Inputs.Raw) == 0 {
		return uniqueNonEmpty(keys...)
	}
	var inputs storyRunInputs
	if err := json.Unmarshal(run.Spec.Inputs.Raw, &inputs); err != nil || inputs.Room == nil {
		return uniqueNonEmpty(keys...)
	}
	keys = append(keys, inputs.Room.Name, inputs.Room.Sid)
	return uniqueNonEmpty(keys...)
}

func resolveImpulseIdentity() (string, string) {
	return strings.TrimSpace(os.Getenv(contracts.ImpulseNameEnv)),
		firstNonEmpty(
			os.Getenv(contracts.ImpulseNamespaceEnv),
			os.Getenv(contracts.PodNamespaceEnv),
		)
}

func matchesAnyKey(candidateKeys []string, keySet map[string]struct{}) bool {
	for _, key := range candidateKeys {
		if _, ok := keySet[strings.TrimSpace(key)]; ok {
			return true
		}
	}
	return false
}

func normalizeSessionKeySet(sessionKeys []string) map[string]struct{} {
	keySet := make(map[string]struct{}, len(sessionKeys))
	for _, key := range sessionKeys {
		if key = strings.TrimSpace(key); key != "" {
			keySet[key] = struct{}{}
		}
	}
	if len(keySet) == 0 {
		return nil
	}
	return keySet
}

func storyRunMatchesImpulse(run *runsv1alpha1.StoryRun, impulseName, namespace string) bool {
	if run == nil || run.Status.Phase.IsTerminal() {
		return false
	}
	if run.Spec.ImpulseRef == nil || strings.TrimSpace(run.Spec.ImpulseRef.Name) != impulseName {
		return false
	}
	if run.Spec.ImpulseRef.Namespace == nil {
		return true
	}
	impulseNamespace := strings.TrimSpace(*run.Spec.ImpulseRef.Namespace)
	return impulseNamespace == "" || impulseNamespace == namespace
}

func resolveEndedSession(event *livekit.WebhookEvent, fallbackKey string) (string, []string) {
	sessionKeys := sessionKeysFromEvent(event)
	if len(sessionKeys) == 0 && fallbackKey != "" {
		sessionKeys = uniqueNonEmpty(fallbackKey)
	}
	if fallbackKey == "" {
		fallbackKey = firstNonEmpty(sessionKeys...)
	}
	return fallbackKey, sessionKeys
}

func (i *LiveKitImpulse) findActiveStoryRuns(
	ctx context.Context,
	k8sClient *sdkk8s.Client,
	impulseName, namespace string,
	sessionKeys []string,
) ([]runsv1alpha1.StoryRun, error) {
	if k8sClient == nil {
		return nil, fmt.Errorf("k8s client is required")
	}
	if impulseName == "" {
		return nil, fmt.Errorf("impulse name is required")
	}
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if len(sessionKeys) == 0 {
		return nil, nil
	}

	keySet := normalizeSessionKeySet(sessionKeys)
	if keySet == nil {
		return nil, nil
	}

	var runs runsv1alpha1.StoryRunList
	if err := k8sClient.List(ctx, &runs, ctrlclient.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list storyruns: %w", err)
	}

	matches := make([]runsv1alpha1.StoryRun, 0, len(runs.Items))
	for idx := range runs.Items {
		run := &runs.Items[idx]
		if !storyRunMatchesImpulse(run, impulseName, namespace) {
			continue
		}
		if matchesAnyKey(storyRunSessionKeys(run), keySet) {
			matches = append(matches, *run)
		}
	}
	return matches, nil
}

func (i *LiveKitImpulse) forgetSessionAliases(keys ...string) {
	if i.dispatcher == nil {
		return
	}
	for _, key := range uniqueNonEmpty(keys...) {
		i.dispatcher.Forget(key)
	}
}

func logStoppedSession(logger *slog.Logger, sessionKey string, session *sdk.StorySession) {
	if session != nil {
		logger.Info("Ended story for room",
			slog.String("story", session.StoryName),
			slog.String("room", sessionKey),
			slog.String("storyRunNamespace", session.Namespace),
			slog.String("storyRun", session.StoryRun),
		)
		return
	}
	logger.Info("Ended story for room", slog.String("room", sessionKey))
}

func logFailedSessionStop(logger *slog.Logger, sessionKey string, session *sdk.StorySession, err error) {
	if session != nil {
		logger.Error("Failed to stop StoryRun",
			slog.String("namespace", session.Namespace),
			slog.String("storyRun", session.StoryRun),
			slog.String("room", sessionKey),
			slog.Any("error", err),
		)
		return
	}
	logger.Error("Failed to stop StoryRun for room",
		slog.String("room", sessionKey),
		slog.Any("error", err),
	)
}

func (i *LiveKitImpulse) tryStopRoomSessionViaDispatcher(
	ctx context.Context,
	logger *slog.Logger,
	sessionKey string,
) bool {
	if i.dispatcher == nil {
		return false
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	session, err := i.dispatcher.Stop(shutdownCtx, sessionKey)
	switch {
	case err == nil:
		logStoppedSession(logger, sessionKey, session)
		return true
	case errors.Is(err, sdk.ErrStoryRunNotFound):
		logger.Info("StoryRun already gone; clearing session", slog.String("room", sessionKey))
		return true
	case errors.Is(err, sdk.ErrImpulseSessionNotFound):
		return false
	default:
		logFailedSessionStop(logger, sessionKey, session, err)
		return true
	}
}

func (i *LiveKitImpulse) stopRoomSessionViaKubernetesLookup(
	ctx context.Context,
	logger *slog.Logger,
	k8sClient *sdkk8s.Client,
	sessionKey string,
	sessionKeys []string,
) {
	impulseName, impulseNamespace := resolveImpulseIdentity()
	if impulseName == "" || impulseNamespace == "" {
		logger.Warn("Cannot resolve impulse identity; skipping Kubernetes fallback stop",
			slog.String("room", sessionKey),
			slog.String("impulseName", impulseName),
			slog.String("impulseNamespace", impulseNamespace),
		)
		return
	}
	if k8sClient == nil {
		logger.Warn("Kubernetes client unavailable; skipping fallback stop",
			slog.String("room", sessionKey),
			slog.String("impulse", impulseName),
		)
		return
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	matches, err := i.findActiveStoryRuns(lookupCtx, k8sClient, impulseName, impulseNamespace, sessionKeys)
	if err != nil {
		logger.Error("Failed to locate StoryRun for LiveKit end event",
			slog.String("room", sessionKey),
			slog.String("impulse", impulseName),
			slog.Any("error", err),
		)
		return
	}
	if len(matches) == 0 {
		logger.Info("No active StoryRun recorded; skipping shutdown", slog.String("room", sessionKey))
		return
	}

	for _, run := range matches {
		if err := k8sClient.StopStoryRun(lookupCtx, run.Name, run.Namespace); err != nil {
			logger.Error("Failed to stop StoryRun from Kubernetes fallback lookup",
				slog.String("room", sessionKey),
				slog.String("storyRunNamespace", run.Namespace),
				slog.String("storyRun", run.Name),
				slog.Any("error", err),
			)
			continue
		}
		i.forgetSessionAliases(append(sessionKeys, storyRunSessionKeys(&run)...)...)
		logger.Info("Ended story for room via Kubernetes fallback",
			slog.String("room", sessionKey),
			slog.String("storyRunNamespace", run.Namespace),
			slog.String("storyRun", run.Name),
		)
	}
}

func (i *LiveKitImpulse) handleRoomEnded(
	ctx context.Context,
	k8sClient *sdkk8s.Client,
	sessionKey string,
	event *livekit.WebhookEvent,
) {
	logger := i.loggerWithContext(ctx)
	sessionKey, sessionKeys := resolveEndedSession(event, sessionKey)
	if sessionKey == "" {
		logger.Warn("Room end event missing room identifier; skipping StoryRun shutdown")
		return
	}
	if i.tryStopRoomSessionViaDispatcher(ctx, logger, sessionKey) {
		return
	}
	i.stopRoomSessionViaKubernetesLookup(ctx, logger, k8sClient, sessionKey, sessionKeys)
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
func (i *LiveKitImpulse) webhookHandler(k8sClient *sdkk8s.Client) http.HandlerFunc {
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
		if i.handleNonStartingEvent(r.Context(), k8sClient, w, logger, event, evType, sessionKey) {
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
			triggerTokenFromEvent(event),
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
	k8sClient *sdkk8s.Client,
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
		i.handleRoomEnded(ctx, k8sClient, sessionKey, event)
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
	evType, sessionKey, triggerToken, roomName, participantIdentity string,
	inputs map[string]any,
	matchingPolicies []cfgpkg.Policy,
) error {
	if len(matchingPolicies) == 0 {
		logger.Info("Starting default story for event",
			slog.String("event", evType),
			slog.String("room", roomName),
		)
		if err := i.dispatchStory(ctx, logger, sessionKey, triggerToken, "", inputs, nil); err != nil {
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
		if err := i.dispatchStory(ctx, logger, sessionKey, triggerToken, storyName, inputs, &policy); err != nil {
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
	sessionKey, triggerToken, storyName string,
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
		Key:          sessionKey,
		TriggerToken: triggerToken,
		StoryName:    storyName,
		Inputs:       payload,
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
