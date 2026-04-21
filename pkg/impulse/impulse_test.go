package impulse

import (
	"context"
	"encoding/json"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	sdk "github.com/bubustack/bubu-sdk-go"
	sdkengram "github.com/bubustack/bubu-sdk-go/engram"
	sdkk8s "github.com/bubustack/bubu-sdk-go/k8s"
	"github.com/bubustack/core/contracts"
	livekit "github.com/livekit/protocol/livekit"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cfgpkg "github.com/bubustack/livekit-webhook-impulse/pkg/config"
)

const testStoryRunNamespace = "livekit-voice"

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

func TestSessionKeyFromEventAndTriggerTokenPreferSID(t *testing.T) {
	t.Parallel()

	event := &livekit.WebhookEvent{
		Id: "EV_123",
		Room: &livekit.Room{
			Name: "support-demo",
			Sid:  "RM_123",
		},
	}

	if got := sessionKeyFromEvent(event); got != "RM_123" {
		t.Fatalf("sessionKeyFromEvent() = %q, want %q", got, "RM_123")
	}
	if got := triggerTokenFromEvent(event); got != "RM_123" {
		t.Fatalf("triggerTokenFromEvent() = %q, want %q", got, "RM_123")
	}
}

func TestFindActiveStoryRunsMatchesImpulseAndRoomIdentifiers(t *testing.T) {
	t.Parallel()

	client := newTestK8sClient(t,
		newStoryRun(
			t,
			"active-token-match",
			"livekit-webhook",
			enums.PhaseRunning,
			map[string]string{runsidentity.StoryRunTriggerTokenAnnotation: "RM_123"},
			map[string]any{"room": map[string]any{"name": "support-demo", "sid": "RM_123"}},
		),
		newStoryRun(
			t,
			"active-room-match",
			"livekit-webhook",
			enums.PhaseRunning,
			nil,
			map[string]any{"room": map[string]any{"name": "support-demo", "sid": "RM_999"}},
		),
		newStoryRun(
			t,
			"terminal-ignored",
			"livekit-webhook",
			enums.PhaseSucceeded,
			map[string]string{runsidentity.StoryRunTriggerTokenAnnotation: "RM_123"},
			map[string]any{"room": map[string]any{"name": "support-demo", "sid": "RM_123"}},
		),
		newStoryRun(
			t,
			"other-impulse-ignored",
			"another-impulse",
			enums.PhaseRunning,
			map[string]string{runsidentity.StoryRunTriggerTokenAnnotation: "RM_123"},
			map[string]any{"room": map[string]any{"name": "support-demo", "sid": "RM_123"}},
		),
	)

	imp := New()
	matches, err := imp.findActiveStoryRuns(
		context.Background(),
		client,
		"livekit-webhook",
		testStoryRunNamespace,
		[]string{"support-demo", "RM_123"},
	)
	if err != nil {
		t.Fatalf("findActiveStoryRuns() error = %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("findActiveStoryRuns() matched %d runs, want 2", len(matches))
	}
}

func TestHandleRoomEndedFallsBackToKubernetesLookupAndClearsAliases(t *testing.T) {
	const (
		namespace = testStoryRunNamespace
		impulse   = "livekit-webhook"
		storyRun  = "voice-run"
		roomSID   = "RM_123"
		roomName  = "support-demo"
		storyName = "livekit-voice-assistant"
	)
	t.Setenv(contracts.ImpulseNameEnv, impulse)
	t.Setenv(contracts.ImpulseNamespaceEnv, namespace)

	run := newStoryRun(
		t,
		storyRun,
		impulse,
		enums.PhaseRunning,
		nil,
		map[string]any{"room": map[string]any{"name": roomName, "sid": roomSID}},
	)
	client := newTestK8sClient(t, run)

	imp := New()
	imp.dispatcher = sdk.NewStoryDispatcher(sdk.WithStoryRuntime(
		func(ctx context.Context, gotStoryName, gotNamespace string, inputs map[string]any) (*runsv1alpha1.StoryRun, error) {
			if gotStoryName != storyName {
				t.Fatalf("start story name = %q, want %q", gotStoryName, storyName)
			}
			if gotNamespace != namespace {
				t.Fatalf("start story namespace = %q, want %q", gotNamespace, namespace)
			}
			return &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{Name: storyRun, Namespace: namespace},
			}, nil
		},
		func(ctx context.Context, storyRunName, storyRunNamespace string) error {
			t.Fatalf("dispatcher stop should not be called with mismatched session key")
			return nil
		},
	))
	if _, err := imp.dispatcher.Trigger(context.Background(), sdk.StoryTriggerRequest{
		Key:            roomSID,
		StoryName:      storyName,
		StoryNamespace: namespace,
	}); err != nil {
		t.Fatalf("dispatcher.Trigger() error = %v", err)
	}
	if !imp.dispatcher.HasSession(roomSID) {
		t.Fatal("expected dispatcher to track the original SID session")
	}

	event := &livekit.WebhookEvent{
		Event: "participant_left",
		Room: &livekit.Room{
			Name: roomName,
		},
	}

	imp.handleRoomEnded(context.Background(), client, sessionKeyFromEvent(event), event)

	updated := &runsv1alpha1.StoryRun{}
	if err := client.Get(
		context.Background(),
		ctrlclient.ObjectKey{Name: storyRun, Namespace: namespace},
		updated,
	); err != nil {
		t.Fatalf("get updated StoryRun: %v", err)
	}
	if updated.Spec.CancelRequested == nil || !*updated.Spec.CancelRequested {
		t.Fatal("expected Kubernetes fallback stop to set spec.cancelRequested=true")
	}
	if imp.dispatcher.HasSession(roomSID) {
		t.Fatal("expected fallback stop to clear the stale dispatcher SID alias")
	}
}

func newTestK8sClient(t *testing.T, objects ...ctrlclient.Object) *sdkk8s.Client {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add StoryRun scheme: %v", err)
	}
	return &sdkk8s.Client{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(),
	}
}

func newStoryRun(
	t *testing.T,
	name, impulseName string,
	phase enums.Phase,
	annotations map[string]string,
	inputs map[string]any,
) *runsv1alpha1.StoryRun {
	t.Helper()

	rawInputs, err := json.Marshal(inputs)
	if err != nil {
		t.Fatalf("marshal StoryRun inputs: %v", err)
	}

	return &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   testStoryRunNamespace,
			Annotations: annotations,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			ImpulseRef: &refs.ImpulseReference{
				ObjectReference: refs.ObjectReference{Name: impulseName},
			},
			Inputs: &runtime.RawExtension{Raw: rawInputs},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: phase,
		},
	}
}
