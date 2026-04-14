package config

// Config holds LiveKit webhook impulse configuration.
// The target story is resolved from the Impulse's spec.storyRef, not from config.
type Config struct {
	EventsAllowlist []string `json:"eventsAllowlist" mapstructure:"eventsAllowlist"`
	SelfIdentities  []string `json:"selfIdentities" mapstructure:"selfIdentities"`
	Path            string   `json:"path" mapstructure:"path"`
	Policies        []Policy `json:"policies" mapstructure:"policies"`
	IncludeRawEvent bool     `json:"includeRawEvent" mapstructure:"includeRawEvent"`
	StartEvents     []string `json:"startEvents" mapstructure:"startEvents"`
	EndEvents       []string `json:"endEvents" mapstructure:"endEvents"`
}

// Policy describes how the impulse should react to a matching webhook event.
type Policy struct {
	Name         string            `json:"name" mapstructure:"name"`
	Description  string            `json:"description,omitempty" mapstructure:"description"`
	StoryName    string            `json:"storyName" mapstructure:"storyName"`
	Events       []string          `json:"events,omitempty" mapstructure:"events"`
	Rooms        []string          `json:"rooms,omitempty" mapstructure:"rooms"`
	Participants []string          `json:"participants,omitempty" mapstructure:"participants"`
	StoryInputs  map[string]any    `json:"storyInputs,omitempty" mapstructure:"storyInputs"`
	Metadata     map[string]string `json:"metadata,omitempty" mapstructure:"metadata"`
}
