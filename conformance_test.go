package main

import (
	"os"
	"strings"
	"testing"
)

func TestTemplateConformance(t *testing.T) {
	data, err := os.ReadFile("Impulse.yaml")
	if err != nil {
		t.Fatalf("read template: %v", err)
	}
	content := string(data)
	required := []string{
		"kind: ImpulseTemplate",
		"metadata:",
		"spec:",
		"supportedModes:",
		"registry.bubustack.io/maturity",
	}
	for _, needle := range required {
		if !strings.Contains(content, needle) {
			t.Fatalf("template missing %q", needle)
		}
	}
}
