package handlers_test

import (
	"testing"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/server/handlers"
)

func rule(window any) map[string]any {
	return map[string]any{
		"metric":    "cost_usd",
		"op":        "greater_than",
		"threshold": 1.0,
		"window":    window,
		"action":    map[string]any{"field": "model", "value": "claude-haiku-4-5"},
	}
}

func TestValidateAgentLifecycleRules_NoRulesKey(t *testing.T) {
	if err := handlers.ValidateAgentLifecycleRules(map[string]any{}); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAgentLifecycleRules_ValidRule(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(5.0)}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAgentLifecycleRules_AtMax(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(float64(domain.MaxLifecycleWindow))}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAgentLifecycleRules_OverMax(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(float64(domain.MaxLifecycleWindow + 1))}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_ZeroWindow(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(0.0)}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_NegativeWindow(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(-1.0)}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_MissingWindow(t *testing.T) {
	r := rule(5.0)
	delete(r, "window")
	policy := map[string]any{"rules": []any{r}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_WindowWrongType(t *testing.T) {
	policy := map[string]any{"rules": []any{rule("five")}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_RuleNotAnObject(t *testing.T) {
	policy := map[string]any{"rules": []any{"not-a-rule"}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_RulesNotAnArray(t *testing.T) {
	policy := map[string]any{"rules": "not-an-array"}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}
