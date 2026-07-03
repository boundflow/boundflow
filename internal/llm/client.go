package llm

import (
	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// NewClient creates an Anthropic client using the provided API key.
// The API key should come from the BOUNDFLOW_LLM_API_KEY environment variable.
func NewClient(apiKey string) anthropic.Client {
	return anthropic.NewClient(option.WithAPIKey(apiKey))
}
