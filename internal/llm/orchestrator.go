package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
)

const SubmitResultTool = "submit_result"

// AgentPolicy defines limits for an agent step execution.
type AgentPolicy struct {
	MaxLLMCalls int
	MaxCostUSD  float64
}

// AllowedCallback describes a callback the LLM is permitted to invoke.
type AllowedCallback struct {
	Name string
	// Description is shown to the LLM as the tool description.
	Description string
	// Mode is informational (read / write / write_draft), appended to description.
	Mode string
	// InputSchema is a JSON Schema properties map describing the tool's expected input.
	// Passed directly to the LLM as the tool's input_schema properties.
	// If nil, the tool accepts an open-ended object.
	InputSchema map[string]any
}

// AgentStepConfig carries everything needed to run an agent step.
type AgentStepConfig struct {
	Objective        string
	AllowedCallbacks []AllowedCallback
	Policy           AgentPolicy
	// SystemPrompt is the system instruction given to the LLM. Required.
	SystemPrompt string
	// OutputSchema is a JSON Schema properties map describing the expected output structure.
	// The LLM is given a submit_result tool with this schema and will call it naturally
	// when done. If a policy limit is hit before it calls submit_result, one final forced
	// call is made. When nil, submit_result accepts an open-ended object.
	OutputSchema map[string]any
	// LLMContext is the accumulated context passed by the worker SDK.
	// Each entry has a "metadata" (string description) and "payload" (data) field.
	LLMContext []map[string]any
}

// StepResult is returned by Orchestrator.Run when the agent step completes.
type StepResult struct {
	LLMCallsUsed int
	CostUSD      float64
}

// CallbackHandler is called by the orchestrator when the LLM invokes a callback
// (including submit_result). It executes the callback and returns its output as a
// JSON-serialisable map. The caller can identify submit_result by checking
// callbackName == SubmitResultTool.
type CallbackHandler func(ctx context.Context, callbackName string, input map[string]any) (map[string]any, error)

// CallbackExchange is sent by the CallbackHandler to the caller over a channel.
// The caller dispatches the callback, then sends the result back on ResultCh.
type CallbackExchange struct {
	Name     string
	Input    map[string]any
	ResultCh chan CallbackExchangeResult
}

// CallbackExchangeResult is the caller's response to a CallbackExchange.
type CallbackExchangeResult struct {
	Output map[string]any
	Error  error
}

// NewCallbackHandler returns a CallbackHandler driven by a channel. For each
// callback the orchestrator invokes, a CallbackExchange is sent on ch. The
// caller reads from ch, executes the callback, and sends the result back on
// CallbackExchange.ResultCh.
func NewCallbackHandler(ch chan<- CallbackExchange) CallbackHandler {
	return func(ctx context.Context, name string, input map[string]any) (map[string]any, error) {
		resultCh := make(chan CallbackExchangeResult, 1)
		select {
		case ch <- CallbackExchange{Name: name, Input: input, ResultCh: resultCh}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case r := <-resultCh:
			return r.Output, r.Error
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Orchestrator runs the agentic step loop against the Claude API.
type Orchestrator struct {
	client anthropic.Client
	model  string
	log    *slog.Logger
}

func NewOrchestrator(client anthropic.Client, model string, log *slog.Logger) *Orchestrator {
	return &Orchestrator{
		client: client,
		model:  model,
		log:    log.With("component", "llm.orchestrator"),
	}
}

// Run executes an agent step to completion. The model may call allowed callbacks freely
// and calls submit_result when done — both are delivered via onCallback. If a policy
// limit is hit before submit_result is called, one final forced call is made.
func (o *Orchestrator) Run(ctx context.Context, cfg AgentStepConfig, onCallback CallbackHandler) (*StepResult, error) {
	tools := buildTools(cfg.AllowedCallbacks, cfg.OutputSchema)
	messages := []anthropic.MessageParam{
		anthropic.NewUserMessage(anthropic.NewTextBlock(buildUserContent(cfg.Objective, cfg.LLMContext))),
	}

	result := &StepResult{}

	for {
		policyLimitReached := cfg.Policy.MaxLLMCalls > 0 && result.LLMCallsUsed >= cfg.Policy.MaxLLMCalls

		if policyLimitReached {
			o.log.Warn("policy limit reached, forcing submit_result", "llm_calls", result.LLMCallsUsed)
		}

		o.log.Debug("calling LLM", "llm_calls_so_far", result.LLMCallsUsed, "forced_submit", policyLimitReached)

		params := anthropic.MessageNewParams{
			Model:     anthropic.Model(o.model),
			MaxTokens: 4096,
			System:    []anthropic.TextBlockParam{{Text: cfg.SystemPrompt + "\n\nWhen you have completed your objective, call the submit_result tool with your findings."}},
			Messages:  messages,
			Tools:     tools,
		}
		if policyLimitReached {
			params.ToolChoice = anthropic.ToolChoiceParamOfTool(SubmitResultTool)
		}

		resp, err := o.client.Messages.New(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("LLM API call failed: %w", err)
		}

		result.LLMCallsUsed++
		result.CostUSD += estimateCost(resp.Usage)

		if cfg.Policy.MaxCostUSD > 0 && result.CostUSD > cfg.Policy.MaxCostUSD && !policyLimitReached {
			o.log.Warn("cost limit reached, will force submit_result on next call",
				"cost_usd", result.CostUSD, "limit", cfg.Policy.MaxCostUSD)
		}

		o.log.Debug("LLM response", "stop_reason", resp.StopReason, "content_blocks", len(resp.Content))

		messages = append(messages, resp.ToParam())

		// Model finished without calling submit_result — force it next iteration.
		if resp.StopReason == anthropic.StopReasonEndTurn {
			o.log.Warn("model reached end_turn without calling submit_result, forcing")
			messages = append(messages, anthropic.NewUserMessage(
				anthropic.NewTextBlock("Please call submit_result with your findings."),
			))
			cfg.Policy.MaxLLMCalls = result.LLMCallsUsed + 1
			continue
		}

		if resp.StopReason != anthropic.StopReasonToolUse {
			return nil, fmt.Errorf("unexpected stop reason: %s", resp.StopReason)
		}

		// Dispatch all tool calls — submit_result and callbacks go through onCallback uniformly.
		var toolResults []anthropic.ContentBlockParamUnion
		for _, block := range resp.Content {
			if block.Type != "tool_use" {
				continue
			}

			o.log.Info("dispatching callback", "callback", block.Name)

			var input map[string]any
			if err := json.Unmarshal(block.Input, &input); err != nil {
				return nil, fmt.Errorf("failed to parse tool input for %s: %w", block.Name, err)
			}

			cbOutput, err := onCallback(ctx, block.Name, input)
			if block.Name == SubmitResultTool {
				if err != nil {
					return nil, fmt.Errorf("submit_result callback failed: %w", err)
				}
				o.log.Info("agent step complete via submit_result",
					"llm_calls", result.LLMCallsUsed, "cost_usd", result.CostUSD)
				return result, nil
			}
			if err != nil {
				o.log.Warn("callback returned error, reporting to LLM", "callback", block.Name, "error", err)
				toolResults = append(toolResults, anthropic.NewToolResultBlock(block.ID, fmt.Sprintf("error: %s", err), true))
				continue
			}

			outputJSON, err := json.Marshal(cbOutput)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal callback output for %s: %w", block.Name, err)
			}

			o.log.Info("callback completed", "callback", block.Name)
			toolResults = append(toolResults, anthropic.NewToolResultBlock(block.ID, string(outputJSON), false))
		}

		if len(toolResults) > 0 {
			messages = append(messages, anthropic.NewUserMessage(toolResults...))
		}

		// After dispatching callbacks, check cost limit for next iteration.
		if cfg.Policy.MaxCostUSD > 0 && result.CostUSD > cfg.Policy.MaxCostUSD {
			cfg.Policy.MaxLLMCalls = result.LLMCallsUsed
		}
	}
}

// buildTools constructs the full tool list: all allowed callbacks + the submit_result tool.
func buildTools(callbacks []AllowedCallback, outputSchema map[string]any) []anthropic.ToolUnionParam {
	tools := make([]anthropic.ToolUnionParam, 0, len(callbacks)+1)

	for _, cb := range callbacks {
		inputSchema := cb.InputSchema
		if inputSchema == nil {
			inputSchema = map[string]any{"type": "object"}
		}
		desc := cb.Description
		if desc == "" {
			desc = cb.Name
		}
		if cb.Mode != "" {
			desc = fmt.Sprintf("%s [%s]", desc, cb.Mode)
		}
		tool := anthropic.ToolParam{
			Name:        cb.Name,
			Description: anthropic.String(desc),
			InputSchema: anthropic.ToolInputSchemaParam{Properties: inputSchema},
		}
		tools = append(tools, anthropic.ToolUnionParam{OfTool: &tool})
	}

	// submit_result is always the last tool; its schema is customer-defined.
	submitSchema := outputSchema
	if submitSchema == nil {
		submitSchema = map[string]any{"type": "object"}
	}
	submitTool := anthropic.ToolParam{
		Name:        SubmitResultTool,
		Description: anthropic.String("Call this when you have completed your objective to submit your final result."),
		InputSchema: anthropic.ToolInputSchemaParam{Properties: submitSchema},
	}
	tools = append(tools, anthropic.ToolUnionParam{OfTool: &submitTool})

	return tools
}

func buildUserContent(objective string, llmContext []map[string]any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Objective: %s\n", objective)
	if len(llmContext) > 0 {
		b.WriteString("\nContext:\n")
		for _, entry := range llmContext {
			metadata, _ := entry["metadata"].(string)
			payload := entry["payload"]
			payloadJSON, _ := json.Marshal(payload)
			fmt.Fprintf(&b, "- %s: %s\n", metadata, string(payloadJSON))
		}
	}
	return b.String()
}

func estimateCost(usage anthropic.Usage) float64 {
	// Rough estimate based on claude-sonnet-4-6 pricing.
	const inputCostPer1M = 3.0
	const outputCostPer1M = 15.0
	return (float64(usage.InputTokens)/1_000_000)*inputCostPer1M +
		(float64(usage.OutputTokens)/1_000_000)*outputCostPer1M
}
