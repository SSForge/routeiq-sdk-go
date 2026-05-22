package routeiq

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	completionSuccess = "1"
	completionFailure = "2"
	toolSuccess       = "1"
	toolFailure       = "2"
)

var permissionLevel = map[string]string{
	"READ_ONLY":  "1",
	"READ_WRITE": "2",
	"PRIVILEGED": "3",
}

// ── TaskHandle ────────────────────────────────────────────────────────────────

// TaskHandle tracks a single agent task span. Call End() when the task is done.
type TaskHandle struct {
	riq        *RouteIQ
	ctx        context.Context
	span       trace.Span
	taskID     string
	runID      string
	intent     string
	taskType   string
	done       bool
	stepIndex  int
}

func newTaskHandle(ctx context.Context, riq *RouteIQ, intent string) *TaskHandle {
	return &TaskHandle{
		riq:    riq,
		ctx:    ctx,
		taskID: newUUID(),
		runID:  newUUID(),
		intent: intent,
	}
}

func (t *TaskHandle) start() {
	_, t.span = t.riq.tracer.Start(t.ctx, fmt.Sprintf("task:%s", t.taskID))
	attrs := append(t.riq.envelope(t, nil),
		attribute.String("routeiq.event.type", "1"),
		attribute.String("routeiq.task.input_intent", truncate(t.intent, 256)),
	)
	if t.taskType != "" {
		attrs = append(attrs, attribute.String("routeiq.task.type", t.taskType))
	}
	t.span.SetAttributes(attrs...)
}

// Step starts a reasoning step within this task.
func (t *TaskHandle) Step(opts ...StepOption) *StepHandle {
	t.stepIndex++
	s := &StepHandle{task: t, stepID: newUUID(), index: t.stepIndex}
	for _, o := range opts {
		o(s)
	}
	s.start()
	return s
}

// StepOption configures a step.
type StepOption func(*StepHandle)

// WithAction sets routeiq.step.selected_action.
func WithAction(action string) StepOption { return func(s *StepHandle) { s.action = action } }

// WithRationale sets routeiq.step.action_rationale.
func WithRationale(r string) StepOption { return func(s *StepHandle) { s.rationale = r } }

// Complete marks the task as successfully done.
func (t *TaskHandle) Complete(opts ...CompleteOption) {
	c := &completeConfig{}
	for _, o := range opts {
		o(c)
	}
	t.finish(completionSuccess, c)
}

// Fail marks the task as failed.
func (t *TaskHandle) Fail(category ...string) {
	cat := ""
	if len(category) > 0 {
		cat = category[0]
	}
	t.finish(completionFailure, &completeConfig{failureCategory: cat})
}

func (t *TaskHandle) finish(status string, c *completeConfig) {
	if t.done {
		return
	}
	t.done = true
	attrs := []attribute.KeyValue{attribute.String("routeiq.task.completion_status", status)}
	if c.tokens > 0 {
		attrs = append(attrs, attribute.Int("routeiq.task.total_tokens", c.tokens))
	}
	if c.costUSD > 0 {
		attrs = append(attrs, attribute.Float64("routeiq.task.cost_usd", c.costUSD))
	}
	if c.cohort != "" {
		attrs = append(attrs, attribute.String("routeiq.task.cohort", c.cohort))
	}
	if c.failureCategory != "" {
		attrs = append(attrs, attribute.String("routeiq.task.failure_category", c.failureCategory))
	}
	t.span.SetAttributes(attrs...)
}

// End closes the task span. Auto-completes if not already done.
func (t *TaskHandle) End() {
	if !t.done {
		t.Complete()
	}
	t.span.End()
}

// CompleteOption configures task completion.
type CompleteOption func(*completeConfig)

type completeConfig struct {
	tokens          int
	costUSD         float64
	cohort          string
	failureCategory string
}

// WithTokens sets routeiq.task.total_tokens.
func WithTokens(n int) CompleteOption { return func(c *completeConfig) { c.tokens = n } }

// WithCostUSD sets routeiq.task.cost_usd.
func WithCostUSD(usd float64) CompleteOption { return func(c *completeConfig) { c.costUSD = usd } }

// WithCohort sets routeiq.task.cohort.
func WithCohort(cohort string) CompleteOption { return func(c *completeConfig) { c.cohort = cohort } }

// ── StepHandle ────────────────────────────────────────────────────────────────

// StepHandle tracks one reasoning step span.
type StepHandle struct {
	task      *TaskHandle
	span      trace.Span
	stepID    string
	action    string
	rationale string
	index     int
	done      bool
}

func (s *StepHandle) start() {
	_, s.span = s.task.riq.tracer.Start(s.task.ctx, fmt.Sprintf("step:%s", s.stepID))
	attrs := append(s.task.riq.envelope(s.task, s),
		attribute.String("routeiq.event.type", "4"),
		attribute.Int("routeiq.step.index", s.index),
	)
	if s.action != "" {
		attrs = append(attrs, attribute.String("routeiq.step.selected_action", s.action))
	}
	if s.rationale != "" {
		attrs = append(attrs, attribute.String("routeiq.step.action_rationale", s.rationale))
	}
	s.span.SetAttributes(attrs...)
}

// Tool starts a tool invocation within this step.
func (s *StepHandle) Tool(name string, opts ...ToolOption) *ToolHandle {
	th := &ToolHandle{step: s, name: name, permission: "READ_ONLY", start: time.Now()}
	for _, o := range opts {
		o(th)
	}
	th.begin()
	return th
}

// ToolOption configures a tool call.
type ToolOption func(*ToolHandle)

// WithArgs sets the arguments for hashing.
func WithArgs(args map[string]any) ToolOption {
	return func(t *ToolHandle) { t.args = args }
}

// WithPermission sets routeiq.tool.permission_level.
func WithPermission(p string) ToolOption { return func(t *ToolHandle) { t.permission = p } }

// Complete marks the step as successfully completed.
func (s *StepHandle) Complete() { s.finish(completionSuccess, "") }

// Fail marks the step as failed.
func (s *StepHandle) Fail(category ...string) {
	cat := ""
	if len(category) > 0 {
		cat = category[0]
	}
	s.finish(completionFailure, cat)
}

func (s *StepHandle) finish(status, category string) {
	if s.done {
		return
	}
	s.done = true
	attrs := []attribute.KeyValue{attribute.String("routeiq.step.completion_status", status)}
	if category != "" {
		attrs = append(attrs, attribute.String("routeiq.step.failure_category", category))
	}
	s.span.SetAttributes(attrs...)
}

// End closes the step span. Auto-completes if not already done.
func (s *StepHandle) End() {
	if !s.done {
		s.Complete()
	}
	s.span.End()
}

// ── ToolHandle ────────────────────────────────────────────────────────────────

// ToolHandle tracks a single tool invocation span.
type ToolHandle struct {
	step       *StepHandle
	span       trace.Span
	name       string
	args       map[string]any
	permission string
	start      time.Time
	done       bool
}

func (t *ToolHandle) begin() {
	_, t.span = t.step.task.riq.tracer.Start(t.step.task.ctx, fmt.Sprintf("tool:%s", t.name))

	argsHash := argsHash(t.args)
	perm := permissionLevel[t.permission]
	if perm == "" {
		perm = "1"
	}
	attrs := append(t.step.task.riq.envelope(t.step.task, t.step),
		attribute.String("routeiq.event.type", "7"),
		attribute.String("routeiq.tool.name", t.name),
		attribute.String("routeiq.tool.arguments_hash", argsHash),
		attribute.String("routeiq.tool.permission_level", perm),
	)
	t.span.SetAttributes(attrs...)
}

// Success records a successful tool result.
func (t *ToolHandle) Success(latencyMs ...float64) {
	t.finish(toolSuccess, "", latencyMs...)
}

// Fail records a failed tool result.
func (t *ToolHandle) Fail(errorCode string, latencyMs ...float64) {
	t.finish(toolFailure, errorCode, latencyMs...)
}

func (t *ToolHandle) finish(status, errorCode string, latencyMs ...float64) {
	if t.done {
		return
	}
	t.done = true
	ms := float64(time.Since(t.start).Milliseconds())
	if len(latencyMs) > 0 {
		ms = latencyMs[0]
	}
	attrs := []attribute.KeyValue{
		attribute.String("routeiq.tool.result_status", status),
		attribute.Float64("routeiq.tool.latency_ms", ms),
	}
	if errorCode != "" {
		attrs = append(attrs, attribute.String("routeiq.tool.error_code", errorCode))
	}
	t.span.SetAttributes(attrs...)
}

// End closes the tool span. Auto-succeeds if not already done.
func (t *ToolHandle) End() {
	if !t.done {
		t.Success()
	}
	t.span.End()
}

// ── helpers ───────────────────────────────────────────────────────────────────

func argsHash(args map[string]any) string {
	b, _ := json.Marshal(args)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:8])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
