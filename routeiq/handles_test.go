package routeiq

import (
	"context"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ── test helpers ──────────────────────────────────────────────────────────────

type spanRecorder struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (r *spanRecorder) OnStart(_ context.Context, _ sdktrace.ReadWriteSpan) {}
func (r *spanRecorder) OnEnd(s sdktrace.ReadOnlySpan) {
	r.mu.Lock()
	r.spans = append(r.spans, s)
	r.mu.Unlock()
}
func (r *spanRecorder) Shutdown(_ context.Context) error  { return nil }
func (r *spanRecorder) ForceFlush(_ context.Context) error { return nil }

func makeTestRiq(t *testing.T) (*RouteIQ, *spanRecorder) {
	t.Helper()
	rec := &spanRecorder{}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	riq := newForTest(provider, Options{
		AgentID:      "test-agent",
		TenantID:     "test-tenant",
		Environment:  "test",
		Model:        "gpt-4o",
		AgentVersion: "1.0.0",
	})
	return riq, rec
}

func (r *spanRecorder) all() []sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(r.spans))
	copy(out, r.spans)
	return out
}

func byName(spans []sdktrace.ReadOnlySpan, prefix string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if strings.HasPrefix(s.Name(), prefix) {
			return s
		}
	}
	return nil
}

func attrStr(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func attrInt(s sdktrace.ReadOnlySpan, key attribute.Key) int64 {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value.AsInt64()
		}
	}
	return 0
}

// ── TaskHandle ────────────────────────────────────────────────────────────────

func TestTaskSpanName(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "find Paris")
	task.End()

	if byName(rec.all(), "task:") == nil {
		t.Fatal("expected span with name starting task:")
	}
}

func TestTaskEnvelopeAttrs(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "find Paris")
	taskID := task.taskID
	task.End()

	span := byName(rec.all(), "task:")
	if span == nil {
		t.Fatal("task span not found")
	}
	if attrStr(span, "routeiq.agent.id") != "test-agent" {
		t.Error("missing agent.id")
	}
	if attrStr(span, "routeiq.session.id") != riq.sessionID {
		t.Error("session_id mismatch")
	}
	if attrStr(span, "routeiq.task.id") != taskID {
		t.Error("task_id mismatch")
	}
	if attrStr(span, "routeiq.task.input_intent") != "find Paris" {
		t.Error("intent mismatch")
	}
	if attrStr(span, "routeiq.version.model.name") != "gpt-4o" {
		t.Error("model mismatch")
	}
}

func TestTaskComplete(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	task.Complete(WithTokens(100), WithCohort("test"))
	task.End()

	span := byName(rec.all(), "task:")
	if span == nil {
		t.Fatal("task span not found")
	}
	if attrStr(span, "routeiq.task.completion_status") != "1" {
		t.Error("expected success status")
	}
	if attrInt(span, "routeiq.task.total_tokens") != 100 {
		t.Error("tokens mismatch")
	}
	if attrStr(span, "routeiq.task.cohort") != "test" {
		t.Error("cohort mismatch")
	}
}

func TestTaskFail(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	task.Fail("tool_error")
	task.End()

	span := byName(rec.all(), "task:")
	if span == nil {
		t.Fatal("task span not found")
	}
	if attrStr(span, "routeiq.task.completion_status") != "2" {
		t.Error("expected failure status")
	}
	if attrStr(span, "routeiq.task.failure_category") != "tool_error" {
		t.Error("failure_category mismatch")
	}
}

func TestTaskAutoComplete(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	task.End()

	span := byName(rec.all(), "task:")
	if span == nil {
		t.Fatal("task span not found")
	}
	if attrStr(span, "routeiq.task.completion_status") != "1" {
		t.Error("expected auto-success on End()")
	}
}

// ── StepHandle ────────────────────────────────────────────────────────────────

func TestStepSpanName(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step(WithAction("tool_call"))
	step.End()
	task.End()

	if byName(rec.all(), "step:") == nil {
		t.Fatal("expected span with name starting step:")
	}
}

func TestStepCarriesTaskID(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	stepID := step.stepID
	step.End()
	task.End()

	span := byName(rec.all(), "step:")
	if span == nil {
		t.Fatal("step span not found")
	}
	if attrStr(span, "routeiq.task.id") != task.taskID {
		t.Error("task_id mismatch on step span")
	}
	if attrStr(span, "routeiq.step.id") != stepID {
		t.Error("step_id mismatch")
	}
}

func TestStepIndexIncrements(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	s1 := task.Step()
	s1.End()
	s2 := task.Step()
	s2.End()
	task.End()

	var indices []int64
	for _, s := range rec.all() {
		if strings.HasPrefix(s.Name(), "step:") {
			indices = append(indices, attrInt(s, "routeiq.step.index"))
		}
	}
	if len(indices) != 2 || indices[0]+indices[1] != 3 {
		t.Errorf("expected step indices {1,2}, got %v", indices)
	}
}

// ── ToolHandle ────────────────────────────────────────────────────────────────

func TestToolSpanName(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	tool := step.Tool("search", WithArgs(map[string]any{"query": "Paris"}))
	tool.End()
	step.End()
	task.End()

	if byName(rec.all(), "tool:search") == nil {
		t.Fatal("expected span tool:search")
	}
}

func TestToolSuccess(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	tool := step.Tool("search")
	tool.Success(50.0)
	tool.End()
	step.End()
	task.End()

	span := byName(rec.all(), "tool:search")
	if span == nil {
		t.Fatal("tool span not found")
	}
	if attrStr(span, "routeiq.tool.result_status") != "1" {
		t.Error("expected success status")
	}
}

func TestToolFail(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	tool := step.Tool("search")
	tool.Fail("TIMEOUT")
	tool.End()
	step.End()
	task.End()

	span := byName(rec.all(), "tool:search")
	if span == nil {
		t.Fatal("tool span not found")
	}
	if attrStr(span, "routeiq.tool.result_status") != "2" {
		t.Error("expected failure status")
	}
	if attrStr(span, "routeiq.tool.error_code") != "TIMEOUT" {
		t.Error("error_code mismatch")
	}
}

func TestToolAutoSucceeds(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	tool := step.Tool("search")
	tool.End()
	step.End()
	task.End()

	span := byName(rec.all(), "tool:search")
	if span == nil {
		t.Fatal("tool span not found")
	}
	if attrStr(span, "routeiq.tool.result_status") != "1" {
		t.Error("expected auto-success on End()")
	}
}

func TestToolArgsHash(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	tool := step.Tool("search", WithArgs(map[string]any{"query": "Paris"}))
	tool.End()
	step.End()
	task.End()

	span := byName(rec.all(), "tool:search")
	if span == nil {
		t.Fatal("tool span not found")
	}
	h := attrStr(span, "routeiq.tool.arguments_hash")
	if len(h) != 16 {
		t.Errorf("expected 16-char hash, got %q", h)
	}
}

func TestSessionIDConsistent(t *testing.T) {
	riq, rec := makeTestRiq(t)
	task := riq.Task(context.Background(), "q")
	step := task.Step()
	tool := step.Tool("search")
	tool.End()
	step.End()
	task.End()

	seen := map[string]bool{}
	for _, s := range rec.all() {
		seen[attrStr(s, "routeiq.session.id")] = true
	}
	if len(seen) != 1 || !seen[riq.sessionID] {
		t.Errorf("session_id inconsistent across spans: %v", seen)
	}
}
