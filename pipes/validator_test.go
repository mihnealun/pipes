package pipes

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestValidator_Execute_NilInput(t *testing.T) {
	v := &Validator{}

	if err := v.Execute(testContext(), testTracer(), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil input")
	}
}

func TestValidator_Execute_WrongType(t *testing.T) {
	v := &Validator{}

	if err := v.Execute(testContext(), testTracer(), testLogger(), "not bytes"); err == nil {
		t.Fatal("expected error for non-[]byte input")
	}
}

func TestValidator_Execute_MalformedJSON(t *testing.T) {
	v := &Validator{}

	err := v.Execute(testContext(), testTracer(), testLogger(), []byte(`{not valid json`))
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestValidator_Execute_RejectsUnwhitelistedValues(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "event_type not whitelisted",
			body: `{"event_type":"not.a.real.event","resource_type":"backup","status":"READY"}`,
		},
		{
			name: "resource_type not whitelisted",
			body: `{"event_type":"backup.create.end","resource_type":"not-a-real-resource","status":"READY"}`,
		},
		{
			name: "status not whitelisted",
			body: `{"event_type":"backup.create.end","resource_type":"backup","status":"NOT_A_REAL_STATUS"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := &Validator{}
			err := v.Execute(testContext(), testTracer(), testLogger(), []byte(tc.body))
			if err == nil {
				t.Fatal("expected error for invalid message")
			}
		})
	}
}

func TestValidator_Execute_ValidMessageForwardsUnmarshalledMap(t *testing.T) {
	body := []byte(`{"event_type":"backup.create.end","resource_type":"backup","status":"READY","note":"hello"}`)

	var expected map[string]any
	if err := json.Unmarshal(body, &expected); err != nil {
		t.Fatalf("failed to prepare expected value: %v", err)
	}

	next := &mockProcessor{}
	v := &Validator{}
	v.SetNext(next)

	err := v.Execute(testContext(), testTracer(), testLogger(), body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(next.calls) != 1 {
		t.Fatalf("expected next to be called once, got %d", len(next.calls))
	}
	got, ok := next.calls[0].(map[string]any)
	if !ok {
		t.Fatalf("expected next to receive map[string]any, got %T", next.calls[0])
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("got %#v, want %#v", got, expected)
	}
}

func TestValidator_Execute_NoNext(t *testing.T) {
	body := []byte(`{"event_type":"backup.create.end","resource_type":"backup","status":"READY"}`)
	v := &Validator{}

	if err := v.Execute(testContext(), testTracer(), testLogger(), body); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidator_Execute_PanicsOnMissingRequiredField documents an existing
// bug in isValidMessage: it type-asserts m["event_type"] etc. directly
// (m["event_type"].(string)) instead of using the comma-ok form, so a
// message missing one of the required fields panics instead of failing
// validation gracefully. If isValidMessage is hardened to return false
// instead of panicking, this test should be updated/removed.
func TestValidator_Execute_PanicsOnMissingRequiredField(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic due to missing event_type field; isValidMessage may have been fixed, update this test")
		}
	}()

	v := &Validator{}
	_ = v.Execute(testContext(), testTracer(), testLogger(), []byte(`{"resource_type":"backup","status":"READY"}`))
}
