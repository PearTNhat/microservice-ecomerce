package logger

import (
	"context"
	"testing"
)

func TestLogger_TraceContext(t *testing.T) {
	InitLogger("test-service", "development", "")

	ctx := context.Background()
	ctx = SetTraceID(ctx, "test-trace-12345")
	ctx = SetUserID(ctx, "user-999")

	if GetTraceID(ctx) != "test-trace-12345" {
		t.Errorf("GetTraceID không khớp, nhận: %s", GetTraceID(ctx))
	}

	InfoContext(ctx, "Test structured log with context", "custom_key", "custom_value")
}
