package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

type contextKey string

const (
	TraceIDKey contextKey = "trace_id"
	UserIDKey  contextKey = "user_id"
)

var (
	defaultLogger *slog.Logger
	gelfConn      net.Conn
	gelfMutex     sync.Mutex
	serviceName   string
	environment   string
)

// GELFLogRecord định dạng theo chuẩn Graylog Extended Log Format (GELF 1.1)
type GELFLogRecord struct {
	Version      string                 `json:"version"`
	Host         string                 `json:"host"`
	ShortMessage string                 `json:"short_message"`
	Timestamp    float64                `json:"timestamp"`
	Level        int                    `json:"level"`
	Facility     string                 `json:"_facility,omitempty"`
	Service      string                 `json:"_service,omitempty"`
	Environment  string                 `json:"_environment,omitempty"`
	TraceID      string                 `json:"_trace_id,omitempty"`
	UserID       string                 `json:"_user_id,omitempty"`
	Extra        map[string]interface{} `json:"-"`
}

// ContextHandler tự động trích xuất TraceID/UserID từ context.Context vào log fields
type ContextHandler struct {
	slog.Handler
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
			r.AddAttrs(slog.String("trace_id", traceID))
		}
		if userID, ok := ctx.Value(UserIDKey).(string); ok && userID != "" {
			r.AddAttrs(slog.String("user_id", userID))
		}
	}

	// Gửi thêm bản sao qua GELF UDP đến Graylog nếu đã cấu hình
	go sendGELF(ctx, r)

	return h.Handler.Handle(ctx, r)
}

// InitLogger khởi tạo hệ thống logging có cấu trúc
func InitLogger(svcName, env, graylogAddr string) {
	serviceName = svcName
	environment = env

	var level slog.Level
	if env == "production" {
		level = slog.LevelInfo
	} else {
		level = slog.LevelDebug
	}

	hostname, _ := os.Hostname()

	// Handler in ra stdout định dạng JSON
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}).WithAttrs([]slog.Attr{
		slog.String("service", svcName),
		slog.String("env", env),
		slog.String("host", hostname),
	})

	contextHandler := &ContextHandler{Handler: jsonHandler}
	defaultLogger = slog.New(contextHandler)
	slog.SetDefault(defaultLogger)

	// Khởi tạo kết nối UDP tới Graylog GELF Input
	if graylogAddr != "" {
		conn, err := net.DialTimeout("udp", graylogAddr, 2*time.Second)
		if err == nil {
			gelfConn = conn
			slog.Info("✅ Đã kết nối UDP tới Graylog GELF input", slog.String("addr", graylogAddr))
		} else {
			slog.Warn("⚠️ Chưa thể kết nối tới Graylog UDP (sẽ chỉ log ra console)", slog.String("error", err.Error()))
		}
	}
}

func sendGELF(ctx context.Context, r slog.Record) {
	if gelfConn == nil {
		return
	}

	gelfMutex.Lock()
	defer gelfMutex.Unlock()

	hostname, _ := os.Hostname()

	// Map slog.Level sang Syslog Level cho Graylog (1=Alert, 3=Error, 4=Warn, 6=Info, 7=Debug)
	syslogLevel := 6
	switch {
	case r.Level >= slog.LevelError:
		syslogLevel = 3
	case r.Level >= slog.LevelWarn:
		syslogLevel = 4
	case r.Level >= slog.LevelInfo:
		syslogLevel = 6
	default:
		syslogLevel = 7
	}

	traceID := ""
	userID := ""
	if ctx != nil {
		if tid, ok := ctx.Value(TraceIDKey).(string); ok {
			traceID = tid
		}
		if uid, ok := ctx.Value(UserIDKey).(string); ok {
			userID = uid
		}
	}

	gelf := map[string]interface{}{
		"version":       "1.1",
		"host":          hostname,
		"short_message": r.Message,
		"timestamp":     float64(r.Time.UnixNano()) / 1e9,
		"level":         syslogLevel,
		"_service":      serviceName,
		"_environment":  environment,
		"_trace_id":     traceID,
		"_user_id":      userID,
	}

	r.Attrs(func(a slog.Attr) bool {
		gelf["_"+a.Key] = a.Value.Any()
		return true
	})

	data, err := json.Marshal(gelf)
	if err == nil {
		gelfConn.Write(data)
	}
}

// Helper context functions
func SetTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

func GetTraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(TraceIDKey).(string); ok {
		return val
	}
	return ""
}

func SetUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

// Log functions
func Info(msg string, args ...any) {
	slog.Info(msg, args...)
}

func InfoContext(ctx context.Context, msg string, args ...any) {
	slog.InfoContext(ctx, msg, args...)
}

func Error(msg string, args ...any) {
	slog.Error(msg, args...)
}

func ErrorContext(ctx context.Context, msg string, args ...any) {
	slog.ErrorContext(ctx, msg, args...)
}

func Warn(msg string, args ...any) {
	slog.Warn(msg, args...)
}

func WarnContext(ctx context.Context, msg string, args ...any) {
	slog.WarnContext(ctx, msg, args...)
}

func Debug(msg string, args ...any) {
	slog.Debug(msg, args...)
}

func DebugContext(ctx context.Context, msg string, args ...any) {
	slog.DebugContext(ctx, msg, args...)
}

func LogWriter() io.Writer {
	return &logWriterWrapper{}
}

type logWriterWrapper struct{}

func (w *logWriterWrapper) Write(p []byte) (n int, err error) {
	cleanMsg := string(bytes.TrimSpace(p))
	if cleanMsg != "" {
		slog.Info(cleanMsg)
	}
	return len(p), nil
}

func init() {
	if defaultLogger == nil {
		InitLogger("user-service", "development", "")
	}
}
