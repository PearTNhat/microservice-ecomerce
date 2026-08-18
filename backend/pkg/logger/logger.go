package logger

import (
	"context"
	"encoding/json"
	"fmt"
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
	defaultLogger *slog.Logger = slog.Default()
	gelfConn      net.Conn
	gelfMutex     sync.Mutex
	serviceName   string = "app"
	environment   string = "development"
)

func getLogger() *slog.Logger {
	if defaultLogger == nil {
		return slog.Default()
	}
	return defaultLogger
}

// PrettyTextHandler định dạng log ở terminal cực kỳ ngắn gọn, dễ đọc khi dev local
type PrettyTextHandler struct {
	slog.Handler
	svcName string
}

func (h *PrettyTextHandler) Handle(ctx context.Context, r slog.Record) error {
	// Gửi bản sao đầy đủ thông tin tới Graylog qua UDP
	go sendGELF(ctx, r)

	timeStr := r.Time.Format("15:04:05")

	var levelColor, levelStr string
	switch {
	case r.Level >= slog.LevelError:
		levelColor = "\033[31m" // Đỏ
		levelStr = "ERROR"
	case r.Level >= slog.LevelWarn:
		levelColor = "\033[33m" // Vàng
		levelStr = "WARN "
	case r.Level >= slog.LevelInfo:
		levelColor = "\033[32m" // Xanh lá
		levelStr = "INFO "
	default:
		levelColor = "\033[36m" // Cyan
		levelStr = "DEBUG"
	}
	resetColor := "\033[0m"

	// Thu thập các thuộc tính kèm theo
	attrs := ""
	r.Attrs(func(a slog.Attr) bool {
		attrs += fmt.Sprintf(" %s=%v", a.Key, a.Value)
		return true
	})

	// Lấy trace_id nếu có trong context
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		attrs += fmt.Sprintf(" trace_id=%s", traceID)
	}

	// In ra terminal 1 dòng duy nhất cực gọn: [15:04:05] INFO [user-service] Message attrs...
	fmt.Fprintf(os.Stdout, "%s [%s%s%s] \033[34m[%s]\033[0m %s%s\n",
		timeStr, levelColor, levelStr, resetColor, h.svcName, r.Message, attrs,
	)

	return nil
}

// ContextHandler nhồi Trace ID & User ID từ context vào JSON Log
type ContextHandler struct {
	slog.Handler
}

func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		r.AddAttrs(slog.String("trace_id", traceID))
	}

	if userID, ok := ctx.Value(UserIDKey).(string); ok && userID != "" {
		r.AddAttrs(slog.String("user_id", userID))
	}

	// Gửi bản sao qua GELF UDP đến Graylog
	go sendGELF(ctx, r)

	return h.Handler.Handle(ctx, r)
}

// InitLogger khởi tạo hệ thống logging
func InitLogger(svcName, env, graylogAddr string) {
	serviceName = svcName
	environment = env

	hostname, _ := os.Hostname()

	if env == "production" {
		// Ở Production: In dạng JSON đầy đủ metadata cho Docker/K8s
		jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level:     slog.LevelInfo,
			AddSource: true,
		}).WithAttrs([]slog.Attr{
			slog.String("service", svcName),
			slog.String("env", env),
			slog.String("host", hostname),
		})
		defaultLogger = slog.New(&ContextHandler{Handler: jsonHandler})
	} else {
		// Ở Development: In dạng Pretty Console 1 dòng có màu sắc cực kỳ gọn gàng
		baseHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})
		defaultLogger = slog.New(&PrettyTextHandler{
			Handler: baseHandler,
			svcName: svcName,
		})
	}

	slog.SetDefault(defaultLogger)

	// Khởi tạo kết nối UDP tới Graylog GELF Input
	if graylogAddr != "" {
		conn, err := net.DialTimeout("udp", graylogAddr, 2*time.Second)
		if err == nil {
			gelfConn = conn
			Info("✅ Đã kết nối UDP tới Graylog GELF input", "addr", graylogAddr)
		} else {
			Warn("⚠️ Chưa thể kết nối tới Graylog UDP (sẽ chỉ log ra console)", "error", err.Error())
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

	gelfMap := map[string]interface{}{
		"version":       "1.1",
		"host":          hostname,
		"short_message": r.Message,
		"timestamp":     float64(r.Time.UnixNano()) / 1e9,
		"level":         syslogLevel,
		"_service":      serviceName,
		"_env":          environment,
	}

	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		gelfMap["_trace_id"] = traceID
	}
	if userID, ok := ctx.Value(UserIDKey).(string); ok && userID != "" {
		gelfMap["_user_id"] = userID
	}

	r.Attrs(func(a slog.Attr) bool {
		gelfMap["_"+a.Key] = a.Value.Any()
		return true
	})

	data, err := json.Marshal(gelfMap)
	if err != nil {
		return
	}

	_, _ = gelfConn.Write(data)
}

func SetTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceIDKey, traceID)
}

func GetTraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value(TraceIDKey).(string); ok {
		return traceID
	}
	return ""
}

func SetUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, UserIDKey, userID)
}

func Info(msg string, args ...any) {
	getLogger().Info(msg, args...)
}

func InfoContext(ctx context.Context, msg string, args ...any) {
	getLogger().InfoContext(ctx, msg, args...)
}

func Warn(msg string, args ...any) {
	getLogger().Warn(msg, args...)
}

func WarnContext(ctx context.Context, msg string, args ...any) {
	getLogger().WarnContext(ctx, msg, args...)
}

func Error(msg string, args ...any) {
	getLogger().Error(msg, args...)
}

func ErrorContext(ctx context.Context, msg string, args ...any) {
	getLogger().ErrorContext(ctx, msg, args...)
}

func Debug(msg string, args ...any) {
	getLogger().Debug(msg, args...)
}

func DebugContext(ctx context.Context, msg string, args ...any) {
	getLogger().DebugContext(ctx, msg, args...)
}
