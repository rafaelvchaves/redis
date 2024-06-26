package ctxlog

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

var keys []any

func AddKeyPrefix(key any) {
	keys = append(keys, key)
}

func prefix(ctx context.Context) string {
	vals := make([]string, len(keys))
	for i, key := range keys {
		if s, ok := ctx.Value(key).(string); ok {
			vals[i] = s
		}
	}
	return "[" + strings.Join(vals, " ") + "]"
}

func Infof(ctx context.Context, msg string, args ...any) {
	slog.InfoContext(ctx, fmt.Sprintf(prefix(ctx)+" "+msg, args...))
}

func Errorf(ctx context.Context, msg string, args ...any) {
	slog.ErrorContext(ctx, fmt.Sprintf(prefix(ctx)+" "+msg, args...))
}

func Fatalf(ctx context.Context, msg string, args ...any) {
	slog.ErrorContext(ctx, fmt.Sprintf(prefix(ctx)+" "+msg, args...))
	os.Exit(1)
}
