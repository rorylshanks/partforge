package main

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
)

const humanByteUnit = 1024

var byteUnits = []string{"B", "KB", "MB", "GB", "TB", "PB"}

func humanizeLogAttr(_ []string, attr slog.Attr) slog.Attr {
	key := strings.ToLower(attr.Key)
	if isByteRateKey(key) {
		if value, ok := logAttrFloat(attr.Value); ok {
			return slog.String(attr.Key, formatByteRate(value))
		}
		return attr
	}
	if isByteKey(key) {
		if value, ok := logAttrFloat(attr.Value); ok {
			return slog.String(attr.Key, formatByteQuantity(value))
		}
	}
	return attr
}

func isByteRateKey(key string) bool {
	return key == "bytes_per_second" || strings.HasSuffix(key, "_bytes_per_second")
}

func isByteKey(key string) bool {
	return key == "bytes" || key == "max_memory_usage" || strings.HasSuffix(key, "_bytes")
}

func logAttrFloat(value slog.Value) (float64, bool) {
	switch value.Kind() {
	case slog.KindInt64:
		n := value.Int64()
		if n < 0 {
			return 0, false
		}
		return float64(n), true
	case slog.KindUint64:
		return float64(value.Uint64()), true
	case slog.KindFloat64:
		n := value.Float64()
		if n < 0 {
			return 0, false
		}
		return n, true
	case slog.KindString:
		n, err := strconv.ParseFloat(strings.TrimSpace(value.String()), 64)
		if err != nil || n < 0 {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func formatBytes(bytes uint64) string {
	return formatByteQuantity(float64(bytes))
}

func formatByteRate(bytesPerSecond float64) string {
	return formatByteValue(bytesPerSecond, "/s")
}

func formatByteQuantity(bytes float64) string {
	return formatByteValue(bytes, "")
}

func formatByteValue(value float64, suffix string) string {
	unit := 0
	for value >= humanByteUnit && unit < len(byteUnits)-1 {
		value /= humanByteUnit
		unit++
	}
	formatted := formatHumanFloat(value)
	return formatted + " " + byteUnits[unit] + suffix
}

func formatHumanFloat(value float64) string {
	if math.Trunc(value) == value {
		return fmt.Sprintf("%.0f", value)
	}
	return fmt.Sprintf("%.1f", value)
}
