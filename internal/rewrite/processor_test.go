package rewrite

import (
	"errors"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/chhttp"
)

func TestReduceInsertSelectThreadSettings(t *testing.T) {
	next, reduced, err := reduceInsertSelectThreadSettings(chhttp.QuerySettings{
		"max_threads":        "8",
		"max_insert_threads": "6",
		"max_memory_usage":   "12345",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reduced {
		t.Fatal("expected settings to be reduced")
	}
	if next["max_threads"] != "4" {
		t.Fatalf("max_threads = %q", next["max_threads"])
	}
	if next["max_insert_threads"] != "3" {
		t.Fatalf("max_insert_threads = %q", next["max_insert_threads"])
	}
	if next["max_memory_usage"] != "12345" {
		t.Fatalf("max_memory_usage = %q", next["max_memory_usage"])
	}
}

func TestReduceInsertSelectThreadSettingsStopsAtOne(t *testing.T) {
	_, reduced, err := reduceInsertSelectThreadSettings(chhttp.QuerySettings{
		"max_threads":        "1",
		"max_insert_threads": "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if reduced {
		t.Fatal("expected no reduction once max_insert_threads is 1")
	}
}

func TestRetryableInsertSelectError(t *testing.T) {
	err := &chhttp.QueryError{StatusCode: 500, Body: "Code: 241. DB::Exception: MEMORY_LIMIT_EXCEEDED"}
	if !retryableInsertSelectError(err) {
		t.Fatal("expected memory limit error to be retryable")
	}

	if retryableInsertSelectError(&chhttp.QueryError{StatusCode: 500, Body: "Syntax error"}) {
		t.Fatal("expected syntax error to be non-retryable")
	}
	if retryableInsertSelectError(errors.New("network error")) {
		t.Fatal("expected unstructured error to be non-retryable")
	}
}

func TestInsertSelectRetryBackoff(t *testing.T) {
	if got := insertSelectRetryBackoff(1); got != time.Second {
		t.Fatalf("attempt 1 backoff = %s", got)
	}
	if got := insertSelectRetryBackoff(4); got != 8*time.Second {
		t.Fatalf("attempt 4 backoff = %s", got)
	}
	if got := insertSelectRetryBackoff(10); got != 10*time.Second {
		t.Fatalf("attempt 10 backoff = %s", got)
	}
}

func TestShouldReportProgress(t *testing.T) {
	now := time.Unix(100, 0)
	if shouldReportProgress(0, time.Time{}, now) {
		t.Fatal("expected disabled interval to skip progress report")
	}
	if !shouldReportProgress(15*time.Second, time.Time{}, now) {
		t.Fatal("expected first progress report")
	}
	if shouldReportProgress(15*time.Second, now.Add(-14*time.Second), now) {
		t.Fatal("expected interval gate to skip report")
	}
	if !shouldReportProgress(15*time.Second, now.Add(-15*time.Second), now) {
		t.Fatal("expected interval gate to allow report")
	}
}
