package rewrite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/metrics"
)

func TestConfigureCompactMergeSettingsDoesNotSetVerticalMergeAlgorithm(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		queries = append(queries, string(body))
	}))
	defer server.Close()

	err := (Compactor{
		ClickHouse: chhttp.Client{URL: server.URL},
		MergeTreeSettings: MergeTreeSettings{
			MergeMaxBlockSize:      32768,
			MergeMaxBlockSizeBytes: 67108864,
			MergeSelectingSleepMS:  1000,
		},
	}).configureCompactMergeSettings(context.Background(), CompactWorkItem{
		JobID:               "job-1",
		OutputPartID:        "compact-1",
		DestinationDatabase: "db",
		DestinationTable:    "query_log_archive_temp",
	}, 100*1024*1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 1 {
		t.Fatalf("queries = %#v, want one query", queries)
	}
	if strings.Contains(queries[0], "enable_vertical_merge_algorithm") {
		t.Fatalf("query = %q, want vertical merge algorithm unset", queries[0])
	}
}

func TestCompactProgressRejectsOutputMoreThanAttachedInput(t *testing.T) {
	err := (Compactor{}).reportProgress(context.Background(), CompactWorkItem{
		JobID:        "job-1",
		OutputPartID: "compact-1",
	}, CompactProgressSnapshot{
		InputStats:       metrics.PartStats{Count: 2},
		DestinationStats: metrics.PartStats{Count: 3},
	})
	if err == nil {
		t.Fatal("expected compact part accounting error")
	}
	if !strings.Contains(err.Error(), "exceeds attached input parts") {
		t.Fatalf("error = %v, want attached input accounting error", err)
	}
}

func TestCompactorPhaseContextCancelsOnShutdown(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	phaseCtx, cancelPhase := (Compactor{ShutdownContext: shutdownCtx}).phaseContext(context.Background())
	defer cancelPhase()

	cancelShutdown()

	select {
	case <-phaseCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("phase context did not cancel after shutdown")
	}
	if !errors.Is(phaseCtx.Err(), context.Canceled) {
		t.Fatalf("phase context error = %v, want context.Canceled", phaseCtx.Err())
	}
}

func TestCompactMergeTimeoutUntil(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)

	timeout, ok := compactMergeTimeoutUntil(time.Time{}, now)
	if ok || timeout != 0 {
		t.Fatalf("compactMergeTimeoutUntil without deadline = %s, %t; want 0, false", timeout, ok)
	}

	timeout, ok = compactMergeTimeoutUntil(now.Add(30*time.Minute), now)
	if !ok || timeout != 30*time.Minute {
		t.Fatalf("compactMergeTimeoutUntil future = %s, %t; want 30m, true", timeout, ok)
	}

	timeout, ok = compactMergeTimeoutUntil(now, now)
	if !ok || timeout != 0 {
		t.Fatalf("compactMergeTimeoutUntil elapsed = %s, %t; want 0, true", timeout, ok)
	}
}

func TestCompactMergeTimeoutsForDeadlineKeepsIdleTimeout(t *testing.T) {
	timeout, maxTimeout := compactMergeTimeoutsForDeadline(15*time.Minute, 24*time.Hour, 2*time.Hour)

	if timeout != 15*time.Minute {
		t.Fatalf("timeout = %s, want compact idle timeout", timeout)
	}
	if maxTimeout != 2*time.Hour {
		t.Fatalf("max timeout = %s, want compact window deadline", maxTimeout)
	}
}

func TestCompactMergeTimeoutsForDeadlineCapsIdleTimeout(t *testing.T) {
	timeout, maxTimeout := compactMergeTimeoutsForDeadline(15*time.Minute, 24*time.Hour, time.Minute)

	if timeout != time.Minute {
		t.Fatalf("timeout = %s, want remaining deadline", timeout)
	}
	if maxTimeout != time.Minute {
		t.Fatalf("max timeout = %s, want remaining deadline", maxTimeout)
	}
}

func TestAddPartStats(t *testing.T) {
	got := addPartStats(
		metrics.PartStats{Count: 1, Rows: 2, Bytes: 3},
		metrics.PartStats{Count: 4, Rows: 5, Bytes: 6},
	)
	want := metrics.PartStats{Count: 5, Rows: 7, Bytes: 9}
	if got != want {
		t.Fatalf("addPartStats = %+v, want %+v", got, want)
	}
}
