package rewrite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/manifest"
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

func TestResetDestinationTableAllowsLargeDrop(t *testing.T) {
	var requests []struct {
		query string
		body  string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		requests = append(requests, struct {
			query string
			body  string
		}{
			query: r.URL.RawQuery,
			body:  string(body),
		})
	}))
	defer server.Close()

	destDDL := "CREATE TABLE `db`.`query_log_archive_temp` (x UInt64) ENGINE = MergeTree ORDER BY tuple()"
	err := resetDestinationTable(context.Background(), chhttp.Client{URL: server.URL}, manifest.Manifest{
		Dest: manifest.TableRef{Database: "db", Table: "query_log_archive_temp"},
	}, destDDL)
	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if requests[0].body != "DROP TABLE IF EXISTS `db`.`query_log_archive_temp` SYNC" {
		t.Fatalf("drop query = %q", requests[0].body)
	}
	dropSettings := requests[0].query
	if !strings.Contains(dropSettings, "max_table_size_to_drop=0") {
		t.Fatalf("drop settings = %q, want max_table_size_to_drop=0", dropSettings)
	}
	if !strings.Contains(dropSettings, "max_partition_size_to_drop=0") {
		t.Fatalf("drop settings = %q, want max_partition_size_to_drop=0", dropSettings)
	}
	if requests[1].body != destDDL {
		t.Fatalf("recreate query = %q", requests[1].body)
	}
	if strings.Contains(requests[1].query, "max_table_size_to_drop") || strings.Contains(requests[1].query, "max_partition_size_to_drop") {
		t.Fatalf("recreate settings = %q, want no drop-size settings", requests[1].query)
	}
}

func TestWaitForMergesReturnsUnsettledAfterTimeout(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if _, err := io.ReadAll(r.Body); err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("3\n"))
	}))
	defer server.Close()

	timeout := -time.Nanosecond
	result, err := (Processor{
		ClickHouse:   chhttp.Client{URL: server.URL},
		MergeTimeout: timeout,
	}).waitForMerges(context.Background(), "db", "query_log_archive_temp")
	if err != nil {
		t.Fatal(err)
	}
	if result.Settled {
		t.Fatal("expected unsettled merge result")
	}
	if result.ActiveMerges != 3 {
		t.Fatalf("active merges = %d, want 3", result.ActiveMerges)
	}
	if result.Timeout != timeout {
		t.Fatalf("timeout = %s, want %s", result.Timeout, timeout)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

func TestWaitForMergesUsesDefaultTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("0\n"))
	}))
	defer server.Close()

	result, err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).waitForMerges(context.Background(), "db", "query_log_archive_temp")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Settled {
		t.Fatal("expected settled merge result")
	}
	if result.Timeout != DefaultMergeTimeout {
		t.Fatalf("timeout = %s, want %s", result.Timeout, DefaultMergeTimeout)
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

func TestFrozenPartUploadGlobs(t *testing.T) {
	root := t.TempDir()
	diskPath := filepath.Join(root, "disk")
	freezeName := "freeze_1"
	frozenStore := filepath.Join(diskPath, "shadow", freezeName, "store")
	if err := os.MkdirAll(frozenStore, 0o755); err != nil {
		t.Fatal(err)
	}

	globs, err := frozenPartUploadGlobs([]freeze.Disk{{Name: "default", Path: diskPath, Type: "local"}}, freezeName)
	if err != nil {
		t.Fatal(err)
	}
	if len(globs) != 1 {
		t.Fatalf("frozen part globs = %#v, want one glob", globs)
	}
	wantGlob := filepath.Join(frozenStore, "*", "*", "*")
	if globs[0].Disk != "default" || globs[0].Glob != wantGlob {
		t.Fatalf("frozen part globs = %#v, want default at %s", globs, wantGlob)
	}
}

func TestFrozenPartUploadGlobsRequiresAtLeastOneStore(t *testing.T) {
	root := t.TempDir()

	_, err := frozenPartUploadGlobs([]freeze.Disk{{Name: "default", Path: root, Type: "local"}}, "freeze_1")
	if err == nil {
		t.Fatal("expected missing store error")
	}
}

func TestWorkerFreezeNameNeedsNoClickHouseEscaping(t *testing.T) {
	name := workerFreezeName(manifest.Manifest{JobID: "job-1", PartID: "part.2"}, time.Date(2026, 6, 17, 15, 48, 15, 768144022, time.UTC))
	if strings.ContainsAny(name, "-.") {
		t.Fatalf("freeze name = %q, expected no ClickHouse-escaped punctuation", name)
	}
	if name != "partforge_job_1_part_2_20260617T154815768144022Z" {
		t.Fatalf("freeze name = %q", name)
	}
}

func createFrozenPart(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(path, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(path, "data.bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
}
