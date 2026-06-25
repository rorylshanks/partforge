package rewrite

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/artifact"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/manifest"
	"github.com/partforge/partforge/internal/s3copy"
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

func TestRunInsertSelectSendsInsertBlockSettings(t *testing.T) {
	var insertSettings url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.HasPrefix(query, "INSERT "):
			insertSettings = r.URL.Query()
		case query == "SYSTEM FLUSH LOGS":
		case strings.Contains(query, "system.query_log"):
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	settings := chhttp.QuerySettings{
		"max_threads":                 "4",
		"max_insert_threads":          "4",
		"max_memory_usage":            "34359738368",
		"min_insert_block_size_rows":  "0",
		"min_insert_block_size_bytes": "2863311530",
	}
	err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).runInsertSelect(context.Background(), manifest.Manifest{
		JobID:  "job-1",
		PartID: "part-1",
		SQL:    manifest.SQLBundle{InsertSelect: "INSERT INTO dst SELECT * FROM src"},
	}, 1, settings)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range settings {
		if got := insertSettings.Get(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
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

func TestConfigureDestinationMergeSettings(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		queries = append(queries, query)
		if strings.Contains(query, "system.parts") {
			_, _ = w.Write([]byte("8\t1000\t107374182400\n"))
		}
	}))
	defer server.Close()

	err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
		MergeTreeSettings: MergeTreeSettings{
			MergeMaxBlockSize:      32768,
			MergeMaxBlockSizeBytes: 67108864,
			MergeSelectingSleepMS:  1000,
		},
	}).configureDestinationMergeSettings(context.Background(), manifest.Manifest{
		JobID:  "job-1",
		PartID: "part-1",
		Dest:   manifest.TableRef{Database: "db", Table: "query_log_archive_temp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "ALTER TABLE `db`.`query_log_archive_temp` MODIFY SETTING merge_max_block_size = 32768, merge_max_block_size_bytes = 67108864, merge_selecting_sleep_ms = 1000, max_bytes_to_merge_at_max_space_in_pool = 161061273600, max_bytes_to_merge_at_min_space_in_pool = 161061273600"
	if len(queries) != 2 || queries[1] != want {
		t.Fatalf("queries = %#v, want %q", queries, want)
	}
}

func TestConfigureDestinationCompressionCodec(t *testing.T) {
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

	err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
		MergeTreeSettings: MergeTreeSettings{
			DefaultCompressionCodec: "ZSTD(5)",
		},
	}).configureDestinationCompressionCodec(context.Background(), manifest.Manifest{
		JobID:  "job-1",
		PartID: "part-1",
		Dest:   manifest.TableRef{Database: "db", Table: "query_log_archive_temp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "ALTER TABLE `db`.`query_log_archive_temp` MODIFY SETTING default_compression_codec = 'ZSTD(5)'"
	if len(queries) != 1 || queries[0] != want {
		t.Fatalf("queries = %#v, want %q", queries, want)
	}
}

func TestMergePoolByteSettingsUseTargetPartSize(t *testing.T) {
	got := targetMergePoolByteSettings()
	if got.MaxBytesAtMaxSpaceInPool != targetMergePartBytes {
		t.Fatalf("max_bytes_to_merge_at_max_space_in_pool = %d, want %d", got.MaxBytesAtMaxSpaceInPool, targetMergePartBytes)
	}
	if got.MaxBytesAtMinSpaceInPool != targetMergePartBytes {
		t.Fatalf("max_bytes_to_merge_at_min_space_in_pool = %d, want %d", got.MaxBytesAtMinSpaceInPool, targetMergePartBytes)
	}
	if got.MaxBytesAtMinSpaceInPool == 0 || got.MaxBytesAtMinSpaceInPool > got.MaxBytesAtMaxSpaceInPool {
		t.Fatalf("invalid merge byte settings: %+v", got)
	}
}

func TestRunInsertSelectRetryDoesNotApplyDestinationMergeSettings(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		queries = append(queries, query)
		if strings.HasPrefix(query, "INSERT ") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("MEMORY_LIMIT_EXCEEDED"))
			return
		}
	}))
	defer server.Close()

	err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
		InsertSettings: chhttp.QuerySettings{
			"max_threads":        "2",
			"max_insert_threads": "2",
		},
		MergeTreeSettings: MergeTreeSettings{
			MergeMaxBlockSize:       32768,
			MergeMaxBlockSizeBytes:  67108864,
			MergeSelectingSleepMS:   1000,
			DefaultCompressionCodec: "ZSTD(5)",
		},
	}).runInsertSelectWithRetries(context.Background(), manifest.Manifest{
		JobID:  "job-1",
		PartID: "part-1",
		Dest:   manifest.TableRef{Database: "db", Table: "query_log_archive_temp"},
		SQL:    manifest.SQLBundle{InsertSelect: "INSERT INTO db.query_log_archive_temp SELECT 1"},
	}, "CREATE TABLE `db`.`query_log_archive_temp` (x UInt64) ENGINE = MergeTree ORDER BY x")
	if err == nil {
		t.Fatal("expected retryable insert error after reduced retry")
	}

	if containsQueryWith(queries, "merge_max_block_size") {
		t.Fatalf("queries = %#v, did not expect merge settings during insert retry", queries)
	}
	wantCompression := "ALTER TABLE `db`.`query_log_archive_temp` MODIFY SETTING default_compression_codec = 'ZSTD(5)'"
	if !containsString(queries, wantCompression) {
		t.Fatalf("queries = %#v, want compression settings after destination reset", queries)
	}
}

func TestRestartClickHouse(t *testing.T) {
	called := false
	err := (Processor{
		RestartClickHouse: func(ctx context.Context) error {
			called = true
			return nil
		},
	}).restartClickHouse(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected restart callback to be called")
	}
}

func TestRestartClickHouseRequiresCallback(t *testing.T) {
	err := (Processor{}).restartClickHouse(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"})
	if err == nil {
		t.Fatal("expected missing restart callback error")
	}
}

func TestDestinationFailedMergeCount(t *testing.T) {
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		queries = append(queries, query)
		if strings.Contains(query, "system.part_log") {
			_, _ = w.Write([]byte("7\n"))
		}
	}))
	defer server.Close()

	count, err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).destinationFailedMergeCount(context.Background(), testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if count != 7 {
		t.Fatalf("failed merge count = %d, want 7", count)
	}
	if len(queries) != 2 || queries[0] != "SYSTEM FLUSH LOGS" || !strings.Contains(queries[1], "system.part_log") || !strings.Contains(queries[1], "error != 0") {
		t.Fatalf("queries = %#v, want flush logs then failed merge count", queries)
	}
}

func TestWaitForDestinationMergesReportsSettled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte("0\t0\t0\n"))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	tracker := newRewriteStageTracker(time.Now(), stageProcessPart)
	settled, err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).waitForDestinationMerges(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"}, tracker, testMergeWaitTarget(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if !settled {
		t.Fatal("expected merge wait to settle")
	}
}

func TestWaitForDestinationMergesReturnsFalseAfterTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("1\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte(multiPartMergeSnapshot()))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	tracker := newRewriteStageTracker(time.Now(), stageProcessPart)
	settled, err := (Processor{
		ClickHouse:        chhttp.Client{URL: server.URL},
		MergeTimeout:      time.Nanosecond,
		MergeMaxTimeout:   time.Nanosecond,
		MergePollInterval: time.Nanosecond,
	}).waitForDestinationMerges(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"}, tracker, testMergeWaitTarget(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if settled {
		t.Fatal("expected merge wait timeout to return unsettled")
	}
}

func TestWaitForMergesReturnsUnsettledAfterTimeout(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("3\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte(largeTailMergeSnapshot()))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	timeout := time.Nanosecond
	result, err := (Processor{
		ClickHouse:      chhttp.Client{URL: server.URL},
		MergeTimeout:    timeout,
		MergeMaxTimeout: timeout,
	}).waitForMerges(context.Background(), testMergeWaitTarget())
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
	if requests < 2 {
		t.Fatalf("requests = %d, want at least 2", requests)
	}
}

func TestWaitForMergesUsesDefaultTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte("0\t0\t0\n"))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	result, err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).waitForMerges(context.Background(), testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Settled {
		t.Fatal("expected settled merge result")
	}
	if result.Timeout != DefaultMergeTimeout {
		t.Fatalf("timeout = %s, want %s", result.Timeout, DefaultMergeTimeout)
	}
	if result.MaxTimeout != DefaultMergeMaxTimeout {
		t.Fatalf("max timeout = %s, want %s", result.MaxTimeout, DefaultMergeMaxTimeout)
	}
}

func TestWaitForMergesSettlesWhenMergeTargetReachedAfterIdleWindow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "partition_id, bytes_on_disk"):
			_, _ = w.Write([]byte("202401\t161061273600\n202401\t53687091200\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte("2\t214748364800\t161061273600\n"))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	minWait := 5 * time.Millisecond
	result, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeSettleMinWait:  minWait,
		MergePollInterval:   time.Millisecond,
		MergeSettleMinParts: 1,
	}).waitForMerges(context.Background(), testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Settled {
		t.Fatal("expected target-sized parts to settle")
	}
	if result.Reason != "destination_merge_target_reached" {
		t.Fatalf("reason = %q, want destination_merge_target_reached", result.Reason)
	}
	if result.ActiveParts != 2 {
		t.Fatalf("active parts = %d, want 2", result.ActiveParts)
	}
	if result.ZeroMergesIdle < minWait {
		t.Fatalf("zero merges idle = %s, want at least %s", result.ZeroMergesIdle, minWait)
	}
}

func TestWaitForMergesStopsAtMergeMaxTimeoutForActiveMerges(t *testing.T) {
	var mergeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			mergeRequests++
			_, _ = w.Write([]byte("1\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte(multiPartMergeSnapshot()))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	baseTimeout := time.Millisecond
	maxTimeout := 5 * time.Millisecond
	result, err := (Processor{
		ClickHouse:        chhttp.Client{URL: server.URL},
		MergeTimeout:      baseTimeout,
		MergeMaxTimeout:   maxTimeout,
		MergePollInterval: time.Millisecond,
	}).waitForMerges(context.Background(), testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if result.Settled {
		t.Fatal("expected merge timeout before settling")
	}
	if result.Reason != "merge_max_timeout" {
		t.Fatalf("reason = %q, want merge_max_timeout", result.Reason)
	}
	if result.Timeout != maxTimeout {
		t.Fatalf("timeout = %s, want %s", result.Timeout, maxTimeout)
	}
	if mergeRequests == 0 {
		t.Fatal("expected system.merges to be queried")
	}
}

func TestWaitForMergesKeepsWaitingWhenZeroMergesAndManyActiveParts(t *testing.T) {
	var mergeRequests int
	var partRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			mergeRequests++
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "system.parts"):
			partRequests++
			_, _ = w.Write([]byte(multiPartMergeSnapshot()))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeSettleMinWait:  time.Hour,
		MergeSettleMinParts: 3,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForMerges error = %v, want context deadline exceeded", err)
	}
	if mergeRequests == 0 {
		t.Fatal("expected system.merges to be queried")
	}
	if partRequests == 0 {
		t.Fatal("expected system.parts to be queried after zero active merges")
	}
}

func TestWaitForMergesKeepsWaitingWhenZeroMergesAndLargeTailParts(t *testing.T) {
	var mergeRequests int
	var partRequests int
	var partSizeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			mergeRequests++
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "partition_id, bytes_on_disk"):
			partSizeRequests++
			_, _ = w.Write([]byte("202401\t5368709120\n202401\t5368709120\n"))
		case strings.Contains(query, "system.parts"):
			partRequests++
			_, _ = w.Write([]byte(largeTailMergeSnapshot()))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeSettleMinWait:  time.Millisecond,
		MergeSettleMinParts: 1,
		MergePollInterval:   time.Millisecond,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForMerges error = %v, want context deadline exceeded", err)
	}
	if mergeRequests == 0 {
		t.Fatal("expected system.merges to be queried")
	}
	if partRequests == 0 {
		t.Fatal("expected system.parts to be queried after zero active merges")
	}
	if partSizeRequests == 0 {
		t.Fatal("expected active part sizes to be queried after idle window")
	}
}

func TestWaitForMergesResetsIdleWindowWhenActivePartCountChanges(t *testing.T) {
	var mergeRequests int
	var partRequests int
	activeParts := []string{
		"4\t1073741824\t268435456\n",
		"3\t1073741824\t357913941\n",
		"4\t1073741824\t268435456\n",
		"3\t1073741824\t357913941\n",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			mergeRequests++
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "system.parts"):
			partRequests++
			_, _ = w.Write([]byte(activeParts[(partRequests-1)%len(activeParts)]))
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeSettleMinWait:  10 * time.Millisecond,
		MergeSettleMinParts: 1,
		MergePollInterval:   time.Millisecond,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForMerges error = %v, want context deadline exceeded", err)
	}
	if mergeRequests < 2 {
		t.Fatalf("merge requests = %d, want at least 2", mergeRequests)
	}
	if partRequests < 2 {
		t.Fatalf("part requests = %d, want at least 2", partRequests)
	}
}

func TestWaitForMergesRunsOptimizeFinalAfterIdle(t *testing.T) {
	var optimizeRequests int
	var optimizeQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "GROUP BY partition_id"):
			_, _ = w.Write([]byte("202401\t2\t0\t1073741824\n"))
		case strings.Contains(query, "system.parts"):
			if optimizeRequests == 0 {
				_, _ = w.Write([]byte(multiPartMergeSnapshot()))
			} else {
				_, _ = w.Write([]byte("1\t1073741824\t1073741824\n"))
			}
		case strings.HasPrefix(query, "OPTIMIZE TABLE "):
			optimizeRequests++
			optimizeQuery = query
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	result, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeTimeout:        time.Second,
		MergeMaxTimeout:     time.Second,
		MergeSettleMinWait:  time.Hour,
		MergeSettleMinParts: 1,
		MergePollInterval:   time.Millisecond,
		OptimizeFinalAfter:  2 * time.Millisecond,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Settled {
		t.Fatal("expected merge wait to settle after optimize final")
	}
	if result.ActiveParts != 1 {
		t.Fatalf("active parts = %d, want 1", result.ActiveParts)
	}
	if optimizeRequests != 1 {
		t.Fatalf("optimize requests = %d, want 1", optimizeRequests)
	}
	if want := "OPTIMIZE TABLE `db`.`query_log_archive_temp` PARTITION ID '202401' FINAL"; optimizeQuery != want {
		t.Fatalf("optimize query = %q, want %q", optimizeQuery, want)
	}
}

func TestWaitForMergesRunsOptimizeFinalOnceForStableSnapshot(t *testing.T) {
	var optimizeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "GROUP BY partition_id"):
			_, _ = w.Write([]byte("202401\t4\t0\t1073741824\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte(multiPartMergeSnapshot()))
		case strings.HasPrefix(query, "OPTIMIZE TABLE "):
			optimizeRequests++
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeTimeout:        time.Hour,
		MergeMaxTimeout:     time.Hour,
		MergeSettleMinWait:  time.Hour,
		MergeSettleMinParts: 1,
		MergePollInterval:   time.Millisecond,
		OptimizeFinalAfter:  time.Millisecond,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForMerges error = %v, want context deadline exceeded", err)
	}
	if optimizeRequests != 1 {
		t.Fatalf("optimize requests = %d, want 1", optimizeRequests)
	}
}

func TestWaitForMergesSkipsOptimizeFinalWhenPartsAreInDifferentPartitions(t *testing.T) {
	var partitionRequests int
	var optimizeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "GROUP BY partition_id"):
			partitionRequests++
			_, _ = w.Write([]byte("202401\t1\t0\t100\n202402\t1\t0\t100\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte("2\t200\t100\n"))
		case strings.HasPrefix(query, "OPTIMIZE TABLE "):
			optimizeRequests++
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeTimeout:        time.Hour,
		MergeMaxTimeout:     time.Hour,
		MergeSettleMinWait:  time.Hour,
		MergeSettleMinParts: 1,
		MergePollInterval:   time.Millisecond,
		OptimizeFinalAfter:  time.Millisecond,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForMerges error = %v, want context deadline exceeded", err)
	}
	if partitionRequests != 1 {
		t.Fatalf("partition requests = %d, want 1", partitionRequests)
	}
	if optimizeRequests != 0 {
		t.Fatalf("optimize requests = %d, want 0", optimizeRequests)
	}
}

func TestWaitForMergesSkipsOptimizeFinalWhenPartitionExceedsTargetPartBytes(t *testing.T) {
	var partitionRequests int
	var optimizeRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		query := string(body)
		switch {
		case strings.Contains(query, "system.merges"):
			_, _ = w.Write([]byte("0\n"))
		case strings.Contains(query, "GROUP BY partition_id"):
			partitionRequests++
			_, _ = w.Write([]byte("202401\t2\t0\t214748364800\n"))
		case strings.Contains(query, "system.parts"):
			_, _ = w.Write([]byte("2\t214748364800\t107374182400\n"))
		case strings.HasPrefix(query, "OPTIMIZE TABLE "):
			optimizeRequests++
		default:
			t.Errorf("unexpected query: %s", query)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := (Processor{
		ClickHouse:          chhttp.Client{URL: server.URL},
		MergeTimeout:        time.Hour,
		MergeMaxTimeout:     time.Hour,
		MergeSettleMinWait:  time.Hour,
		MergeSettleMinParts: 1,
		MergePollInterval:   time.Millisecond,
		OptimizeFinalAfter:  time.Millisecond,
	}).waitForMerges(ctx, testMergeWaitTarget())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForMerges error = %v, want context deadline exceeded", err)
	}
	if partitionRequests != 1 {
		t.Fatalf("partition requests = %d, want 1", partitionRequests)
	}
	if optimizeRequests != 0 {
		t.Fatalf("optimize requests = %d, want 0", optimizeRequests)
	}
}

func TestDestinationMergeCountFiltersToTargetTable(t *testing.T) {
	var mergeQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mergeQuery = string(body)
		_, _ = w.Write([]byte("0\n"))
	}))
	defer server.Close()

	count, err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).destinationMergeCount(context.Background(), testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("merge count = %d, want 0", count)
	}
	for _, want := range []string{
		"FROM system.merges",
		"database = 'db'",
		"table = 'query_log_archive_temp'",
	} {
		if !strings.Contains(mergeQuery, want) {
			t.Fatalf("merge query = %q, missing %q", mergeQuery, want)
		}
	}
}

func TestMergePartSnapshotUsesBytesOnDisk(t *testing.T) {
	var partQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		partQuery = string(body)
		_, _ = w.Write([]byte(multiPartMergeSnapshot()))
	}))
	defer server.Close()

	snapshot, err := (Processor{
		ClickHouse: chhttp.Client{URL: server.URL},
	}).mergePartSnapshot(context.Background(), testMergeWaitTarget())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.TotalBytes == 0 {
		t.Fatal("expected snapshot bytes")
	}
	for _, want := range []string{
		"sum(bytes_on_disk)",
		"max(bytes_on_disk)",
	} {
		if !strings.Contains(partQuery, want) {
			t.Fatalf("part query = %q, missing %q", partQuery, want)
		}
	}
	for _, notWant := range []string{
		"countIf(",
		"sumIf(",
	} {
		if strings.Contains(partQuery, notWant) {
			t.Fatalf("part query = %q, should not contain %q", partQuery, notWant)
		}
	}
}

func multiPartMergeSnapshot() string {
	return "4\t1073741824\t268435456\n"
}

func largeTailMergeSnapshot() string {
	return "2\t10737418240\t5368709120\n"
}

func testMergeWaitTarget() mergeWaitTarget {
	return mergeWaitTarget{
		JobID:    "job-1",
		PartID:   "part-1",
		Database: "db",
		Table:    "query_log_archive_temp",
	}
}

func TestDefaultMergeTimeout(t *testing.T) {
	if DefaultMergeTimeout != 5*time.Minute {
		t.Fatalf("DefaultMergeTimeout = %s, want 5m", DefaultMergeTimeout)
	}
	if DefaultMergeMaxTimeout != time.Hour {
		t.Fatalf("DefaultMergeMaxTimeout = %s, want 1h", DefaultMergeMaxTimeout)
	}
	if DefaultMergeSettleMinWait != time.Minute {
		t.Fatalf("DefaultMergeSettleMinWait = %s, want 1m", DefaultMergeSettleMinWait)
	}
	if DefaultCompactMergeTimeout != 15*time.Minute {
		t.Fatalf("DefaultCompactMergeTimeout = %s, want 15m", DefaultCompactMergeTimeout)
	}
	if DefaultCompactMergeMaxTimeout != 24*time.Hour {
		t.Fatalf("DefaultCompactMergeMaxTimeout = %s, want 24h", DefaultCompactMergeMaxTimeout)
	}
	if DefaultCompactMergeSettleMinWait != 2*time.Minute {
		t.Fatalf("DefaultCompactMergeSettleMinWait = %s, want 2m", DefaultCompactMergeSettleMinWait)
	}
	if DefaultCompactOptimizeFinalAfter != 30*time.Second {
		t.Fatalf("DefaultCompactOptimizeFinalAfter = %s, want 30s", DefaultCompactOptimizeFinalAfter)
	}
	if DefaultMergeSettleMinParts != 1 {
		t.Fatalf("DefaultMergeSettleMinParts = %d, want 1", DefaultMergeSettleMinParts)
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

func TestProgressHeartbeatReportsImmediatelyAndOnInterval(t *testing.T) {
	reports := make(chan manifest.Manifest, 4)
	processor := Processor{
		ProgressInterval: time.Millisecond,
		ReportProgress: func(ctx context.Context, m manifest.Manifest, snapshot ProgressSnapshot) error {
			if snapshot.QueryProgress != nil || snapshot.SourceActivePartStats != nil || snapshot.DestinationActivePartStats != nil {
				t.Errorf("heartbeat snapshot = %+v, want only stage progress", snapshot)
			}
			if snapshot.StageProgress == nil || snapshot.StageProgress.Stage != stageProcessPart {
				t.Errorf("stage progress = %+v, want %s", snapshot.StageProgress, stageProcessPart)
			}
			select {
			case reports <- m:
			default:
			}
			return nil
		},
	}

	tracker := newRewriteStageTracker(time.Now(), stageProcessPart)
	heartbeat, err := processor.startProgressHeartbeat(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"}, tracker)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := heartbeat.Stop(); err != nil {
			t.Fatal(err)
		}
	})

	for i := 0; i < 2; i++ {
		select {
		case got := <-reports:
			if got.JobID != "job-1" || got.PartID != "part-1" {
				t.Fatalf("heartbeat manifest = %+v", got)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for heartbeat report")
		}
	}
}

func TestRewriteStageTrackerDurations(t *testing.T) {
	started := time.Unix(100, 0)
	tracker := newRewriteStageTracker(started, stageProcessPart)

	progress := tracker.Start(stageDownloadSource, started.Add(2*time.Second))
	if progress.Stage != stageDownloadSource {
		t.Fatalf("stage = %s, want %s", progress.Stage, stageDownloadSource)
	}
	if got := progress.CompletedStageDurations[stageProcessPart]; got != 2*time.Second {
		t.Fatalf("process_part duration = %s, want 2s", got)
	}

	progress = tracker.Snapshot(started.Add(5 * time.Second))
	if progress.StageElapsed != 3*time.Second {
		t.Fatalf("stage elapsed = %s, want 3s", progress.StageElapsed)
	}
	if progress.TotalElapsed != 5*time.Second {
		t.Fatalf("total elapsed = %s, want 5s", progress.TotalElapsed)
	}

	progress = tracker.Complete(stageCompletePart, started.Add(7*time.Second))
	if progress.Stage != stageCompletePart {
		t.Fatalf("stage = %s, want %s", progress.Stage, stageCompletePart)
	}
	if got := progress.CompletedStageDurations[stageDownloadSource]; got != 5*time.Second {
		t.Fatalf("download_source duration = %s, want 5s", got)
	}
}

func TestProgressHeartbeatDisabled(t *testing.T) {
	called := false
	processor := Processor{
		ProgressInterval: 0,
		ReportProgress: func(ctx context.Context, m manifest.Manifest, snapshot ProgressSnapshot) error {
			called = true
			return nil
		},
	}

	heartbeat, err := processor.startProgressHeartbeat(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("expected disabled heartbeat not to report progress")
	}
}

func TestProgressHeartbeatReportFailureContinues(t *testing.T) {
	reportErr := errors.New("progress update failed")
	attempts := make(chan struct{}, 8)
	reports := 0
	processor := Processor{
		ProgressInterval: time.Millisecond,
		ReportProgress: func(ctx context.Context, m manifest.Manifest, snapshot ProgressSnapshot) error {
			reports++
			attempts <- struct{}{}
			if reports == 2 {
				return reportErr
			}
			return nil
		},
	}

	tracker := newRewriteStageTracker(time.Now(), stageProcessPart)
	heartbeat, err := processor.startProgressHeartbeat(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"}, tracker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := heartbeat.Stop(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 4; i++ {
		select {
		case <-attempts:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for heartbeat report")
		}
	}
	select {
	case <-heartbeat.Context().Done():
		t.Fatal("heartbeat context was canceled by report failure")
	default:
	}
}

func TestProgressHeartbeatStopIgnoresInFlightContextCancellation(t *testing.T) {
	inFlight := make(chan struct{})
	reports := 0
	processor := Processor{
		ProgressInterval: time.Millisecond,
		ReportProgress: func(ctx context.Context, m manifest.Manifest, snapshot ProgressSnapshot) error {
			reports++
			if reports == 1 {
				return nil
			}
			close(inFlight)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	tracker := newRewriteStageTracker(time.Now(), stageProcessPart)
	heartbeat, err := processor.startProgressHeartbeat(context.Background(), manifest.Manifest{JobID: "job-1", PartID: "part-1"}, tracker)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-inFlight:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight heartbeat report")
	}
	if err := heartbeat.Stop(); err != nil {
		t.Fatalf("heartbeat stop error = %v, want nil", err)
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

func TestUploadFinishedArtifactReplacesStablePartPrefixWithTarballs(t *testing.T) {
	binary, logFile := fakeS5cmdRecorder(t)
	frozenStore := filepath.Join(t.TempDir(), "shadow", "freeze", "store")
	createFrozenPart(t, filepath.Join(frozenStore, "abc", "def", "all_1_1_0"))
	createFrozenPart(t, filepath.Join(frozenStore, "abc", "def", "all_2_2_0"))
	frozenGlob := filepath.Join(frozenStore, "*", "*", "*")
	finishedKey := "partforge/jobs/job-1/finished/part-1"
	tarDir := filepath.Join(t.TempDir(), "finished-tars")

	err := (Processor{
		S3Copy: s3copy.Copier{Binary: binary},
	}).uploadFinishedArtifact(context.Background(), manifest.Manifest{
		JobID:  "job-1",
		PartID: "part-1",
		S3: manifest.S3Refs{
			Bucket:      "bucket",
			FinishedKey: finishedKey,
		},
	}, tarDir, []frozenPartGlob{
		{Disk: "default", Glob: frozenGlob},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("s5cmd calls = %#v, want delete then upload", lines)
	}
	if !strings.Contains(lines[0], " rm s3://bucket/"+finishedKey+"/*") {
		t.Fatalf("delete call = %q, want finished part prefix delete", lines[0])
	}
	if !strings.Contains(lines[1], " cp "+tarDir+string(filepath.Separator)+" s3://bucket/"+finishedKey+"/") {
		t.Fatalf("upload call = %q, want finished tarball directory upload", lines[1])
	}
	for _, line := range lines {
		if strings.Contains(line, "/data/") || strings.Contains(line, "/attempt-") {
			t.Fatalf("s5cmd call uses old finished layout: %q", line)
		}
	}

	tarEntries, err := os.ReadDir(tarDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(tarEntries) != 2 {
		t.Fatalf("tarball count = %d, want 2", len(tarEntries))
	}
	extractRoot := filepath.Join(t.TempDir(), "extract")
	parts, err := artifact.ExtractFinishedTar(filepath.Join(tarDir, "all_1_1_0.tar"), extractRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || parts[0] != "all_1_1_0" {
		t.Fatalf("extracted parts = %#v, want all_1_1_0", parts)
	}
}

func TestUploadFinishedArtifactRequiresFrozenPartGlobs(t *testing.T) {
	err := (Processor{}).uploadFinishedArtifact(context.Background(), manifest.Manifest{
		JobID:  "job-1",
		PartID: "part-1",
		S3: manifest.S3Refs{
			Bucket:      "bucket",
			FinishedKey: "partforge/jobs/job-1/finished/part-1",
		},
	}, filepath.Join(t.TempDir(), "finished-tars"), nil, nil)
	if err == nil {
		t.Fatal("expected missing frozen part globs error")
	}
	if !strings.Contains(err.Error(), "no frozen part globs") {
		t.Fatalf("error = %q, want missing globs", err)
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

func fakeS5cmdRecorder(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	logFile := filepath.Join(dir, "calls")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + shellQuote(logFile) + "\n" +
		"exit 0\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, logFile
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
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

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsQueryWith(values []string, want string) bool {
	for _, value := range values {
		if strings.Contains(value, want) {
			return true
		}
	}
	return false
}

func countQueriesWith(values []string, want string) int {
	count := 0
	for _, value := range values {
		if strings.Contains(value, want) {
			count++
		}
	}
	return count
}
