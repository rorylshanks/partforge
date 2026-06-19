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
		queries = append(queries, string(body))
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
	want := "ALTER TABLE `db`.`query_log_archive_temp` MODIFY SETTING merge_max_block_size = 32768, merge_max_block_size_bytes = 67108864, merge_selecting_sleep_ms = 1000"
	if len(queries) != 1 || queries[0] != want {
		t.Fatalf("queries = %#v, want %q", queries, want)
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
			MergeMaxBlockSize:      32768,
			MergeMaxBlockSizeBytes: 67108864,
			MergeSelectingSleepMS:  1000,
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

	want := "ALTER TABLE `db`.`query_log_archive_temp` MODIFY SETTING merge_max_block_size = 32768, merge_max_block_size_bytes = 67108864, merge_selecting_sleep_ms = 1000"
	if containsString(queries, want) {
		t.Fatalf("queries = %#v, did not expect merge settings during insert retry", queries)
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

func TestOptimizeFinal(t *testing.T) {
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

	err := (Processor{ClickHouse: chhttp.Client{URL: server.URL}}).optimizeFinal(context.Background(), manifest.Manifest{
		Dest: manifest.TableRef{Database: "db", Table: "query_log_archive_temp"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "OPTIMIZE TABLE `db`.`query_log_archive_temp` FINAL"
	if len(queries) != 1 || queries[0] != want {
		t.Fatalf("queries = %#v, want %q", queries, want)
	}
}

func TestShouldOptimizeFinal(t *testing.T) {
	if (Processor{}).shouldOptimizeFinal(manifest.Manifest{}) {
		t.Fatal("expected optimize final to be disabled by default")
	}
	if !(Processor{}).shouldOptimizeFinal(manifest.Manifest{Options: manifest.Options{OptimizeFinal: true}}) {
		t.Fatal("expected manifest option to enable optimize final")
	}
	if !(Processor{ForceOptimizeFinal: true}).shouldOptimizeFinal(manifest.Manifest{}) {
		t.Fatal("expected worker override to enable optimize final")
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
			_, _ = w.Write([]byte("4\n"))
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
	}).waitForMerges(ctx, "db", "query_log_archive_temp")
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

func TestDefaultMergeTimeout(t *testing.T) {
	if DefaultMergeTimeout != 10*time.Minute {
		t.Fatalf("DefaultMergeTimeout = %s, want 10m", DefaultMergeTimeout)
	}
	if DefaultMergeSettleMinWait != 2*time.Minute {
		t.Fatalf("DefaultMergeSettleMinWait = %s, want 2m", DefaultMergeSettleMinWait)
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

func TestProgressHeartbeatReportFailureCancelsContext(t *testing.T) {
	reportErr := errors.New("progress update failed")
	reports := 0
	processor := Processor{
		ProgressInterval: time.Millisecond,
		ReportProgress: func(ctx context.Context, m manifest.Manifest, snapshot ProgressSnapshot) error {
			reports++
			if reports > 1 {
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

	select {
	case <-heartbeat.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat context cancellation")
	}
	if err := heartbeat.Stop(); !errors.Is(err, reportErr) {
		t.Fatalf("heartbeat stop error = %v, want %v", err, reportErr)
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
