package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/metrics"
	"github.com/partforge/partforge/internal/rewrite"
	"github.com/partforge/partforge/internal/state"
)

func TestSummarizeJob(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{PartID: "part-1", Status: state.StatusImported, ReadRows: 10, ReadBytes: 100, WrittenRows: 9, WrittenBytes: 90, DestinationFailedMerges: 1},
		{PartID: "part-2", Status: state.StatusFinished, ReadRows: 20, ReadBytes: 200, WrittenRows: 19, WrittenBytes: 190, DestinationFailedMerges: 2},
		{PartID: "part-3", Status: state.StatusFailed, Error: "boom"},
	})

	if summary.Status != "FAILED" {
		t.Fatalf("status = %s", summary.Status)
	}
	if summary.Total != 3 {
		t.Fatalf("total = %d", summary.Total)
	}
	if summary.RewriteCompleted != 2 {
		t.Fatalf("rewrite completed = %d", summary.RewriteCompleted)
	}
	if summary.ImportCompleted != 1 {
		t.Fatalf("import completed = %d", summary.ImportCompleted)
	}
	if summary.ReadRows != 30 || summary.ReadBytes != 300 || summary.WrittenRows != 28 || summary.WrittenBytes != 280 {
		t.Fatalf("summary progress = %+v", summary)
	}
	if summary.FailedMerges != 3 {
		t.Fatalf("failed merges = %d, want 3", summary.FailedMerges)
	}
	if len(summary.FailedParts) != 1 || summary.FailedParts[0].PartID != "part-3" {
		t.Fatalf("failed parts = %+v", summary.FailedParts)
	}
}

func TestSummarizeJobCountsInProgressStages(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{PartID: "part-1", Status: state.StatusInProgress, RewriteStage: "wait_merges"},
		{PartID: "part-2", Status: state.StatusInProgress, RewriteStage: "insert_select"},
		{PartID: "part-3", Status: state.StatusInProgress, RewriteStage: "insert_select"},
		{PartID: "part-4", Status: state.StatusInProgress},
		{PartID: "part-5", Status: state.StatusFinished, RewriteStage: "insert_select"},
	})

	want := []inProgressStageCount{
		{Stage: "insert_select", Count: 2},
		{Stage: "wait_merges", Count: 1},
		{Stage: "unknown", Count: 1},
	}
	if len(summary.InProgressStages) != len(want) {
		t.Fatalf("in-progress stages = %+v, want %+v", summary.InProgressStages, want)
	}
	for i := range want {
		if summary.InProgressStages[i] != want[i] {
			t.Fatalf("in-progress stage %d = %+v, want %+v", i, summary.InProgressStages[i], want[i])
		}
	}
}

func TestSelectRetryParts(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusFailed},
		{PartID: "part-2", Status: state.StatusImported},
		{PartID: "part-3", Status: state.StatusFailed},
		{PartID: "part-4", Status: state.StatusInProgress},
		{PartID: "part-5", Status: state.StatusFinished},
	}

	all, err := selectRetryParts(parts, retryPartSelection{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all len = %d", len(all))
	}

	allWithInProgress, err := selectRetryParts(parts, retryPartSelection{All: true, IncludeInProgress: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(allWithInProgress) != 3 || allWithInProgress[2].PartID != "part-4" {
		t.Fatalf("all with in-progress = %+v", allWithInProgress)
	}

	forced, err := selectRetryParts(parts, retryPartSelection{All: true, Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(forced) != 5 {
		t.Fatalf("forced len = %d", len(forced))
	}

	forcedOne, err := selectRetryParts(parts, retryPartSelection{Force: true, PartID: "part-2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(forcedOne) != 1 || forcedOne[0].PartID != "part-2" {
		t.Fatalf("forced one = %+v", forcedOne)
	}

	one, err := selectRetryParts(parts, retryPartSelection{PartID: "part-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].PartID != "part-1" {
		t.Fatalf("one = %+v", one)
	}

	inProgress, err := selectRetryParts(parts, retryPartSelection{IncludeInProgress: true, PartID: "part-4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(inProgress) != 1 || inProgress[0].PartID != "part-4" {
		t.Fatalf("in-progress = %+v", inProgress)
	}

	if _, err := selectRetryParts(parts, retryPartSelection{PartID: "part-2"}); err == nil {
		t.Fatal("expected non-failed part error")
	}

	if _, err := selectRetryParts(parts, retryPartSelection{IncludeInProgress: true, PartID: "part-5"}); err == nil {
		t.Fatal("expected completed part error")
	}
}

func TestSelectImportFinishedParts(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusFinished},
		{PartID: "part-2", Status: state.StatusFinished},
	}

	all, err := selectImportFinishedParts(parts, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all parts len = %d, want 2", len(all))
	}

	one, err := selectImportFinishedParts(parts, "part-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].PartID != "part-2" {
		t.Fatalf("selected part = %+v, want part-2", one)
	}

	if _, err := selectImportFinishedParts(parts, "part-missing"); err == nil {
		t.Fatal("expected missing part error")
	}
}

func TestSelectRetryPartsStale(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	parts := []state.Part{
		{PartID: "part-failed", Status: state.StatusFailed},
		{PartID: "part-fresh", Status: state.StatusInProgress, ProgressUpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano)},
		{PartID: "part-stale", Status: state.StatusInProgress, ProgressUpdatedAt: now.Add(-6 * time.Minute).Format(time.RFC3339Nano)},
		{PartID: "part-empty", Status: state.StatusInProgress},
		{PartID: "part-exact", Status: state.StatusInProgress, ProgressUpdatedAt: now.Add(-5 * time.Minute).Format(time.RFC3339Nano)},
	}

	selected, err := selectRetryParts(parts, retryPartSelection{
		Stale:      true,
		StaleAfter: 5 * time.Minute,
		Now:        now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].PartID != "part-stale" {
		t.Fatalf("stale selected = %+v", selected)
	}
}

func TestSelectRetryPartsStaleRejectsMalformedProgressTime(t *testing.T) {
	_, err := selectRetryParts([]state.Part{
		{PartID: "part-bad", Status: state.StatusInProgress, ProgressUpdatedAt: "not-time"},
	}, retryPartSelection{
		Stale:      true,
		StaleAfter: 5 * time.Minute,
		Now:        time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("expected malformed progress_updated_at error")
	}
}

func TestJobS3Prefixes(t *testing.T) {
	prefixes, err := jobS3Prefixes("job-1", []state.Part{
		{
			JobID:       "job-1",
			PartID:      "part-1",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-1/source/part-1",
			FinishedKey: "partforge/jobs/job-1/finished/part-1",
		},
		{
			JobID:       "job-1",
			PartID:      "part-2",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-1/source/part-2",
			FinishedKey: "partforge/jobs/job-1/finished/part-2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixes) != 1 {
		t.Fatalf("prefixes = %+v", prefixes)
	}
	if prefixes[0].Bucket != "bucket" || prefixes[0].Prefix != "partforge/jobs/job-1" {
		t.Fatalf("prefix = %+v", prefixes[0])
	}
}

func TestJobS3PrefixesRejectsWrongJobKey(t *testing.T) {
	_, err := jobS3Prefixes("job-1", []state.Part{
		{
			JobID:       "job-1",
			PartID:      "part-1",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-10/source/part-1",
			FinishedKey: "partforge/jobs/job-10/finished/part-1",
		},
	})
	if err == nil {
		t.Fatal("expected wrong job key error")
	}
}

func TestStateProgress(t *testing.T) {
	query := metrics.QueryProgress{ReadRows: 1, ReadBytes: 2, WrittenRows: 3, WrittenBytes: 4}
	source := metrics.PartStats{Count: 5, Rows: 6, Bytes: 7}
	dest := metrics.PartStats{Count: 8, Rows: 9, Bytes: 10}
	failedMerges := uint64(11)
	stageStartedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)

	progress := stateProgress(rewrite.ProgressSnapshot{
		QueryProgress:              &query,
		SourceActivePartStats:      &source,
		DestinationActivePartStats: &dest,
		DestinationFailedMerges:    &failedMerges,
		StageProgress: &rewrite.StageProgress{
			Stage:          "download_source",
			StageStartedAt: stageStartedAt,
			StageElapsed:   1500 * time.Millisecond,
			TotalElapsed:   2500 * time.Millisecond,
			CompletedStageDurations: map[string]time.Duration{
				"process_part": time.Second,
			},
		},
	})

	if progress.QueryProgress == nil || progress.QueryProgress.WrittenBytes != 4 {
		t.Fatalf("query progress = %+v", progress.QueryProgress)
	}
	if progress.SourceActivePartStats == nil || progress.SourceActivePartStats.Rows != 6 {
		t.Fatalf("source stats = %+v", progress.SourceActivePartStats)
	}
	if progress.DestinationActivePartStats == nil || progress.DestinationActivePartStats.Bytes != 10 {
		t.Fatalf("destination stats = %+v", progress.DestinationActivePartStats)
	}
	if progress.DestinationFailedMerges == nil || *progress.DestinationFailedMerges != failedMerges {
		t.Fatalf("destination failed merges = %+v", progress.DestinationFailedMerges)
	}
	if progress.StageProgress == nil || progress.StageProgress.Stage != "download_source" {
		t.Fatalf("stage progress = %+v", progress.StageProgress)
	}
	if progress.StageProgress.StageStartedAt != stageStartedAt {
		t.Fatalf("stage started at = %s, want %s", progress.StageProgress.StageStartedAt, stageStartedAt)
	}
	if progress.StageProgress.StageElapsedMs != 1500 || progress.StageProgress.TotalElapsedMs != 2500 {
		t.Fatalf("stage durations = %+v", progress.StageProgress)
	}
	if progress.StageProgress.CompletedStageDurationsMs["process_part"] != 1000 {
		t.Fatalf("completed stage durations = %+v", progress.StageProgress.CompletedStageDurationsMs)
	}
}

func TestFormatStageDurations(t *testing.T) {
	got := formatStageDurations(map[string]int64{
		"b": 2000,
		"a": 1500,
	})
	if got != "a=1.5s,b=2s" {
		t.Fatalf("formatStageDurations = %q", got)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{bytes: 0, want: "0 B"},
		{bytes: 300, want: "300 B"},
		{bytes: 1536, want: "1.5 KB"},
		{bytes: 10 * 1024 * 1024, want: "10 MB"},
		{bytes: 5*1024*1024*1024 + 512*1024*1024, want: "5.5 GB"},
	}

	for _, tt := range tests {
		if got := formatBytes(tt.bytes); got != tt.want {
			t.Fatalf("formatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestFormatByteRate(t *testing.T) {
	tests := []struct {
		bytesPerSecond float64
		want           string
	}{
		{bytesPerSecond: 0, want: "0 B/s"},
		{bytesPerSecond: 1536, want: "1.5 KB/s"},
		{bytesPerSecond: 10 * 1024 * 1024, want: "10 MB/s"},
		{bytesPerSecond: 1.5 * 1024 * 1024 * 1024, want: "1.5 GB/s"},
	}

	for _, tt := range tests {
		if got := formatByteRate(tt.bytesPerSecond); got != tt.want {
			t.Fatalf("formatByteRate(%f) = %q, want %q", tt.bytesPerSecond, got, tt.want)
		}
	}
}

func TestHumanizeLogAttrHumanizesByteFields(t *testing.T) {
	tests := []struct {
		attr slog.Attr
		want string
	}{
		{attr: slog.Uint64("uploaded_bytes", 10*1024*1024), want: "10 MB"},
		{attr: slog.Float64("overall_bytes_per_second", 1.5*1024*1024), want: "1.5 MB/s"},
		{attr: slog.String("max_memory_usage", "1073741824"), want: "1 GB"},
		{attr: slog.Int("bytes", 1536), want: "1.5 KB"},
	}

	for _, tt := range tests {
		got := humanizeLogAttr(nil, tt.attr)
		if got.Value.String() != tt.want {
			t.Fatalf("humanizeLogAttr(%s) = %q, want %q", tt.attr.Key, got.Value.String(), tt.want)
		}
	}
}

func TestHumanizeLogAttrLeavesOtherNumbersRaw(t *testing.T) {
	got := humanizeLogAttr(nil, slog.Int("attempt", 7))
	if got.Value.Kind() != slog.KindInt64 || got.Value.Int64() != 7 {
		t.Fatalf("humanizeLogAttr changed non-byte number: %+v", got)
	}
}

func TestPrintJobSummaryHumanizesBytes(t *testing.T) {
	summary := jobSummary{
		JobID:        "job-1",
		Status:       "READY",
		Total:        1,
		Counts:       map[state.Status]int{state.StatusReady: 1},
		ReadRows:     1000,
		ReadBytes:    10 * 1024 * 1024,
		WrittenRows:  900,
		WrittenBytes: 3 * 1024 * 1024 * 1024,
		FailedMerges: 4,
	}

	got := captureFileOutput(t, func(out *os.File) {
		printJobSummary(out, summary)
	})

	for _, want := range []string{
		"read: 1000 rows 10 MB",
		"written: 900 rows 3 GB",
		"failed_merges: 4",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printJobSummary output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "10485760 bytes") || strings.Contains(got, "3221225472 bytes") {
		t.Fatalf("printJobSummary output contains raw byte values:\n%s", got)
	}
}

func TestPrintJobSummaryIncludesInProgressStages(t *testing.T) {
	summary := jobSummary{
		JobID:  "job-1",
		Status: "REWRITING",
		Total:  3,
		Counts: map[state.Status]int{
			state.StatusInProgress: 3,
		},
		InProgressStages: []inProgressStageCount{
			{Stage: "insert_select", Count: 2},
			{Stage: "wait_merges", Count: 1},
		},
	}

	got := captureFileOutput(t, func(out *os.File) {
		printJobSummary(out, summary)
	})

	for _, want := range []string{
		"IN_PROGRESS STAGES",
		"insert_select",
		"wait_merges",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printJobSummary output missing %q:\n%s", want, got)
		}
	}
}

func TestPrintPartRowsHumanizesBytes(t *testing.T) {
	got := captureFileOutput(t, func(out *os.File) {
		printPartRows(out, []state.Part{
			{
				PartID:                     "part-1",
				Status:                     state.StatusFinished,
				ReadRows:                   100,
				ReadBytes:                  1536,
				WrittenRows:                90,
				WrittenBytes:               2 * 1024 * 1024,
				DestinationActivePartCount: 3,
				DestinationFailedMerges:    4,
				RewriteStageDurationsMs: map[string]int64{
					"wait_merges": 65_000,
				},
			},
		})
	})

	for _, want := range []string{
		"READ_SIZE",
		"WRITTEN_SIZE",
		"OUTPUT_PARTS",
		"FAILED_MERGES",
		"SETTLE_WAIT",
		"1.5 KB",
		"2 MB",
		"4",
		"3",
		"1m5s",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printPartRows output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "READ_BYTES") || strings.Contains(got, "WRITTEN_BYTES") {
		t.Fatalf("printPartRows output still uses raw byte headers:\n%s", got)
	}
}

func TestPrintPartRowsUsesCurrentWaitMergesElapsed(t *testing.T) {
	got := captureFileOutput(t, func(out *os.File) {
		printPartRows(out, []state.Part{
			{
				PartID:                "part-1",
				Status:                state.StatusInProgress,
				RewriteStage:          "wait_merges",
				RewriteStageElapsedMs: 125_000,
			},
		})
	})

	if !strings.Contains(got, "2m5s") {
		t.Fatalf("printPartRows output missing live settle wait:\n%s", got)
	}
}

func TestUploadPartsInParallelProcessesEveryTask(t *testing.T) {
	tasks := []uploadPartTask{
		{Index: 1, SourcePart: freeze.Part{Name: "part-1"}},
		{Index: 2, SourcePart: freeze.Part{Name: "part-2"}},
		{Index: 3, SourcePart: freeze.Part{Name: "part-3"}},
		{Index: 4, SourcePart: freeze.Part{Name: "part-4"}},
		{Index: 5, SourcePart: freeze.Part{Name: "part-5"}},
	}

	var active int64
	var maxActive int64
	seen := map[int]bool{}
	var seenMu sync.Mutex

	err := uploadPartsInParallel(context.Background(), tasks, 2, func(ctx context.Context, workerID int, task uploadPartTask) (uploadPartResult, error) {
		current := atomic.AddInt64(&active, 1)
		for {
			max := atomic.LoadInt64(&maxActive)
			if current <= max || atomic.CompareAndSwapInt64(&maxActive, max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		return uploadPartResult{Index: task.Index, SourcePart: task.SourcePart}, nil
	}, func(result uploadPartResult) {
		seenMu.Lock()
		defer seenMu.Unlock()
		seen[result.Index] = true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(tasks) {
		t.Fatalf("processed %d tasks, want %d", len(seen), len(tasks))
	}
	if max := atomic.LoadInt64(&maxActive); max > 2 {
		t.Fatalf("max active uploads = %d, want <= 2", max)
	}
}

func TestUploadPartsInParallelCancelsOnError(t *testing.T) {
	boom := errors.New("boom")
	tasks := []uploadPartTask{
		{Index: 1},
		{Index: 2},
		{Index: 3},
	}

	err := uploadPartsInParallel(context.Background(), tasks, 2, func(ctx context.Context, workerID int, task uploadPartTask) (uploadPartResult, error) {
		if task.Index == 1 {
			return uploadPartResult{}, boom
		}
		<-ctx.Done()
		return uploadPartResult{}, ctx.Err()
	}, nil)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want %v", err, boom)
	}
}

func TestResolveS5cmdNumWorkers(t *testing.T) {
	tests := []struct {
		name              string
		configured        int
		uploadConcurrency int
		want              int
	}{
		{name: "explicit", configured: 17, uploadConcurrency: 4, want: 17},
		{name: "auto divides default workers", configured: 0, uploadConcurrency: 4, want: 64},
		{name: "auto clamps concurrency", configured: 0, uploadConcurrency: 0, want: 256},
		{name: "auto clamps workers", configured: 0, uploadConcurrency: 512, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveS5cmdNumWorkers(tt.configured, tt.uploadConcurrency)
			if got != tt.want {
				t.Fatalf("workers = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCreateWorkerRunDirs(t *testing.T) {
	workDir := t.TempDir()
	dirs, err := createWorkerRunDirs(workDir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(dirs.Root) != workDir {
		t.Fatalf("run dir parent = %q, want %q", filepath.Dir(dirs.Root), workDir)
	}
	if filepath.Base(dirs.ClickHouse) != "clickhouse" || filepath.Dir(dirs.ClickHouse) != dirs.Root {
		t.Fatalf("clickhouse dir = %q, want child of %q", dirs.ClickHouse, dirs.Root)
	}
	if filepath.Base(dirs.Scratch) != "scratch" || filepath.Dir(dirs.Scratch) != dirs.Root {
		t.Fatalf("scratch dir = %q, want child of %q", dirs.Scratch, dirs.Root)
	}
	for _, dir := range []string{dirs.Root, dirs.ClickHouse, dirs.Scratch} {
		if info, err := os.Stat(dir); err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		} else if !info.IsDir() {
			t.Fatalf("%s is not a directory", dir)
		}
	}
}

func TestWorkerProcessContextWaitsUntilGracePeriodExpires(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	processCtx, shutdown := workerProcessContext(shutdownCtx, 50*time.Millisecond, "job-1", "part-1")
	defer shutdown.Stop()

	cancelShutdown()
	waitForShutdownRequested(t, shutdown)

	select {
	case <-processCtx.Done():
		t.Fatal("process context canceled before grace period expired")
	case <-time.After(10 * time.Millisecond):
	}
	if shutdown.Forced() {
		t.Fatal("shutdown forced before grace period expired")
	}

	select {
	case <-processCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("process context was not canceled after grace period expired")
	}
	if !shutdown.Forced() {
		t.Fatal("shutdown was not marked forced after grace period expired")
	}
}

func TestWorkerProcessContextStopBeforeGracePeriodExpires(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	processCtx, shutdown := workerProcessContext(shutdownCtx, time.Second, "job-1", "part-1")

	cancelShutdown()
	waitForShutdownRequested(t, shutdown)
	shutdown.Stop()

	select {
	case <-processCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("process context was not canceled after stop")
	}
	if shutdown.Forced() {
		t.Fatal("shutdown marked forced after worker stopped during grace period")
	}
}

func TestWorkerProcessContextCancelsImmediatelyWithoutGracePeriod(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	processCtx, shutdown := workerProcessContext(shutdownCtx, 0, "job-1", "part-1")
	defer shutdown.Stop()

	cancelShutdown()

	select {
	case <-processCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("process context was not canceled")
	}
	if !shutdown.Forced() {
		t.Fatal("shutdown was not marked forced")
	}
}

func waitForShutdownRequested(t *testing.T, shutdown workerPartShutdown) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if shutdown.Requested() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("shutdown was not requested")
}

func TestUploadConcurrencyFromCPUs(t *testing.T) {
	got, err := uploadConcurrencyFromCPUs(12)
	if err != nil {
		t.Fatal(err)
	}
	if got != 12 {
		t.Fatalf("concurrency = %d, want 12", got)
	}

	if _, err := uploadConcurrencyFromCPUs(0); err == nil {
		t.Fatal("expected invalid CPU count error")
	}
}

func TestApplyConfigDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeTestFile(t, path, `{
  "bucket": "global-bucket",
  "aws_region": "us-west-2",
  "commands": {
    "upload-freeze": {
      "bucket": "upload-bucket",
      "prefix": "uploads"
    }
  }
}`)

	fs := flag.NewFlagSet("upload-freeze", flag.ContinueOnError)
	bucket := fs.String("bucket", "", "")
	prefix := fs.String("prefix", "partforge", "")
	region := fs.String("aws-region", "", "")
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigDefaults(fs, path, "upload-freeze"); err != nil {
		t.Fatal(err)
	}

	if *bucket != "upload-bucket" {
		t.Fatalf("bucket = %q", *bucket)
	}
	if *prefix != "uploads" {
		t.Fatalf("prefix = %q", *prefix)
	}
	if *region != "us-west-2" {
		t.Fatalf("region = %q", *region)
	}
}

func TestApplyConfigDefaultsDoesNotOverrideCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeTestFile(t, path, `{"bucket": "config-bucket"}`)

	fs := flag.NewFlagSet("upload-freeze", flag.ContinueOnError)
	bucket := fs.String("bucket", "", "")
	if err := fs.Parse([]string{"-bucket=cli-bucket"}); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigDefaults(fs, path, "upload-freeze"); err != nil {
		t.Fatal(err)
	}

	if *bucket != "cli-bucket" {
		t.Fatalf("bucket = %q", *bucket)
	}
}

func TestDestinationTableRefFromSchema(t *testing.T) {
	got, err := destinationTableRefFromSchema(
		"CREATE TABLE posthog.query_log_archive_temp (x UInt64) ENGINE = MergeTree ORDER BY x",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Database != "posthog" || got.Table != "query_log_archive_temp" {
		t.Fatalf("destination = %+v", got)
	}
}

func TestDestinationTableRefFromSchemaRequiresDatabase(t *testing.T) {
	_, err := destinationTableRefFromSchema(
		"CREATE TABLE query_log_archive_temp (x UInt64) ENGINE = MergeTree ORDER BY x",
	)
	if err == nil {
		t.Fatal("expected unqualified table error")
	}
}

func TestApplyClickHouseClientConfigDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config>
  <user>alice</user>
  <password>secret</password>
</config>`)

	user := ""
	password := ""
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "alice" || password != "secret" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestApplyClickHouseClientConfigDefaultsFillsOnlyMissingCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config>
  <user>alice</user>
  <password>secret</password>
</config>`)

	user := "bob"
	password := ""
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "bob" || password != "secret" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestApplyClickHouseClientConfigDefaultsDoesNotOverrideConfiguredCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config>
  <user>alice</user>
  <password>secret</password>
</config>`)

	user := "bob"
	password := "configured"
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "bob" || password != "configured" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestApplyClickHouseClientConfigDefaultsUsesDefaultUserForPasswordOnlyConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config><password>secret</password></config>`)

	user := ""
	password := ""
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "default" || password != "secret" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func captureFileOutput(t *testing.T, write func(*os.File)) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "output.txt")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	write(out)
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
