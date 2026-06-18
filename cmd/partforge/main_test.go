package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
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
		{PartID: "part-1", Status: state.StatusImported, ReadRows: 10, ReadBytes: 100, WrittenRows: 9, WrittenBytes: 90},
		{PartID: "part-2", Status: state.StatusFinished, ReadRows: 20, ReadBytes: 200, WrittenRows: 19, WrittenBytes: 190},
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
	if len(summary.FailedParts) != 1 || summary.FailedParts[0].PartID != "part-3" {
		t.Fatalf("failed parts = %+v", summary.FailedParts)
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

	all, err := selectRetryParts(parts, true, false, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all len = %d", len(all))
	}

	allWithInProgress, err := selectRetryParts(parts, true, false, true, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(allWithInProgress) != 3 || allWithInProgress[2].PartID != "part-4" {
		t.Fatalf("all with in-progress = %+v", allWithInProgress)
	}

	forced, err := selectRetryParts(parts, true, true, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(forced) != 5 {
		t.Fatalf("forced len = %d", len(forced))
	}

	one, err := selectRetryParts(parts, false, false, false, "part-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].PartID != "part-1" {
		t.Fatalf("one = %+v", one)
	}

	inProgress, err := selectRetryParts(parts, false, false, true, "part-4")
	if err != nil {
		t.Fatal(err)
	}
	if len(inProgress) != 1 || inProgress[0].PartID != "part-4" {
		t.Fatalf("in-progress = %+v", inProgress)
	}

	if _, err := selectRetryParts(parts, false, false, false, "part-2"); err == nil {
		t.Fatal("expected non-failed part error")
	}

	if _, err := selectRetryParts(parts, false, false, true, "part-5"); err == nil {
		t.Fatal("expected completed part error")
	}
}

func TestJobS3Prefixes(t *testing.T) {
	prefixes, err := jobS3Prefixes("job-1", []state.Part{
		{
			JobID:       "job-1",
			PartID:      "part-1",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-1/source/part-1",
			FinishedKey: "partforge/jobs/job-1/finished/part-1/attempt-000001",
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

	progress := stateProgress(rewrite.ProgressSnapshot{
		QueryProgress:              &query,
		SourceActivePartStats:      &source,
		DestinationActivePartStats: &dest,
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
