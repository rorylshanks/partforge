package rewrite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/partforge/partforge/internal/artifact"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/ddl"
	"github.com/partforge/partforge/internal/fileutil"
	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/manifest"
	"github.com/partforge/partforge/internal/metrics"
	"github.com/partforge/partforge/internal/s3copy"
)

type Processor struct {
	S3Copy           s3copy.Copier
	ClickHouse       chhttp.Client
	WorkDir          string
	MergeTimeout     time.Duration
	Metrics          metrics.Recorder
	InsertSettings   chhttp.QuerySettings
	ProgressInterval time.Duration
	ReportProgress   ProgressReporter
}

type ProgressReporter func(context.Context, manifest.Manifest, ProgressSnapshot) error

type ProgressSnapshot struct {
	QueryProgress              *metrics.QueryProgress
	SourceActivePartStats      *metrics.PartStats
	DestinationActivePartStats *metrics.PartStats
}

type WorkItem struct {
	Bucket    string
	SourceKey string
	JobID     string
	PartID    string
	Attempt   int
}

type ProcessResult struct {
	FinishedKey string
}

type activePart struct {
	Name        string
	PartitionID string
}

type rewriteResult struct {
	Output  manifest.Output
	Cleanup func()
}

type workerTableInfo struct {
	Database string
	Name     string
	Engine   string
}

func (p Processor) ProcessPart(ctx context.Context, item WorkItem) (ProcessResult, error) {
	if item.Bucket == "" || item.SourceKey == "" || item.JobID == "" || item.PartID == "" {
		return ProcessResult{}, fmt.Errorf("work item is missing bucket, source_key, job_id, or part_id")
	}
	if item.Attempt < 1 {
		return ProcessResult{}, fmt.Errorf("work item attempt must be at least 1, got %d", item.Attempt)
	}
	if err := validateSafeSegment(item.JobID); err != nil {
		return ProcessResult{}, err
	}
	if err := validateSafeSegment(item.PartID); err != nil {
		return ProcessResult{}, err
	}

	startedAt := time.Now()
	slog.Info(
		"processing part",
		"stage", "process_part",
		"job_id", item.JobID,
		"part_id", item.PartID,
		"attempt", item.Attempt,
		"source_key", item.SourceKey,
	)

	root := filepath.Join(defaultWorkDir(p.WorkDir), item.JobID, item.PartID)
	if err := os.RemoveAll(root); err != nil {
		return ProcessResult{}, err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return ProcessResult{}, err
	}

	sourceRoot := filepath.Join(root, "source")
	slog.Info("downloading source artifact", "stage", "download_source", "job_id", item.JobID, "part_id", item.PartID, "bucket", item.Bucket, "source_key", item.SourceKey)
	downloadStartedAt := time.Now()
	if err := p.S3Copy.DownloadPrefix(ctx, item.Bucket, item.SourceKey, sourceRoot); err != nil {
		return ProcessResult{}, fmt.Errorf("download source artifact %s: %w", item.SourceKey, err)
	}
	sourceStats, err := fileutil.StatDir(sourceRoot)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("stat downloaded source artifact %s: %w", item.SourceKey, err)
	}
	downloadElapsed := time.Since(downloadStartedAt)
	slog.Info(
		"downloaded source artifact",
		"stage", "download_source",
		"job_id", item.JobID,
		"part_id", item.PartID,
		"files", sourceStats.Files,
		"bytes", sourceStats.Bytes,
		"elapsed", downloadElapsed,
		"bytes_per_second", ratePerSecond(sourceStats.Bytes, downloadElapsed),
	)

	m, err := artifact.ReadManifest(sourceRoot)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("read source manifest: %w", err)
	}
	slog.Info(
		"read source manifest",
		"stage", "read_manifest",
		"job_id", m.JobID,
		"part_id", m.PartID,
		"source_table", chhttp.TableSQL(m.Source.Database, m.Source.Table),
		"destination_table", chhttp.TableSQL(m.Dest.Database, m.Dest.Table),
		"part", m.Part.RelativePath,
		"disk", m.Part.Disk,
	)
	if m.JobID != item.JobID || m.PartID != item.PartID {
		return ProcessResult{}, fmt.Errorf("work item references %s/%s but manifest contains %s/%s", item.JobID, item.PartID, m.JobID, m.PartID)
	}
	if m.S3.Bucket != item.Bucket || m.S3.SourceKey != item.SourceKey {
		return ProcessResult{}, fmt.Errorf("work item S3 reference does not match manifest")
	}

	if err := artifact.RemoveManifest(sourceRoot); err != nil {
		return ProcessResult{}, err
	}

	recorder := p.recorder()
	recorder.ForgeStarted(m)
	finishedRoot := filepath.Join(root, "finished")
	slog.Info("rewriting source part", "stage", "rewrite_part", "job_id", m.JobID, "part_id", m.PartID)
	rewriteStartedAt := time.Now()
	rewriteResult, err := p.rewritePart(ctx, m, sourceRoot, finishedRoot)
	if err != nil {
		recorder.ForgeFailed(m)
		return ProcessResult{}, err
	}
	defer rewriteResult.Cleanup()
	slog.Info("rewrote source part", "stage", "rewrite_part", "job_id", m.JobID, "part_id", m.PartID, "output_parts", len(rewriteResult.Output.Parts), "elapsed", time.Since(rewriteStartedAt))

	m.Output = rewriteResult.Output
	m.S3.FinishedKey = manifest.FinishedPartAttemptPrefix(m.S3.FinishedKey, item.Attempt, time.Now().UTC())
	if err := artifact.WriteManifest(finishedRoot, m); err != nil {
		recorder.ForgeFailed(m)
		return ProcessResult{}, err
	}
	finishedStats, err := fileutil.StatDir(finishedRoot)
	if err != nil {
		recorder.ForgeFailed(m)
		return ProcessResult{}, fmt.Errorf("stat finished artifact %s: %w", m.S3.FinishedKey, err)
	}
	slog.Info(
		"uploading finished artifact",
		"stage", "upload_finished",
		"job_id", m.JobID,
		"part_id", m.PartID,
		"finished_key", m.S3.FinishedKey,
		"files", finishedStats.Files,
		"bytes", finishedStats.Bytes,
	)
	uploadStartedAt := time.Now()
	if err := p.S3Copy.UploadDir(ctx, finishedRoot, m.S3.Bucket, m.S3.FinishedKey); err != nil {
		recorder.ForgeFailed(m)
		return ProcessResult{}, fmt.Errorf("upload finished artifact %s: %w", m.S3.FinishedKey, err)
	}
	uploadElapsed := time.Since(uploadStartedAt)
	slog.Info(
		"uploaded finished artifact",
		"stage", "upload_finished",
		"job_id", m.JobID,
		"part_id", m.PartID,
		"finished_key", m.S3.FinishedKey,
		"files", finishedStats.Files,
		"bytes", finishedStats.Bytes,
		"elapsed", uploadElapsed,
		"bytes_per_second", ratePerSecond(finishedStats.Bytes, uploadElapsed),
	)
	recorder.ForgeCompleted(m)
	slog.Info("processed part", "stage", "complete_part", "job_id", m.JobID, "part_id", m.PartID, "finished_key", m.S3.FinishedKey, "output_parts", len(rewriteResult.Output.Parts), "elapsed", time.Since(startedAt))
	return ProcessResult{FinishedKey: m.S3.FinishedKey}, nil
}

func (p Processor) rewritePart(ctx context.Context, m manifest.Manifest, sourcePartRoot, finishedRoot string) (result rewriteResult, err error) {
	if m.Source.Database == m.Dest.Database && m.Source.Table == m.Dest.Table {
		return rewriteResult{}, fmt.Errorf("source and destination table names must differ inside the worker")
	}
	sourceDDL, err := ddl.ForTable(m.SQL.SourceSchema, m.Source.Database, m.Source.Table)
	if err != nil {
		return rewriteResult{}, fmt.Errorf("normalize source DDL: %w", err)
	}
	destDDL := strings.TrimSpace(m.SQL.DestinationSchema)
	var freezeName string

	cleanup := func() {
		cleanupCtx := context.Background()
		if freezeName != "" {
			if err := p.ClickHouse.Exec(cleanupCtx, "ALTER TABLE "+chhttp.TableSQL(m.Dest.Database, m.Dest.Table)+" UNFREEZE WITH NAME "+chhttp.StringLiteral(freezeName)); err != nil {
				slog.Warn("failed to remove frozen destination backup", "freeze", freezeName, "error", err)
			}
		}
		for _, database := range uniqueStrings(m.Source.Database, m.Dest.Database) {
			if err := p.ClickHouse.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+chhttp.Ident(database)+" SYNC"); err != nil {
				slog.Warn("failed to drop worker database", "database", database, "error", err)
			}
		}
	}
	result.Cleanup = cleanup
	defer func() {
		if result.Cleanup == nil {
			result.Cleanup = func() {}
		}
		if err != nil {
			p.logWorkerDiagnostics("rewrite_part_failed", m, err)
			cleanup()
		}
	}()

	slog.Info("preparing worker databases", "stage", "prepare_worker_tables", "job_id", m.JobID, "part_id", m.PartID)
	databases := uniqueStrings(m.Source.Database, m.Dest.Database)
	for _, database := range databases {
		if err := p.ClickHouse.Exec(ctx, "DROP DATABASE IF EXISTS "+chhttp.Ident(database)+" SYNC"); err != nil {
			return rewriteResult{}, fmt.Errorf("drop worker database %s: %w", database, err)
		}
	}

	for _, database := range databases {
		if err := p.ClickHouse.Exec(ctx, "CREATE DATABASE "+chhttp.Ident(database)); err != nil {
			return rewriteResult{}, fmt.Errorf("create worker database %s: %w", database, err)
		}
	}
	slog.Info("creating worker source table", "stage", "prepare_worker_tables", "job_id", m.JobID, "part_id", m.PartID, "source_table", chhttp.TableSQL(m.Source.Database, m.Source.Table))
	if err := p.ClickHouse.Exec(ctx, sourceDDL); err != nil {
		return rewriteResult{}, fmt.Errorf("create source table: %w", err)
	}
	slog.Info("creating worker destination table", "stage", "prepare_worker_tables", "job_id", m.JobID, "part_id", m.PartID, "destination_table", chhttp.TableSQL(m.Dest.Database, m.Dest.Table))
	if err := p.ClickHouse.Exec(ctx, destDDL); err != nil {
		return rewriteResult{}, fmt.Errorf("create destination table: %w", err)
	}
	slog.Info("created worker tables", "stage", "prepare_worker_tables", "job_id", m.JobID, "part_id", m.PartID)

	sourceDataPath, err := p.tableDataPath(ctx, m.Source.Database, m.Source.Table)
	if err != nil {
		return rewriteResult{}, err
	}
	sourceDetached := filepath.Join(sourceDataPath, "detached")
	if err := os.MkdirAll(sourceDetached, 0o755); err != nil {
		return rewriteResult{}, err
	}
	if err := fileutil.MoveDir(sourcePartRoot, filepath.Join(sourceDetached, m.Part.Name)); err != nil {
		return rewriteResult{}, fmt.Errorf("move source part to detached: %w", err)
	}
	slog.Info("attaching source part", "stage", "attach_source_part", "job_id", m.JobID, "part_id", m.PartID, "part", m.Part.Name, "source_table", chhttp.TableSQL(m.Source.Database, m.Source.Table))
	if err := p.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(m.Source.Database, m.Source.Table)+" ATTACH PART "+chhttp.StringLiteral(m.Part.Name)); err != nil {
		return rewriteResult{}, fmt.Errorf("attach source part %s: %w", m.Part.Name, err)
	}
	sourceStats, err := p.activePartStats(ctx, m.Source.Database, m.Source.Table)
	if err != nil {
		return rewriteResult{}, fmt.Errorf("measure source active parts: %w", err)
	}
	p.recorder().SetActivePartStats("source", m, sourceStats)
	if err := p.reportProgress(ctx, m, ProgressSnapshot{SourceActivePartStats: &sourceStats}); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("attached source part", "stage", "attach_source_part", "job_id", m.JobID, "part_id", m.PartID, "active_parts", sourceStats.Count, "active_rows", sourceStats.Rows, "active_bytes", sourceStats.Bytes)

	slog.Info("running insert-select", "stage", "insert_select", "job_id", m.JobID, "part_id", m.PartID)
	insertStartedAt := time.Now()
	if err := p.runInsertSelectWithRetries(ctx, m, destDDL); err != nil {
		return rewriteResult{}, fmt.Errorf("run insert-select: %w", err)
	}
	slog.Info("insert-select complete", "stage", "insert_select", "job_id", m.JobID, "part_id", m.PartID, "elapsed", time.Since(insertStartedAt))
	slog.Info("waiting for destination merges", "stage", "wait_merges", "job_id", m.JobID, "part_id", m.PartID, "destination_table", chhttp.TableSQL(m.Dest.Database, m.Dest.Table))
	if err := p.waitForMerges(ctx, m.Dest.Database, m.Dest.Table); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("destination merges complete", "stage", "wait_merges", "job_id", m.JobID, "part_id", m.PartID)
	destStats, err := p.activePartStats(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return rewriteResult{}, fmt.Errorf("measure destination active parts: %w", err)
	}
	p.recorder().SetActivePartStats("destination", m, destStats)
	if err := p.reportProgress(ctx, m, ProgressSnapshot{DestinationActivePartStats: &destStats}); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("measured destination parts", "stage", "measure_destination_parts", "job_id", m.JobID, "part_id", m.PartID, "active_parts", destStats.Count, "active_rows", destStats.Rows, "active_bytes", destStats.Bytes)

	activeParts, err := p.activeParts(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return rewriteResult{}, err
	}
	if len(activeParts) == 0 {
		result.Output = manifest.Output{}
		return result, nil
	}
	slog.Info("freezing produced destination parts", "stage", "freeze_destination_parts", "job_id", m.JobID, "part_id", m.PartID, "parts", len(activeParts))
	freezeName = workerFreezeName(m, time.Now().UTC())
	if err := p.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(m.Dest.Database, m.Dest.Table)+" FREEZE WITH NAME "+chhttp.StringLiteral(freezeName)); err != nil {
		return rewriteResult{}, fmt.Errorf("freeze destination table %s: %w", chhttp.TableSQL(m.Dest.Database, m.Dest.Table), err)
	}
	disks, err := freeze.LocalDisks(ctx, p.ClickHouse)
	if err != nil {
		return rewriteResult{}, err
	}
	frozenParts, err := freeze.ScanDisks(disks, freezeName)
	if err != nil {
		return rewriteResult{}, err
	}
	output, err := copyFrozenOutputParts(activeParts, frozenParts, finishedRoot)
	if err != nil {
		return rewriteResult{}, err
	}
	result.Output = output
	slog.Info("froze produced destination parts", "stage", "freeze_destination_parts", "job_id", m.JobID, "part_id", m.PartID, "freeze", freezeName, "parts", len(output.Parts))

	return result, nil
}

func (p Processor) runInsertSelectWithRetries(ctx context.Context, m manifest.Manifest, destDDL string) error {
	settings := cloneQuerySettings(p.InsertSettings)
	for attempt := 1; ; attempt++ {
		if err := p.runInsertSelect(ctx, m, attempt, settings); err != nil {
			if !retryableInsertSelectError(err) {
				return err
			}
			nextSettings, reduced, reduceErr := reduceInsertSelectThreadSettings(settings)
			if reduceErr != nil {
				return reduceErr
			}
			if !reduced {
				return err
			}
			backoff := insertSelectRetryBackoff(attempt)
			slog.Warn(
				"insert-select failed with retryable resource error; retrying with lower thread settings",
				"job_id", m.JobID,
				"part_id", m.PartID,
				"attempt", attempt,
				"next_attempt", attempt+1,
				"backoff", backoff.String(),
				"max_threads", nextSettings["max_threads"],
				"max_insert_threads", nextSettings["max_insert_threads"],
				"error", err,
			)
			if resetErr := resetDestinationTable(ctx, p.ClickHouse, m, destDDL); resetErr != nil {
				return fmt.Errorf("insert-select failed with retryable resource error (%w), but reset destination table failed: %v", err, resetErr)
			}
			if err := sleepOrDone(ctx, backoff); err != nil {
				return err
			}
			settings = nextSettings
			continue
		}
		return nil
	}
}

func (p Processor) runInsertSelect(ctx context.Context, m manifest.Manifest, attempt int, settings chhttp.QuerySettings) error {
	queryID := fmt.Sprintf("partforge-%s-%s-attempt-%d", m.JobID, m.PartID, attempt)
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.ClickHouse.ExecWithOptions(queryCtx, m.SQL.InsertSelect, chhttp.QueryOptions{
			QueryID:  queryID,
			Settings: settings,
		})
	}()

	recorder := p.recorder()
	progress := metrics.QueryProgress{}
	defer recorder.ClearCurrentProgress(m)
	lastProgressReport := time.Time{}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
			finalProgress, found, err := p.queryLogProgress(ctx, queryID)
			if err != nil {
				return fmt.Errorf("read final query progress: %w", err)
			}
			if found {
				recorder.ObserveProgress(m, progress, finalProgress)
				if err := p.reportProgress(ctx, m, ProgressSnapshot{QueryProgress: &finalProgress}); err != nil {
					return err
				}
			}
			return nil
		case <-ticker.C:
			now := time.Now()
			current, found, err := p.queryProgress(ctx, queryID)
			if err != nil {
				cancel()
				<-errCh
				return fmt.Errorf("read live query progress: %w", err)
			}
			if found {
				recorder.ObserveProgress(m, progress, current)
				progress = current
				if shouldReportProgress(p.ProgressInterval, lastProgressReport, now) {
					if err := p.reportProgress(ctx, m, ProgressSnapshot{QueryProgress: &current}); err != nil {
						cancel()
						<-errCh
						return err
					}
					lastProgressReport = now
				}
			}
		case <-ctx.Done():
			cancel()
			<-errCh
			return ctx.Err()
		}
	}
}

func (p Processor) reportProgress(ctx context.Context, m manifest.Manifest, snapshot ProgressSnapshot) error {
	if p.ReportProgress == nil {
		return nil
	}
	return p.ReportProgress(ctx, m, snapshot)
}

func shouldReportProgress(interval time.Duration, last time.Time, now time.Time) bool {
	if interval <= 0 {
		return false
	}
	return last.IsZero() || !now.Before(last.Add(interval))
}

func resetDestinationTable(ctx context.Context, ch chhttp.Client, m manifest.Manifest, destDDL string) error {
	table := chhttp.TableSQL(m.Dest.Database, m.Dest.Table)
	if err := ch.Exec(ctx, "DROP TABLE IF EXISTS "+table+" SYNC"); err != nil {
		return fmt.Errorf("drop destination table before retry: %w", err)
	}
	if err := ch.Exec(ctx, destDDL); err != nil {
		return fmt.Errorf("recreate destination table before retry: %w", err)
	}
	return nil
}

func (p Processor) logWorkerDiagnostics(stage string, m manifest.Manifest, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	databases, dbErr := p.workerDatabases(ctx)
	tables, tableErr := p.workerTables(ctx)
	diagnosticAttrs := []any{
		"stage", stage,
		"job_id", m.JobID,
		"part_id", m.PartID,
		"source_table", chhttp.TableSQL(m.Source.Database, m.Source.Table),
		"destination_table", chhttp.TableSQL(m.Dest.Database, m.Dest.Table),
		"insert_select_bytes", len(m.SQL.InsertSelect),
		"insert_select_preview", previewSQL(m.SQL.InsertSelect),
		"error", cause,
	}
	if dbErr != nil {
		diagnosticAttrs = append(diagnosticAttrs, "database_list_error", dbErr)
	} else {
		diagnosticAttrs = append(diagnosticAttrs, "databases", strings.Join(databases, ","))
	}
	if tableErr != nil {
		diagnosticAttrs = append(diagnosticAttrs, "table_list_error", tableErr)
	} else {
		diagnosticAttrs = append(diagnosticAttrs, "tables", formatWorkerTables(tables))
	}
	slog.Error("worker ClickHouse diagnostics", diagnosticAttrs...)
}

func (p Processor) workerDatabases(ctx context.Context) ([]string, error) {
	out, err := p.ClickHouse.QueryString(ctx, "SELECT name FROM system.databases ORDER BY name FORMAT TSV")
	if err != nil {
		return nil, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 1)
	if err != nil {
		return nil, err
	}
	databases := make([]string, 0, len(rows))
	for _, row := range rows {
		databases = append(databases, row[0])
	}
	return databases, nil
}

func (p Processor) workerTables(ctx context.Context) ([]workerTableInfo, error) {
	query := "SELECT database, name, engine FROM system.tables " +
		"WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') " +
		"ORDER BY database, name FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 3)
	if err != nil {
		return nil, err
	}
	tables := make([]workerTableInfo, 0, len(rows))
	for _, row := range rows {
		tables = append(tables, workerTableInfo{Database: row[0], Name: row[1], Engine: row[2]})
	}
	return tables, nil
}

func formatWorkerTables(tables []workerTableInfo) string {
	if len(tables) == 0 {
		return "<none>"
	}
	parts := make([]string, 0, len(tables))
	for _, table := range tables {
		parts = append(parts, fmt.Sprintf("%s(%s)", chhttp.TableSQL(table.Database, table.Name), table.Engine))
	}
	return strings.Join(parts, ",")
}

func previewSQL(query string) string {
	preview := strings.Join(strings.Fields(query), " ")
	if len(preview) <= 1000 {
		return preview
	}
	return preview[:1000] + "...<truncated>"
}

func cloneQuerySettings(settings chhttp.QuerySettings) chhttp.QuerySettings {
	if len(settings) == 0 {
		return nil
	}
	out := make(chhttp.QuerySettings, len(settings))
	for key, value := range settings {
		out[key] = value
	}
	return out
}

func reduceInsertSelectThreadSettings(settings chhttp.QuerySettings) (chhttp.QuerySettings, bool, error) {
	currentInsertThreads, ok, err := positiveIntSetting(settings, "max_insert_threads")
	if err != nil || !ok {
		return nil, false, err
	}
	if currentInsertThreads <= 1 {
		return nil, false, nil
	}

	next := cloneQuerySettings(settings)
	next["max_insert_threads"] = strconv.Itoa(halvedAtLeastOne(currentInsertThreads))
	if currentMaxThreads, ok, err := positiveIntSetting(settings, "max_threads"); err != nil {
		return nil, false, err
	} else if ok && currentMaxThreads > 1 {
		next["max_threads"] = strconv.Itoa(halvedAtLeastOne(currentMaxThreads))
	}
	return next, true, nil
}

func positiveIntSetting(settings chhttp.QuerySettings, name string) (int, bool, error) {
	if len(settings) == 0 {
		return 0, false, nil
	}
	raw, ok := settings[name]
	if !ok {
		return 0, false, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false, fmt.Errorf("parse clickhouse setting %s=%q: %w", name, raw, err)
	}
	if value < 1 {
		return 0, false, fmt.Errorf("clickhouse setting %s must be at least 1, got %d", name, value)
	}
	return value, true, nil
}

func halvedAtLeastOne(value int) int {
	next := value / 2
	if next < 1 {
		return 1
	}
	return next
}

func retryableInsertSelectError(err error) bool {
	var queryErr *chhttp.QueryError
	if !errors.As(err, &queryErr) {
		return false
	}
	body := strings.ToLower(queryErr.Body)
	for _, marker := range []string{
		"memory_limit_exceeded",
		"memory limit",
		"cannot allocate memory",
		"not enough memory",
		"std::bad_alloc",
		"too many threads",
	} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

func insertSelectRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		return time.Second
	}
	if attempt >= 5 {
		return 10 * time.Second
	}
	backoff := time.Second << (attempt - 1)
	return backoff
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func ratePerSecond(bytes uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func (p Processor) tableDataPath(ctx context.Context, database, table string) (string, error) {
	query := "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = " +
		chhttp.StringLiteral(database) + " AND name = " + chhttp.StringLiteral(table) + " FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return "", err
	}
	path := strings.TrimSpace(out)
	if path == "" {
		return "", fmt.Errorf("could not find data path for %s", chhttp.TableSQL(database, table))
	}
	return path, nil
}

func (p Processor) activeParts(ctx context.Context, database, table string) ([]activePart, error) {
	query := "SELECT name, partition_id FROM system.parts WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) +
		" AND active ORDER BY name FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 2)
	if err != nil {
		return nil, err
	}
	parts := make([]activePart, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, activePart{Name: row[0], PartitionID: row[1]})
	}
	return parts, nil
}

func (p Processor) activePartStats(ctx context.Context, database, table string) (metrics.PartStats, error) {
	query := "SELECT count(), ifNull(sum(rows), 0), ifNull(sum(bytes_on_disk), 0) FROM system.parts WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) +
		" AND active FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return metrics.PartStats{}, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 3)
	if err != nil {
		return metrics.PartStats{}, err
	}
	if len(rows) != 1 {
		return metrics.PartStats{}, fmt.Errorf("expected one active part stats row, got %d", len(rows))
	}
	count, err := chhttp.ParseUInt(rows[0][0])
	if err != nil {
		return metrics.PartStats{}, err
	}
	partRows, err := chhttp.ParseUInt(rows[0][1])
	if err != nil {
		return metrics.PartStats{}, err
	}
	bytes, err := chhttp.ParseUInt(rows[0][2])
	if err != nil {
		return metrics.PartStats{}, err
	}
	return metrics.PartStats{Count: count, Rows: partRows, Bytes: bytes}, nil
}

func (p Processor) queryProgress(ctx context.Context, queryID string) (metrics.QueryProgress, bool, error) {
	query := "SELECT read_rows, read_bytes, written_rows, written_bytes FROM system.processes WHERE query_id = " +
		chhttp.StringLiteral(queryID) + " FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	return parseQueryProgress(out)
}

func (p Processor) queryLogProgress(ctx context.Context, queryID string) (metrics.QueryProgress, bool, error) {
	if err := p.ClickHouse.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		return metrics.QueryProgress{}, false, err
	}
	query := "SELECT read_rows, read_bytes, written_rows, written_bytes FROM system.query_log WHERE query_id = " +
		chhttp.StringLiteral(queryID) + " AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	return parseQueryProgress(out)
}

func parseQueryProgress(out string) (metrics.QueryProgress, bool, error) {
	rows, err := chhttp.FormatTSVStrings(out, 4)
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	if len(rows) == 0 {
		return metrics.QueryProgress{}, false, nil
	}
	if len(rows) != 1 {
		return metrics.QueryProgress{}, false, fmt.Errorf("expected one query progress row, got %d", len(rows))
	}
	readRows, err := chhttp.ParseUInt(rows[0][0])
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	readBytes, err := chhttp.ParseUInt(rows[0][1])
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	writtenRows, err := chhttp.ParseUInt(rows[0][2])
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	writtenBytes, err := chhttp.ParseUInt(rows[0][3])
	if err != nil {
		return metrics.QueryProgress{}, false, err
	}
	return metrics.QueryProgress{
		ReadRows:     readRows,
		ReadBytes:    readBytes,
		WrittenRows:  writtenRows,
		WrittenBytes: writtenBytes,
	}, true, nil
}

func (p Processor) waitForMerges(ctx context.Context, database, table string) error {
	timeout := p.MergeTimeout
	if timeout == 0 {
		timeout = 10 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	query := "SELECT count() FROM system.merges WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) + " FORMAT TSV"
	for {
		out, err := p.ClickHouse.QueryString(ctx, query)
		if err != nil {
			return err
		}
		count, err := chhttp.ParseUInt(out)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("destination merges did not settle within %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func defaultWorkDir(workDir string) string {
	if workDir == "" {
		return "/tmp/partforge"
	}
	return workDir
}

func workerFreezeName(m manifest.Manifest, frozenAt time.Time) string {
	return fmt.Sprintf("partforge-%s-%s-%s", m.JobID, m.PartID, frozenAt.UTC().Format("20060102T150405.000000000Z"))
}

func copyFrozenOutputParts(activeParts []activePart, frozenParts []freeze.Part, finishedRoot string) (manifest.Output, error) {
	if len(activeParts) != len(frozenParts) {
		return manifest.Output{}, fmt.Errorf("frozen part count %d does not match active part count %d", len(frozenParts), len(activeParts))
	}

	frozenByName := make(map[string]freeze.Part, len(frozenParts))
	for _, part := range frozenParts {
		if _, exists := frozenByName[part.Name]; exists {
			return manifest.Output{}, fmt.Errorf("frozen snapshot contains duplicate part name %s", part.Name)
		}
		frozenByName[part.Name] = part
	}

	outputParts := make([]manifest.OutputPart, 0, len(activeParts))
	for _, part := range activeParts {
		frozenPart, ok := frozenByName[part.Name]
		if !ok {
			return manifest.Output{}, fmt.Errorf("frozen snapshot is missing active part %s", part.Name)
		}
		if err := fileutil.CopyDir(frozenPart.Path, artifact.FinishedPartPath(finishedRoot, part.Name)); err != nil {
			return manifest.Output{}, fmt.Errorf("copy frozen part %s from %s: %w", part.Name, frozenPart.Path, err)
		}
		outputParts = append(outputParts, manifest.OutputPart{Name: part.Name, PartitionID: part.PartitionID})
	}
	return manifest.Output{Parts: outputParts}, nil
}

func (p Processor) recorder() metrics.Recorder {
	if p.Metrics == nil {
		return metrics.Noop{}
	}
	return p.Metrics
}

func validateSafeSegment(value string) error {
	if value == "" || strings.Contains(value, "/") || strings.Contains(value, "..") {
		return fmt.Errorf("unsafe path segment %q", value)
	}
	return nil
}

func uniqueStrings(values ...string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
