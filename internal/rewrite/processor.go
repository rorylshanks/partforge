package rewrite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
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

const DefaultMergeTimeout = 10 * time.Minute
const DefaultMergeSettleMinWait = 2 * time.Minute
const DefaultMergeSettleMinParts uint64 = 1
const defaultMergePollInterval = time.Second

const (
	stageProcessPart             = "process_part"
	stagePrepareWorkDir          = "prepare_work_dir"
	stageDownloadSource          = "download_source"
	stageReadManifest            = "read_manifest"
	stagePrepareWorkerTables     = "prepare_worker_tables"
	stageAttachSourcePart        = "attach_source_part"
	stageInsertSelect            = "insert_select"
	stageConfigureMergeSettings  = "configure_merge_settings"
	stageRestartClickHouse       = "restart_clickhouse"
	stageOptimizeFinal           = "optimize_final"
	stageWaitMerges              = "wait_merges"
	stageMeasureDestinationParts = "measure_destination_parts"
	stageFreezeDestinationParts  = "freeze_destination_parts"
	stageArchiveFinishedParts    = "archive_finished_parts"
	stageDeleteFinishedArtifact  = "delete_finished_artifact"
	stageUploadFinishedTarballs  = "upload_finished_tarballs"
	stageCompletePart            = "complete_part"
)

var stageOrder = []string{
	stageProcessPart,
	stagePrepareWorkDir,
	stageDownloadSource,
	stageReadManifest,
	stagePrepareWorkerTables,
	stageAttachSourcePart,
	stageInsertSelect,
	stageConfigureMergeSettings,
	stageRestartClickHouse,
	stageOptimizeFinal,
	stageWaitMerges,
	stageMeasureDestinationParts,
	stageFreezeDestinationParts,
	stageArchiveFinishedParts,
	stageDeleteFinishedArtifact,
	stageUploadFinishedTarballs,
	stageCompletePart,
}

// StageOrder returns the rewrite progress stages in the order a worker reaches them.
func StageOrder() []string {
	return append([]string(nil), stageOrder...)
}

type Processor struct {
	S3Copy              s3copy.Copier
	ClickHouse          chhttp.Client
	WorkDir             string
	MergeTimeout        time.Duration
	Metrics             metrics.Recorder
	InsertSettings      chhttp.QuerySettings
	ProgressInterval    time.Duration
	ReportProgress      ProgressReporter
	MergeTreeSettings   MergeTreeSettings
	MergeSettleMinWait  time.Duration
	MergeSettleMinParts uint64
	MergePollInterval   time.Duration
	RestartClickHouse   func(context.Context) error
	ForceOptimizeFinal  bool
}

type MergeTreeSettings struct {
	MergeMaxBlockSize      uint64
	MergeMaxBlockSizeBytes uint64
	MergeSelectingSleepMS  uint64
}

type ProgressReporter func(context.Context, manifest.Manifest, ProgressSnapshot) error

type ProgressSnapshot struct {
	QueryProgress              *metrics.QueryProgress
	SourceActivePartStats      *metrics.PartStats
	DestinationActivePartStats *metrics.PartStats
	StageProgress              *StageProgress
}

type StageProgress struct {
	Stage                   string
	StageStartedAt          time.Time
	StageElapsed            time.Duration
	TotalElapsed            time.Duration
	CompletedStageDurations map[string]time.Duration
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

type frozenPartGlob struct {
	Disk string
	Glob string
}

type rewriteResult struct {
	FrozenPartGlobs []frozenPartGlob
}

type workerTableInfo struct {
	Database string
	Name     string
	Engine   string
}

type rewriteStageTracker struct {
	mu             sync.Mutex
	reportMu       sync.Mutex
	startedAt      time.Time
	stage          string
	stageStartedAt time.Time
	completed      map[string]time.Duration
}

func newRewriteStageTracker(startedAt time.Time, stage string) *rewriteStageTracker {
	return &rewriteStageTracker{
		startedAt:      startedAt,
		stage:          stage,
		stageStartedAt: startedAt,
		completed:      map[string]time.Duration{},
	}
}

func (t *rewriteStageTracker) Start(stage string, now time.Time) StageProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startLocked(stage, now)
	return t.snapshotLocked(now)
}

func (t *rewriteStageTracker) Complete(stage string, now time.Time) StageProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.startLocked(stage, now)
	return t.snapshotLocked(now)
}

func (t *rewriteStageTracker) Snapshot(now time.Time) StageProgress {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.snapshotLocked(now)
}

func (t *rewriteStageTracker) startLocked(stage string, now time.Time) {
	if stage == "" || stage == t.stage {
		return
	}
	if t.stage != "" && !t.stageStartedAt.IsZero() {
		t.completed[t.stage] += nonNegativeDuration(now.Sub(t.stageStartedAt))
	}
	t.stage = stage
	t.stageStartedAt = now
}

func (t *rewriteStageTracker) snapshotLocked(now time.Time) StageProgress {
	completed := make(map[string]time.Duration, len(t.completed))
	for stage, duration := range t.completed {
		completed[stage] = duration
	}
	return StageProgress{
		Stage:                   t.stage,
		StageStartedAt:          t.stageStartedAt,
		StageElapsed:            nonNegativeDuration(now.Sub(t.stageStartedAt)),
		TotalElapsed:            nonNegativeDuration(now.Sub(t.startedAt)),
		CompletedStageDurations: completed,
	}
}

func nonNegativeDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
}

func (p Processor) ProcessPart(ctx context.Context, item WorkItem) (result ProcessResult, err error) {
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

	progressManifest := manifest.Manifest{JobID: item.JobID, PartID: item.PartID}
	stageTracker := newRewriteStageTracker(startedAt, stageProcessPart)
	heartbeat, err := p.startProgressHeartbeat(ctx, progressManifest, stageTracker)
	if err != nil {
		return ProcessResult{}, err
	}
	ctx = heartbeat.Context()
	defer func() {
		if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
			err = errors.Join(err, heartbeatErr)
		}
	}()

	if err := p.reportStageProgress(ctx, progressManifest, stageTracker, stagePrepareWorkDir); err != nil {
		return ProcessResult{}, err
	}
	root := filepath.Join(defaultWorkDir(p.WorkDir), item.JobID, item.PartID)
	if err := os.RemoveAll(root); err != nil {
		return ProcessResult{}, err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return ProcessResult{}, err
	}

	sourceRoot := filepath.Join(root, "source")
	if err := p.reportStageProgress(ctx, progressManifest, stageTracker, stageDownloadSource); err != nil {
		return ProcessResult{}, err
	}
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

	if err := p.reportStageProgress(ctx, progressManifest, stageTracker, stageReadManifest); err != nil {
		return ProcessResult{}, err
	}
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
		"optimize_final", m.Options.OptimizeFinal,
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
	if err := p.reportStageProgress(ctx, m, stageTracker, stagePrepareWorkerTables); err != nil {
		return ProcessResult{}, err
	}
	slog.Info("rewriting source part", "stage", "rewrite_part", "job_id", m.JobID, "part_id", m.PartID)
	rewriteStartedAt := time.Now()
	rewriteResult, err := p.rewritePart(ctx, m, sourceRoot, stageTracker)
	if err != nil {
		recorder.ForgeFailed(m)
		return ProcessResult{}, err
	}
	slog.Info("rewrote source part", "stage", "rewrite_part", "job_id", m.JobID, "part_id", m.PartID, "frozen_part_globs", len(rewriteResult.FrozenPartGlobs), "elapsed", time.Since(rewriteStartedAt))

	slog.Info(
		"uploading finished artifact",
		"stage", "upload_finished",
		"job_id", m.JobID,
		"part_id", m.PartID,
		"finished_key", m.S3.FinishedKey,
		"frozen_part_globs", len(rewriteResult.FrozenPartGlobs),
	)
	uploadStartedAt := time.Now()
	finishedTarDir := filepath.Join(root, "finished-tars")
	if err := p.uploadFinishedArtifact(ctx, m, finishedTarDir, rewriteResult.FrozenPartGlobs, stageTracker); err != nil {
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
		"frozen_part_globs", len(rewriteResult.FrozenPartGlobs),
		"elapsed", uploadElapsed,
	)
	recorder.ForgeCompleted(m)
	if err := p.reportStageComplete(ctx, m, stageTracker, stageCompletePart); err != nil {
		return ProcessResult{}, err
	}
	slog.Info("processed part", "stage", "complete_part", "job_id", m.JobID, "part_id", m.PartID, "finished_key", m.S3.FinishedKey, "frozen_part_globs", len(rewriteResult.FrozenPartGlobs), "elapsed", time.Since(startedAt))
	return ProcessResult{FinishedKey: m.S3.FinishedKey}, nil
}

func (p Processor) rewritePart(ctx context.Context, m manifest.Manifest, sourcePartRoot string, stageTracker *rewriteStageTracker) (result rewriteResult, err error) {
	if m.Source.Database == m.Dest.Database && m.Source.Table == m.Dest.Table {
		return rewriteResult{}, fmt.Errorf("source and destination table names must differ inside the worker")
	}
	sourceDDL, err := ddl.ForTable(m.SQL.SourceSchema, m.Source.Database, m.Source.Table)
	if err != nil {
		return rewriteResult{}, fmt.Errorf("normalize source DDL: %w", err)
	}
	destDDL := strings.TrimSpace(m.SQL.DestinationSchema)
	defer func() {
		if err != nil && ctx.Err() == nil {
			p.logWorkerDiagnostics("rewrite_part_failed", m, err)
		}
	}()

	if err := p.reportStageProgress(ctx, m, stageTracker, stagePrepareWorkerTables); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("preparing worker databases", "stage", "prepare_worker_tables", "job_id", m.JobID, "part_id", m.PartID)
	databases := uniqueStrings(m.Source.Database, m.Dest.Database)
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
	if err := p.reportStageProgress(ctx, m, stageTracker, stageAttachSourcePart); err != nil {
		return rewriteResult{}, err
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

	if err := p.reportStageProgress(ctx, m, stageTracker, stageInsertSelect); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("running insert-select", "stage", "insert_select", "job_id", m.JobID, "part_id", m.PartID)
	insertStartedAt := time.Now()
	if err := p.runInsertSelectWithRetries(ctx, m, destDDL); err != nil {
		return rewriteResult{}, fmt.Errorf("run insert-select: %w", err)
	}
	slog.Info("insert-select complete", "stage", "insert_select", "job_id", m.JobID, "part_id", m.PartID, "elapsed", time.Since(insertStartedAt))
	if err := p.reportStageProgress(ctx, m, stageTracker, stageConfigureMergeSettings); err != nil {
		return rewriteResult{}, err
	}
	if err := p.configureDestinationMergeSettings(ctx, m); err != nil {
		return rewriteResult{}, err
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageRestartClickHouse); err != nil {
		return rewriteResult{}, err
	}
	if err := p.restartClickHouse(ctx, m); err != nil {
		return rewriteResult{}, err
	}
	if p.shouldOptimizeFinal(m) {
		if err := p.reportStageProgress(ctx, m, stageTracker, stageOptimizeFinal); err != nil {
			return rewriteResult{}, err
		}
		slog.Info("optimizing destination table final", "stage", "optimize_final", "job_id", m.JobID, "part_id", m.PartID, "destination_table", chhttp.TableSQL(m.Dest.Database, m.Dest.Table), "manifest_optimize_final", m.Options.OptimizeFinal, "worker_force_optimize_final", p.ForceOptimizeFinal)
		optimizeStartedAt := time.Now()
		if err := p.optimizeFinal(ctx, m); err != nil {
			return rewriteResult{}, fmt.Errorf("optimize destination table final: %w", err)
		}
		slog.Info("optimized destination table final", "stage", "optimize_final", "job_id", m.JobID, "part_id", m.PartID, "elapsed", time.Since(optimizeStartedAt))
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageWaitMerges); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("waiting for destination merges", "stage", "wait_merges", "job_id", m.JobID, "part_id", m.PartID, "destination_table", chhttp.TableSQL(m.Dest.Database, m.Dest.Table))
	mergeWait, err := p.waitForMerges(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return rewriteResult{}, err
	}
	if mergeWait.Settled {
		slog.Info(
			"destination merges complete",
			"stage", "wait_merges",
			"job_id", m.JobID,
			"part_id", m.PartID,
			"active_parts", mergeWait.ActiveParts,
			"zero_merges_idle", mergeWait.ZeroMergesIdle,
		)
	} else {
		slog.Warn(
			"destination merges did not settle before timeout; freezing current destination parts",
			"stage", "wait_merges",
			"job_id", m.JobID,
			"part_id", m.PartID,
			"timeout", mergeWait.Timeout,
			"active_merges", mergeWait.ActiveMerges,
			"active_parts", mergeWait.ActiveParts,
			"zero_merges_idle", mergeWait.ZeroMergesIdle,
		)
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageMeasureDestinationParts); err != nil {
		return rewriteResult{}, err
	}
	destStats, err := p.activePartStats(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return rewriteResult{}, fmt.Errorf("measure destination active parts: %w", err)
	}
	p.recorder().SetActivePartStats("destination", m, destStats)
	if err := p.reportProgress(ctx, m, ProgressSnapshot{DestinationActivePartStats: &destStats}); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("measured destination parts", "stage", "measure_destination_parts", "job_id", m.JobID, "part_id", m.PartID, "active_parts", destStats.Count, "active_rows", destStats.Rows, "active_bytes", destStats.Bytes)

	if destStats.Count == 0 {
		return result, nil
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageFreezeDestinationParts); err != nil {
		return rewriteResult{}, err
	}
	slog.Info("freezing produced destination parts", "stage", "freeze_destination_parts", "job_id", m.JobID, "part_id", m.PartID, "active_parts", destStats.Count)
	freezeName := workerFreezeName(m, time.Now().UTC())
	if err := p.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(m.Dest.Database, m.Dest.Table)+" FREEZE WITH NAME "+chhttp.StringLiteral(freezeName)); err != nil {
		return rewriteResult{}, fmt.Errorf("freeze destination table %s: %w", chhttp.TableSQL(m.Dest.Database, m.Dest.Table), err)
	}
	disks, err := freeze.LocalDisks(ctx, p.ClickHouse)
	if err != nil {
		return rewriteResult{}, err
	}
	frozenPartGlobs, err := frozenPartUploadGlobs(disks, freezeName)
	if err != nil {
		return rewriteResult{}, err
	}
	result.FrozenPartGlobs = frozenPartGlobs
	slog.Info("froze produced destination parts", "stage", "freeze_destination_parts", "job_id", m.JobID, "part_id", m.PartID, "freeze", freezeName, "frozen_part_globs", len(result.FrozenPartGlobs))

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

func (p Processor) configureDestinationMergeSettings(ctx context.Context, m manifest.Manifest) error {
	mergeTreeSettings := p.MergeTreeSettings
	table := chhttp.TableSQL(m.Dest.Database, m.Dest.Table)
	if mergeTreeSettings.MergeMaxBlockSize == 0 {
		return fmt.Errorf("merge_max_block_size must be greater than zero")
	}
	if mergeTreeSettings.MergeMaxBlockSizeBytes == 0 {
		return fmt.Errorf("merge_max_block_size_bytes must be greater than zero")
	}
	if mergeTreeSettings.MergeSelectingSleepMS == 0 {
		return fmt.Errorf("merge_selecting_sleep_ms must be greater than zero")
	}
	query := "ALTER TABLE " + table +
		" MODIFY SETTING merge_max_block_size = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSize, 10) +
		", merge_max_block_size_bytes = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSizeBytes, 10) +
		", merge_selecting_sleep_ms = " + strconv.FormatUint(mergeTreeSettings.MergeSelectingSleepMS, 10)
	if err := p.ClickHouse.Exec(ctx, query); err != nil {
		return fmt.Errorf("configure destination table merge settings: %w", err)
	}
	slog.Info(
		"configured destination merge settings",
		"stage", stageConfigureMergeSettings,
		"job_id", m.JobID,
		"part_id", m.PartID,
		"destination_table", table,
		"merge_max_block_size", mergeTreeSettings.MergeMaxBlockSize,
		"merge_max_block_size_bytes", mergeTreeSettings.MergeMaxBlockSizeBytes,
		"merge_selecting_sleep_ms", mergeTreeSettings.MergeSelectingSleepMS,
	)
	return nil
}

func (p Processor) restartClickHouse(ctx context.Context, m manifest.Manifest) error {
	if p.RestartClickHouse == nil {
		return fmt.Errorf("restart clickhouse callback is required before destination merges")
	}
	slog.Info("restarting local ClickHouse before destination merges", "stage", "restart_clickhouse", "job_id", m.JobID, "part_id", m.PartID)
	startedAt := time.Now()
	if err := p.RestartClickHouse(ctx); err != nil {
		return fmt.Errorf("restart clickhouse before destination merges: %w", err)
	}
	slog.Info("restarted local ClickHouse before destination merges", "stage", "restart_clickhouse", "job_id", m.JobID, "part_id", m.PartID, "elapsed", time.Since(startedAt))
	return nil
}

func (p Processor) shouldOptimizeFinal(m manifest.Manifest) bool {
	return p.ForceOptimizeFinal || m.Options.OptimizeFinal
}

func (p Processor) optimizeFinal(ctx context.Context, m manifest.Manifest) error {
	return p.ClickHouse.Exec(ctx, "OPTIMIZE TABLE "+chhttp.TableSQL(m.Dest.Database, m.Dest.Table)+" FINAL")
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

type progressHeartbeat struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

func (p Processor) startProgressHeartbeat(ctx context.Context, m manifest.Manifest, tracker *rewriteStageTracker) (*progressHeartbeat, error) {
	heartbeat := &progressHeartbeat{ctx: ctx}
	if p.ReportProgress == nil || p.ProgressInterval <= 0 {
		return heartbeat, nil
	}

	heartbeat.ctx, heartbeat.cancel = context.WithCancel(ctx)
	heartbeat.done = make(chan struct{})
	if err := p.reportStageSnapshot(heartbeat.ctx, m, tracker); err != nil {
		heartbeat.cancel()
		return heartbeat, err
	}

	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(p.ProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := p.reportStageSnapshot(heartbeat.ctx, m, tracker); err != nil {
					heartbeat.setErr(err)
					return
				}
			case <-heartbeat.ctx.Done():
				return
			}
		}
	}()
	return heartbeat, nil
}

func (p Processor) reportStageProgress(ctx context.Context, m manifest.Manifest, tracker *rewriteStageTracker, stage string) error {
	if tracker == nil {
		return nil
	}
	tracker.reportMu.Lock()
	defer tracker.reportMu.Unlock()

	now := time.Now()
	progress := tracker.Start(stage, now)
	return p.reportProgress(ctx, m, ProgressSnapshot{StageProgress: &progress})
}

func (p Processor) reportStageComplete(ctx context.Context, m manifest.Manifest, tracker *rewriteStageTracker, stage string) error {
	if tracker == nil {
		return nil
	}
	tracker.reportMu.Lock()
	defer tracker.reportMu.Unlock()

	now := time.Now()
	progress := tracker.Complete(stage, now)
	return p.reportProgress(ctx, m, ProgressSnapshot{StageProgress: &progress})
}

func (p Processor) reportStageSnapshot(ctx context.Context, m manifest.Manifest, tracker *rewriteStageTracker) error {
	if tracker == nil {
		return p.reportProgress(ctx, m, ProgressSnapshot{})
	}
	tracker.reportMu.Lock()
	defer tracker.reportMu.Unlock()

	progress := tracker.Snapshot(time.Now())
	return p.reportProgress(ctx, m, ProgressSnapshot{StageProgress: &progress})
}

func (h *progressHeartbeat) Context() context.Context {
	return h.ctx
}

func (h *progressHeartbeat) Stop() error {
	if h.cancel == nil {
		return nil
	}
	h.cancel()
	<-h.done
	return h.Err()
}

func (h *progressHeartbeat) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *progressHeartbeat) setErr(err error) {
	h.mu.Lock()
	if h.err == nil {
		h.err = err
		h.cancel()
	}
	h.mu.Unlock()
}

func shouldReportProgress(interval time.Duration, last time.Time, now time.Time) bool {
	if interval <= 0 {
		return false
	}
	return last.IsZero() || !now.Before(last.Add(interval))
}

func resetDestinationTable(ctx context.Context, ch chhttp.Client, m manifest.Manifest, destDDL string) error {
	table := chhttp.TableSQL(m.Dest.Database, m.Dest.Table)
	if err := ch.ExecWithOptions(ctx, "DROP TABLE IF EXISTS "+table+" SYNC", chhttp.QueryOptions{
		Settings: chhttp.QuerySettings{
			"max_table_size_to_drop":     "0",
			"max_partition_size_to_drop": "0",
		},
	}); err != nil {
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

type mergeWaitResult struct {
	Settled        bool
	ActiveMerges   uint64
	ActiveParts    uint64
	ZeroMergesIdle time.Duration
	Timeout        time.Duration
}

func (p Processor) waitForMerges(ctx context.Context, database, table string) (mergeWaitResult, error) {
	timeout := p.MergeTimeout
	if timeout == 0 {
		timeout = DefaultMergeTimeout
	}
	minWait := p.MergeSettleMinWait
	if minWait == 0 {
		minWait = DefaultMergeSettleMinWait
	}
	if minWait < 0 {
		return mergeWaitResult{}, fmt.Errorf("merge settle minimum wait must be non-negative, got %s", minWait)
	}
	minParts := p.MergeSettleMinParts
	if minParts == 0 {
		minParts = DefaultMergeSettleMinParts
	}
	pollInterval := p.MergePollInterval
	if pollInterval == 0 {
		pollInterval = defaultMergePollInterval
	}
	if pollInterval < 0 {
		return mergeWaitResult{}, fmt.Errorf("merge poll interval must be non-negative, got %s", pollInterval)
	}
	deadline := time.Now().Add(timeout)
	mergeQuery := "SELECT count() FROM system.merges WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) + " FORMAT TSV"
	var zeroMergesStablePartsSince time.Time
	var zeroMergesActiveParts uint64
	for {
		out, err := p.ClickHouse.QueryString(ctx, mergeQuery)
		if err != nil {
			return mergeWaitResult{}, err
		}
		count, err := chhttp.ParseUInt(out)
		if err != nil {
			return mergeWaitResult{}, err
		}
		now := time.Now()
		if count == 0 {
			activeParts, err := p.activePartCount(ctx, database, table)
			if err != nil {
				return mergeWaitResult{}, err
			}
			if activeParts <= minParts {
				return mergeWaitResult{Settled: true, ActiveParts: activeParts, Timeout: timeout}, nil
			}
			if zeroMergesStablePartsSince.IsZero() || activeParts != zeroMergesActiveParts {
				zeroMergesStablePartsSince = now
				zeroMergesActiveParts = activeParts
			}
			zeroMergesIdle := now.Sub(zeroMergesStablePartsSince)
			if zeroMergesIdle >= minWait {
				return mergeWaitResult{Settled: true, ActiveParts: activeParts, ZeroMergesIdle: zeroMergesIdle, Timeout: timeout}, nil
			}
			if now.After(deadline) {
				return mergeWaitResult{ActiveParts: activeParts, ZeroMergesIdle: zeroMergesIdle, Timeout: timeout}, nil
			}
		} else {
			zeroMergesStablePartsSince = time.Time{}
			zeroMergesActiveParts = 0
		}
		if now.After(deadline) {
			return mergeWaitResult{ActiveMerges: count, Timeout: timeout}, nil
		}
		select {
		case <-ctx.Done():
			return mergeWaitResult{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (p Processor) activePartCount(ctx context.Context, database, table string) (uint64, error) {
	query := "SELECT count() FROM system.parts WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) +
		" AND active FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return 0, err
	}
	count, err := chhttp.ParseUInt(out)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func defaultWorkDir(workDir string) string {
	if workDir == "" {
		return "/tmp/partforge"
	}
	return workDir
}

func workerFreezeName(m manifest.Manifest, frozenAt time.Time) string {
	frozenAt = frozenAt.UTC()
	timestamp := frozenAt.Format("20060102T150405") + fmt.Sprintf("%09dZ", frozenAt.Nanosecond())
	return fmt.Sprintf("partforge_%s_%s_%s", clickHouseBackupNameSegment(m.JobID), clickHouseBackupNameSegment(m.PartID), timestamp)
}

func clickHouseBackupNameSegment(value string) string {
	var b strings.Builder
	for _, r := range value {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	if b.Len() == 0 {
		return "empty"
	}
	return b.String()
}

func frozenPartUploadGlobs(disks []freeze.Disk, freezeName string) ([]frozenPartGlob, error) {
	var globs []frozenPartGlob
	var roots []string
	for _, disk := range disks {
		root := filepath.Join(disk.Path, "shadow", freezeName, "store")
		roots = append(roots, root)
		info, err := os.Stat(root)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat frozen store root %s: %w", root, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("frozen store root %s is not a directory", root)
		}
		globs = append(globs, frozenPartGlob{Disk: disk.Name, Glob: filepath.Join(root, "*", "*", "*")})
	}
	if len(globs) == 0 {
		return nil, fmt.Errorf("no frozen store roots found: %s", strings.Join(roots, ", "))
	}
	return globs, nil
}

func (p Processor) uploadFinishedArtifact(ctx context.Context, m manifest.Manifest, tarDir string, frozenPartGlobs []frozenPartGlob, stageTracker *rewriteStageTracker) error {
	bucket := m.S3.Bucket
	finishedKey := m.S3.FinishedKey
	if len(frozenPartGlobs) == 0 {
		return fmt.Errorf("no frozen part globs to upload for finished artifact s3://%s/%s", bucket, finishedKey)
	}
	partDirs, err := frozenPartDirs(frozenPartGlobs)
	if err != nil {
		return err
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageArchiveFinishedParts); err != nil {
		return err
	}
	slog.Info("creating finished artifact tarballs", "stage", "upload_finished", "bucket", bucket, "finished_key", finishedKey, "tar_dir", tarDir, "parts", len(partDirs))
	tarFiles, err := createFinishedPartTars(ctx, tarDir, partDirs)
	if err != nil {
		return fmt.Errorf("create finished artifact tarballs in %s: %w", tarDir, err)
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageDeleteFinishedArtifact); err != nil {
		return err
	}
	slog.Info("removing existing finished artifact prefix", "stage", "upload_finished", "bucket", bucket, "finished_key", finishedKey)
	if err := p.S3Copy.DeletePrefixIfExists(ctx, bucket, finishedKey); err != nil {
		return fmt.Errorf("delete existing finished artifact s3://%s/%s: %w", bucket, finishedKey, err)
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageUploadFinishedTarballs); err != nil {
		return err
	}
	slog.Info("uploading finished artifact tarballs", "stage", "upload_finished", "bucket", bucket, "finished_key", finishedKey, "tar_dir", tarDir, "tarballs", len(tarFiles))
	if err := p.S3Copy.UploadDir(ctx, tarDir, bucket, finishedKey); err != nil {
		return fmt.Errorf("upload finished artifact tarballs from %s to s3://%s/%s: %w", tarDir, bucket, finishedKey, err)
	}
	return nil
}

func frozenPartDirs(frozenPartGlobs []frozenPartGlob) ([]string, error) {
	var partDirs []string
	for _, source := range frozenPartGlobs {
		matches, err := filepath.Glob(source.Glob)
		if err != nil {
			return nil, fmt.Errorf("expand frozen part glob %s: %w", source.Glob, err)
		}
		sort.Strings(matches)
		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil {
				return nil, fmt.Errorf("stat frozen part %s: %w", match, err)
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("frozen part match %s is not a directory", match)
			}
			partDirs = append(partDirs, match)
		}
	}
	if len(partDirs) == 0 {
		return nil, fmt.Errorf("no frozen part directories matched for finished artifact")
	}
	return partDirs, nil
}

func createFinishedPartTars(ctx context.Context, tarDir string, partDirs []string) ([]string, error) {
	if len(partDirs) == 0 {
		return nil, fmt.Errorf("no finished part directories to archive")
	}
	if err := os.RemoveAll(tarDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tarDir, 0o755); err != nil {
		return nil, err
	}

	tarFiles := make([]string, len(partDirs))
	seen := map[string]struct{}{}
	for i, partDir := range partDirs {
		partName := filepath.Base(filepath.Clean(partDir))
		if partName == "." || partName == string(filepath.Separator) {
			return nil, fmt.Errorf("invalid finished part directory %q", partDir)
		}
		tarName := partName + manifest.FinishedTarSuffix
		if _, ok := seen[tarName]; ok {
			return nil, fmt.Errorf("duplicate finished part tarball name %q", tarName)
		}
		seen[tarName] = struct{}{}
		tarFiles[i] = filepath.Join(tarDir, tarName)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(partDirs) {
		workers = len(partDirs)
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}

	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := artifact.WriteFinishedTar(tarFiles[idx], []string{partDirs[idx]}); err != nil {
					setErr(fmt.Errorf("create finished part tarball %s: %w", tarFiles[idx], err))
					return
				}
			}
		}()
	}

sendJobs:
	for idx := range partDirs {
		select {
		case <-ctx.Done():
			break sendJobs
		case jobs <- idx:
		}
	}
	close(jobs)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tarFiles, nil
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
