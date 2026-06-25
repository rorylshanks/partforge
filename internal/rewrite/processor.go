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

const DefaultMergeTimeout = 5 * time.Minute
const DefaultMergeMaxTimeout = time.Hour
const DefaultMergeSettleMinWait = time.Minute
const DefaultCompactMergeTimeout = 15 * time.Minute
const DefaultCompactMergeMaxTimeout = 2 * time.Hour
const DefaultCompactMergeSettleMinWait = 2 * time.Minute
const DefaultCompactOptimizeFinalAfter = 30 * time.Second
const DefaultMergeSettleMinParts uint64 = 1
const defaultMergePollInterval = time.Second
const defaultMergeWaitLogInterval = 30 * time.Second

const (
	autoMergeTargetPartCount             uint64 = 4
	minMergeMaxBytesAtMaxSpaceInPool     uint64 = 16 * 1024 * 1024 * 1024
	maxMergeMaxBytesAtMaxSpaceInPool     uint64 = 1024 * 1024 * 1024 * 1024
	minMergeMaxBytesAtMinSpaceInPool     uint64 = 1024 * 1024 * 1024
	maxMergeMaxBytesAtMinSpaceInPool     uint64 = 128 * 1024 * 1024 * 1024
	mergeMaxBytesAtMinSpacePoolDivisor   uint64 = 16
	defaultMergeMaxBytesAtMaxSpaceInPool uint64 = 16 * 1024 * 1024 * 1024
)

const (
	stageProcessPart             = "process_part"
	stagePrepareWorkDir          = "prepare_work_dir"
	stageDownloadSource          = "download_source"
	stageReadManifest            = "read_manifest"
	stagePrepareWorkerTables     = "prepare_worker_tables"
	stageConfigureCompression    = "configure_compression"
	stageAttachSourcePart        = "attach_source_part"
	stageInsertSelect            = "insert_select"
	stageConfigureMergeSettings  = "configure_merge_settings"
	stageRestartClickHouse       = "restart_clickhouse"
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
	stageConfigureCompression,
	stageAttachSourcePart,
	stageInsertSelect,
	stageConfigureMergeSettings,
	stageRestartClickHouse,
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
	MergeMaxTimeout     time.Duration
	Metrics             metrics.Recorder
	InsertSettings      chhttp.QuerySettings
	ProgressInterval    time.Duration
	ReportProgress      ProgressReporter
	MergeTreeSettings   MergeTreeSettings
	MergeSettleMinWait  time.Duration
	MergeSettleMinParts uint64
	MergePollInterval   time.Duration
	OptimizeFinalAfter  time.Duration
	RestartClickHouse   func(context.Context) error
	mergeWaitHook       func(context.Context, mergeWaitTarget, mergePartSnapshot, uint64) (bool, error)
}

type MergeTreeSettings struct {
	MergeMaxBlockSize       uint64
	MergeMaxBlockSizeBytes  uint64
	MergeSelectingSleepMS   uint64
	DefaultCompressionCodec string
}

type ProgressReporter func(context.Context, manifest.Manifest, ProgressSnapshot) error

type ProgressSnapshot struct {
	QueryProgress              *metrics.QueryProgress
	SourceActivePartStats      *metrics.PartStats
	DestinationActivePartStats *metrics.PartStats
	DestinationFailedMerges    *uint64
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
	FinishedKey           string
	DestinationDatabase   string
	DestinationTable      string
	DestinationSchema     string
	DestinationStats      metrics.PartStats
	DestinationPartitions []PartPartitionStats
}

type frozenPartGlob struct {
	Disk string
	Glob string
}

type rewriteResult struct {
	FrozenPartGlobs       []frozenPartGlob
	DestinationStats      metrics.PartStats
	DestinationPartitions []PartPartitionStats
}

type PartPartitionStats struct {
	PartitionID string
	Parts       uint64
	Rows        uint64
	Bytes       uint64
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
	return ProcessResult{
		FinishedKey:           m.S3.FinishedKey,
		DestinationDatabase:   m.Dest.Database,
		DestinationTable:      m.Dest.Table,
		DestinationSchema:     m.SQL.DestinationSchema,
		DestinationStats:      rewriteResult.DestinationStats,
		DestinationPartitions: rewriteResult.DestinationPartitions,
	}, nil
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

	if err := p.reportStageProgress(ctx, m, stageTracker, stageConfigureCompression); err != nil {
		return rewriteResult{}, err
	}
	if err := p.configureDestinationCompressionCodec(ctx, m); err != nil {
		return rewriteResult{}, err
	}

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
	slog.Info("attached source part", "stage", "attach_source_part", "job_id", m.JobID, "part_id", m.PartID, "active_parts", sourceStats.Count, "active_rows", sourceStats.Rows, "active_bytes_on_disk", sourceStats.Bytes)

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
	mergeTarget := mergeWaitTarget{
		JobID:    m.JobID,
		PartID:   m.PartID,
		Database: m.Dest.Database,
		Table:    m.Dest.Table,
	}
	if _, err := p.waitForDestinationMerges(ctx, m, stageTracker, mergeTarget, "after_restart"); err != nil {
		return rewriteResult{}, err
	}
	if err := p.reportStageProgress(ctx, m, stageTracker, stageMeasureDestinationParts); err != nil {
		return rewriteResult{}, err
	}
	var destinationFailedMerges *uint64
	failedMerges, err := p.destinationFailedMergeCount(ctx, mergeTarget)
	if err != nil {
		if ctx.Err() != nil {
			return rewriteResult{}, fmt.Errorf("measure destination failed merges: %w", err)
		}
		slog.Warn(
			"could not measure destination failed merges",
			"stage", stageMeasureDestinationParts,
			"job_id", m.JobID,
			"part_id", m.PartID,
			"destination_table", mergeTarget.tableSQL(),
			"error", err,
		)
	} else {
		destinationFailedMerges = &failedMerges
	}
	destPartitions, err := p.activePartPartitionStats(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return rewriteResult{}, fmt.Errorf("measure destination active part partitions: %w", err)
	}
	destStats := summarizePartPartitions(destPartitions)
	result.DestinationStats = destStats
	result.DestinationPartitions = destPartitions
	p.recorder().SetActivePartStats("destination", m, destStats)
	if err := p.reportProgress(ctx, m, ProgressSnapshot{DestinationActivePartStats: &destStats, DestinationFailedMerges: destinationFailedMerges}); err != nil {
		return rewriteResult{}, err
	}
	logArgs := []any{"stage", stageMeasureDestinationParts, "job_id", m.JobID, "part_id", m.PartID, "active_parts", destStats.Count, "active_rows", destStats.Rows, "active_bytes_on_disk", destStats.Bytes}
	if destinationFailedMerges != nil {
		logArgs = append(logArgs, "failed_merges", *destinationFailedMerges)
	}
	slog.Info("measured destination parts", logArgs...)

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
			if settingsErr := p.configureDestinationCompressionCodec(ctx, m); settingsErr != nil {
				return fmt.Errorf("insert-select failed with retryable resource error (%w), but configure destination compression codec after reset failed: %v", err, settingsErr)
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

func (p Processor) configureDestinationCompressionCodec(ctx context.Context, m manifest.Manifest) error {
	mergeTreeSettings := p.MergeTreeSettings
	table := chhttp.TableSQL(m.Dest.Database, m.Dest.Table)
	if strings.TrimSpace(mergeTreeSettings.DefaultCompressionCodec) == "" {
		return fmt.Errorf("default_compression_codec must not be empty")
	}
	query := "ALTER TABLE " + table +
		" MODIFY SETTING default_compression_codec = " + chhttp.StringLiteral(mergeTreeSettings.DefaultCompressionCodec)
	if err := p.ClickHouse.Exec(ctx, query); err != nil {
		return fmt.Errorf("configure destination compression codec: %w", err)
	}
	slog.Info(
		"configured destination compression codec",
		"stage", stageConfigureCompression,
		"job_id", m.JobID,
		"part_id", m.PartID,
		"destination_table", table,
		"default_compression_codec", mergeTreeSettings.DefaultCompressionCodec,
	)
	return nil
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
	stats, err := p.activePartStats(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return fmt.Errorf("measure destination parts before configuring merge settings: %w", err)
	}
	mergeBytes := mergePoolByteSettingsForActiveBytes(stats.Bytes)
	query := "ALTER TABLE " + table +
		" MODIFY SETTING merge_max_block_size = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSize, 10) +
		", merge_max_block_size_bytes = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSizeBytes, 10) +
		", merge_selecting_sleep_ms = " + strconv.FormatUint(mergeTreeSettings.MergeSelectingSleepMS, 10) +
		", max_bytes_to_merge_at_max_space_in_pool = " + strconv.FormatUint(mergeBytes.MaxBytesAtMaxSpaceInPool, 10) +
		", max_bytes_to_merge_at_min_space_in_pool = " + strconv.FormatUint(mergeBytes.MaxBytesAtMinSpaceInPool, 10)
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
		"destination_active_parts", stats.Count,
		"destination_active_bytes_on_disk", stats.Bytes,
		"max_bytes_to_merge_at_max_space_in_pool", mergeBytes.MaxBytesAtMaxSpaceInPool,
		"max_bytes_to_merge_at_min_space_in_pool", mergeBytes.MaxBytesAtMinSpaceInPool,
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

type mergePoolByteSettings struct {
	MaxBytesAtMaxSpaceInPool uint64
	MaxBytesAtMinSpaceInPool uint64
}

func mergePoolByteSettingsForActiveBytes(activeBytes uint64) mergePoolByteSettings {
	maxAtMaxSpace := defaultMergeMaxBytesAtMaxSpaceInPool
	if activeBytes > 0 {
		target := ceilDivUint64(activeBytes, autoMergeTargetPartCount)
		maxAtMaxSpace = maxUint64(target, minMergeMaxBytesAtMaxSpaceInPool)
		maxAtMaxSpace = minUint64(maxAtMaxSpace, maxMergeMaxBytesAtMaxSpaceInPool)
	}
	if maxAtMaxSpace == 0 {
		maxAtMaxSpace = 1
	}

	maxAtMinSpace := ceilDivUint64(maxAtMaxSpace, mergeMaxBytesAtMinSpacePoolDivisor)
	maxAtMinSpace = clampUint64(maxAtMinSpace, minMergeMaxBytesAtMinSpaceInPool, maxMergeMaxBytesAtMinSpaceInPool)
	maxAtMinSpace = minUint64(maxAtMinSpace, maxAtMaxSpace)
	if maxAtMinSpace == 0 {
		maxAtMinSpace = 1
	}

	return mergePoolByteSettings{
		MaxBytesAtMaxSpaceInPool: maxAtMaxSpace,
		MaxBytesAtMinSpaceInPool: maxAtMinSpace,
	}
}

func ceilDivUint64(value, divisor uint64) uint64 {
	if divisor == 0 {
		return value
	}
	if value == 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func clampUint64(value, minValue, maxValue uint64) uint64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func (p Processor) waitForDestinationMerges(ctx context.Context, m manifest.Manifest, stageTracker *rewriteStageTracker, target mergeWaitTarget, waitContext string) (bool, error) {
	if err := p.reportStageProgress(ctx, m, stageTracker, stageWaitMerges); err != nil {
		return false, err
	}
	slog.Info("waiting for destination merges", "stage", stageWaitMerges, "job_id", m.JobID, "part_id", m.PartID, "destination_table", target.tableSQL(), "wait_context", waitContext)
	mergeWait, err := p.waitForMerges(ctx, target)
	if err != nil && ctx.Err() != nil {
		return false, err
	}
	if err != nil {
		slog.Warn(
			"destination merge wait failed; continuing with current destination parts",
			"stage", stageWaitMerges,
			"job_id", m.JobID,
			"part_id", m.PartID,
			"destination_table", target.tableSQL(),
			"wait_context", waitContext,
			"error", err,
		)
		return false, nil
	}
	if mergeWait.Settled {
		slog.Info(
			"destination merges complete",
			"stage", stageWaitMerges,
			"job_id", m.JobID,
			"part_id", m.PartID,
			"destination_table", target.tableSQL(),
			"wait_context", waitContext,
			"settle_reason", mergeWait.Reason,
			"active_parts", mergeWait.ActiveParts,
			"total_bytes_on_disk", mergeWait.TotalBytes,
			"largest_part_bytes_on_disk", mergeWait.LargestPartBytes,
			"zero_merges_idle", mergeWait.ZeroMergesIdle,
		)
		return true, nil
	}
	slog.Warn(
		"destination merges did not settle before timeout; continuing with current destination parts",
		"stage", stageWaitMerges,
		"job_id", m.JobID,
		"part_id", m.PartID,
		"destination_table", target.tableSQL(),
		"wait_context", waitContext,
		"wait_reason", mergeWait.Reason,
		"timeout", mergeWait.Timeout,
		"active_merges", mergeWait.ActiveMerges,
		"active_parts", mergeWait.ActiveParts,
		"total_bytes_on_disk", mergeWait.TotalBytes,
		"largest_part_bytes_on_disk", mergeWait.LargestPartBytes,
		"zero_merges_idle", mergeWait.ZeroMergesIdle,
	)
	return false, nil
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
}

func (p Processor) startProgressHeartbeat(ctx context.Context, m manifest.Manifest, tracker *rewriteStageTracker) (*progressHeartbeat, error) {
	heartbeat := &progressHeartbeat{ctx: ctx}
	if p.ReportProgress == nil || p.ProgressInterval <= 0 {
		return heartbeat, nil
	}

	heartbeat.ctx, heartbeat.cancel = context.WithCancel(ctx)
	heartbeat.done = make(chan struct{})
	if err := p.reportStageSnapshot(heartbeat.ctx, m, tracker); err != nil {
		slog.Warn("progress heartbeat update failed; continuing", "job_id", m.JobID, "part_id", m.PartID, "error", err)
	}

	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(p.ProgressInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := p.reportStageSnapshot(heartbeat.ctx, m, tracker); err != nil {
					if heartbeat.ctx.Err() != nil {
						return
					}
					slog.Warn("progress heartbeat update failed; continuing", "job_id", m.JobID, "part_id", m.PartID, "error", err)
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
	return nil
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

func (p Processor) activePartPartitionStats(ctx context.Context, database, table string) ([]PartPartitionStats, error) {
	query := "SELECT partition_id, count(), ifNull(sum(rows), 0), ifNull(sum(bytes_on_disk), 0) FROM system.parts WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) +
		" AND active GROUP BY partition_id ORDER BY partition_id FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 4)
	if err != nil {
		return nil, err
	}
	partitions := make([]PartPartitionStats, 0, len(rows))
	for _, row := range rows {
		count, err := chhttp.ParseUInt(row[1])
		if err != nil {
			return nil, err
		}
		partRows, err := chhttp.ParseUInt(row[2])
		if err != nil {
			return nil, err
		}
		bytes, err := chhttp.ParseUInt(row[3])
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, PartPartitionStats{
			PartitionID: row[0],
			Parts:       count,
			Rows:        partRows,
			Bytes:       bytes,
		})
	}
	return partitions, nil
}

func summarizePartPartitions(partitions []PartPartitionStats) metrics.PartStats {
	var stats metrics.PartStats
	for _, partition := range partitions {
		stats.Count += partition.Parts
		stats.Rows += partition.Rows
		stats.Bytes += partition.Bytes
	}
	return stats
}

func (p Processor) destinationFailedMergeCount(ctx context.Context, target mergeWaitTarget) (uint64, error) {
	if err := p.ClickHouse.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		return 0, fmt.Errorf("flush ClickHouse logs before measuring destination failed merges: %w", err)
	}
	query := "SELECT count() FROM system.part_log WHERE database = " +
		chhttp.StringLiteral(target.Database) + " AND table = " + chhttp.StringLiteral(target.Table) +
		" AND event_type = 'MergeParts' AND error != 0 FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("measure destination failed merges from system.part_log: %w", err)
	}
	count, err := chhttp.ParseUInt(out)
	if err != nil {
		return 0, err
	}
	return count, nil
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
	Settled          bool
	Reason           string
	ActiveMerges     uint64
	ActiveParts      uint64
	TotalBytes       uint64
	LargestPartBytes uint64
	ZeroMergesIdle   time.Duration
	Timeout          time.Duration
	MaxTimeout       time.Duration
}

type mergeWaitTarget struct {
	JobID    string
	PartID   string
	Database string
	Table    string
}

func (t mergeWaitTarget) tableSQL() string {
	return chhttp.TableSQL(t.Database, t.Table)
}

type mergeWaitLogState struct {
	lastAt     time.Time
	lastReason string
}

func (p Processor) waitForMerges(ctx context.Context, target mergeWaitTarget) (mergeWaitResult, error) {
	timeout := p.MergeTimeout
	if timeout == 0 {
		timeout = DefaultMergeTimeout
	}
	if timeout < 0 {
		return mergeWaitResult{}, fmt.Errorf("merge timeout must be non-negative, got %s", timeout)
	}
	maxTimeout := p.MergeMaxTimeout
	if maxTimeout == 0 {
		maxTimeout = DefaultMergeMaxTimeout
	}
	if maxTimeout < 0 {
		return mergeWaitResult{}, fmt.Errorf("merge max timeout must be non-negative, got %s", maxTimeout)
	}
	if maxTimeout < timeout {
		maxTimeout = timeout
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
	optimizeFinalAfter := p.OptimizeFinalAfter
	if optimizeFinalAfter < 0 {
		return mergeWaitResult{}, fmt.Errorf("optimize final idle wait must be non-negative, got %s", optimizeFinalAfter)
	}
	startedAt := time.Now()
	lastActivityAt := startedAt
	baseDeadline := lastActivityAt.Add(timeout)
	maxDeadline := startedAt.Add(maxTimeout)
	var zeroMergesStableSnapshotSince time.Time
	var zeroMergesSnapshot mergePartSnapshot
	var previousSnapshot mergePartSnapshot
	var optimizeFinalSnapshot mergePartSnapshot
	havePreviousSnapshot := false
	optimizeFinalAttempted := false
	logState := mergeWaitLogState{}
	for {
		count, err := p.destinationMergeCount(ctx, target)
		if err != nil {
			return mergeWaitResult{}, err
		}
		now := time.Now()
		snapshot, err := p.mergePartSnapshot(ctx, target)
		if err != nil {
			return mergeWaitResult{}, err
		}
		if !havePreviousSnapshot || !sameMergePartSnapshot(snapshot, previousSnapshot) || count > 0 {
			lastActivityAt = now
			baseDeadline = lastActivityAt.Add(timeout)
			previousSnapshot = snapshot
			havePreviousSnapshot = true
			optimizeFinalAttempted = false
			optimizeFinalSnapshot = mergePartSnapshot{}
		}
		if p.mergeWaitHook != nil {
			changed, err := p.mergeWaitHook(ctx, target, snapshot, count)
			if err != nil {
				return mergeWaitResult{}, err
			}
			if changed {
				lastActivityAt = now
				baseDeadline = lastActivityAt.Add(timeout)
				zeroMergesStableSnapshotSince = time.Time{}
				zeroMergesSnapshot = mergePartSnapshot{}
				havePreviousSnapshot = false
				optimizeFinalAttempted = false
				optimizeFinalSnapshot = mergePartSnapshot{}
				continue
			}
		}

		zeroMergesIdle := time.Duration(0)
		reason := "active_destination_merges"
		if count == 0 {
			if snapshot.ActiveParts <= minParts {
				return mergeWaitResult{
					Settled:          true,
					Reason:           "active_parts_settled",
					ActiveParts:      snapshot.ActiveParts,
					TotalBytes:       snapshot.TotalBytes,
					LargestPartBytes: snapshot.LargestPartBytes,
					Timeout:          timeout,
					MaxTimeout:       maxTimeout,
				}, nil
			}
			if zeroMergesStableSnapshotSince.IsZero() || !sameMergePartSnapshot(snapshot, zeroMergesSnapshot) {
				zeroMergesStableSnapshotSince = now
				zeroMergesSnapshot = snapshot
			}
			zeroMergesIdle = now.Sub(zeroMergesStableSnapshotSince)
			if zeroMergesIdle >= minWait {
				return mergeWaitResult{
					Settled:          true,
					Reason:           "destination_merges_idle",
					ActiveParts:      snapshot.ActiveParts,
					TotalBytes:       snapshot.TotalBytes,
					LargestPartBytes: snapshot.LargestPartBytes,
					ZeroMergesIdle:   zeroMergesIdle,
					Timeout:          timeout,
					MaxTimeout:       maxTimeout,
				}, nil
			}
			if !now.Before(maxDeadline) {
				return mergeWaitResult{
					Reason:           "merge_max_timeout_no_destination_merges",
					ActiveParts:      snapshot.ActiveParts,
					TotalBytes:       snapshot.TotalBytes,
					LargestPartBytes: snapshot.LargestPartBytes,
					ZeroMergesIdle:   zeroMergesIdle,
					Timeout:          maxTimeout,
					MaxTimeout:       maxTimeout,
				}, nil
			}
			if optimizeFinalAfter > 0 && snapshot.ActiveParts > minParts && now.Sub(lastActivityAt) >= optimizeFinalAfter &&
				(!optimizeFinalAttempted || !sameMergePartSnapshot(snapshot, optimizeFinalSnapshot)) {
				optimizeFinalAttempted = true
				optimizeFinalSnapshot = snapshot
				optimizeCtx, cancelOptimize := context.WithDeadline(ctx, maxDeadline)
				optimizePartitionIDs, err := p.destinationMergeablePartitions(optimizeCtx, target)
				if err != nil {
					cancelOptimize()
					return mergeWaitResult{}, err
				}
				if len(optimizePartitionIDs) == 0 {
					cancelOptimize()
					slog.Info(
						"skipping optimize final after idle destination merges because no partition has multiple active parts",
						"stage", stageWaitMerges,
						"job_id", target.JobID,
						"part_id", target.PartID,
						"destination_table", target.tableSQL(),
						"idle", nonNegativeDuration(now.Sub(lastActivityAt)),
						"threshold", optimizeFinalAfter,
						"active_parts", snapshot.ActiveParts,
						"total_bytes_on_disk", snapshot.TotalBytes,
						"largest_part_bytes_on_disk", snapshot.LargestPartBytes,
					)
				} else {
					slog.Info(
						"running optimize final after idle destination merges",
						"stage", stageWaitMerges,
						"job_id", target.JobID,
						"part_id", target.PartID,
						"destination_table", target.tableSQL(),
						"idle", nonNegativeDuration(now.Sub(lastActivityAt)),
						"threshold", optimizeFinalAfter,
						"active_parts", snapshot.ActiveParts,
						"total_bytes_on_disk", snapshot.TotalBytes,
						"largest_part_bytes_on_disk", snapshot.LargestPartBytes,
						"partitions", len(optimizePartitionIDs),
					)
					err := p.optimizeFinalPartitions(optimizeCtx, target, optimizePartitionIDs)
					cancelOptimize()
					if err != nil {
						return mergeWaitResult{}, err
					}
					lastActivityAt = now
					baseDeadline = lastActivityAt.Add(timeout)
					zeroMergesStableSnapshotSince = time.Time{}
					zeroMergesSnapshot = mergePartSnapshot{}
					continue
				}
			}
			if !now.Before(baseDeadline) {
				return mergeWaitResult{
					Reason:           "merge_timeout_no_destination_merges",
					ActiveParts:      snapshot.ActiveParts,
					TotalBytes:       snapshot.TotalBytes,
					LargestPartBytes: snapshot.LargestPartBytes,
					ZeroMergesIdle:   zeroMergesIdle,
					Timeout:          timeout,
					MaxTimeout:       maxTimeout,
				}, nil
			}
			reason = "waiting_for_destination_merge_selection"
		} else {
			zeroMergesStableSnapshotSince = time.Time{}
			zeroMergesSnapshot = mergePartSnapshot{}
		}
		if !now.Before(maxDeadline) {
			return mergeWaitResult{
				Reason:           "merge_max_timeout",
				ActiveMerges:     count,
				ActiveParts:      snapshot.ActiveParts,
				TotalBytes:       snapshot.TotalBytes,
				LargestPartBytes: snapshot.LargestPartBytes,
				Timeout:          maxTimeout,
				MaxTimeout:       maxTimeout,
			}, nil
		}
		if !now.Before(baseDeadline) {
			return mergeWaitResult{
				Reason:           "merge_timeout",
				ActiveMerges:     count,
				ActiveParts:      snapshot.ActiveParts,
				TotalBytes:       snapshot.TotalBytes,
				LargestPartBytes: snapshot.LargestPartBytes,
				Timeout:          timeout,
				MaxTimeout:       maxTimeout,
			}, nil
		}
		var optimizeDeadline time.Time
		if optimizeFinalAfter > 0 && count == 0 && snapshot.ActiveParts > minParts &&
			(!optimizeFinalAttempted || !sameMergePartSnapshot(snapshot, optimizeFinalSnapshot)) {
			optimizeDeadline = lastActivityAt.Add(optimizeFinalAfter)
		}
		sleep := mergeWaitSleepDuration(now, pollInterval, baseDeadline, maxDeadline, optimizeDeadline)
		logState.maybeLog(
			now,
			target,
			reason,
			count,
			snapshot,
			zeroMergesIdle,
			timeout,
			maxTimeout,
			minWait,
			startedAt,
			baseDeadline,
			maxDeadline,
			sleep,
		)
		select {
		case <-ctx.Done():
			return mergeWaitResult{}, ctx.Err()
		case <-time.After(sleep):
		}
	}
}

type mergePartSnapshot struct {
	ActiveParts      uint64
	TotalBytes       uint64
	LargestPartBytes uint64
}

func (p Processor) destinationMergeCount(ctx context.Context, target mergeWaitTarget) (uint64, error) {
	query := "SELECT count() FROM system.merges WHERE database = " +
		chhttp.StringLiteral(target.Database) + " AND table = " + chhttp.StringLiteral(target.Table) + " FORMAT TSV"
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

func (p Processor) destinationMergeablePartitions(ctx context.Context, target mergeWaitTarget) ([]string, error) {
	partitions, err := p.activePartPartitionStats(ctx, target.Database, target.Table)
	if err != nil {
		return nil, err
	}
	partitionIDs := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		if partition.Parts > 1 {
			partitionIDs = append(partitionIDs, partition.PartitionID)
		}
	}
	return partitionIDs, nil
}

func (s *mergeWaitLogState) maybeLog(
	now time.Time,
	target mergeWaitTarget,
	reason string,
	activeMerges uint64,
	snapshot mergePartSnapshot,
	zeroMergesIdle time.Duration,
	timeout time.Duration,
	maxTimeout time.Duration,
	minWait time.Duration,
	startedAt time.Time,
	baseDeadline time.Time,
	maxDeadline time.Time,
	nextPollIn time.Duration,
) {
	if !s.lastAt.IsZero() && reason == s.lastReason && now.Sub(s.lastAt) < defaultMergeWaitLogInterval {
		return
	}
	s.lastAt = now
	s.lastReason = reason
	slog.Info(
		"destination merge wait state",
		"stage", stageWaitMerges,
		"job_id", target.JobID,
		"part_id", target.PartID,
		"destination_table", target.tableSQL(),
		"merge_scope", "destination_table",
		"wait_reason", reason,
		"elapsed", nonNegativeDuration(now.Sub(startedAt)),
		"timeout", timeout,
		"base_timeout_remaining", nonNegativeDuration(baseDeadline.Sub(now)),
		"max_timeout", maxTimeout,
		"max_timeout_remaining", nonNegativeDuration(maxDeadline.Sub(now)),
		"active_destination_merges", activeMerges,
		"active_parts", snapshot.ActiveParts,
		"total_bytes_on_disk", snapshot.TotalBytes,
		"largest_part_bytes_on_disk", snapshot.LargestPartBytes,
		"zero_merges_idle", zeroMergesIdle,
		"zero_merges_required", minWait,
		"next_poll_in", nextPollIn,
	)
}

func (p Processor) mergePartSnapshot(ctx context.Context, target mergeWaitTarget) (mergePartSnapshot, error) {
	query := "SELECT count(), ifNull(sum(bytes_on_disk), 0), ifNull(max(bytes_on_disk), 0) FROM system.parts WHERE database = " +
		chhttp.StringLiteral(target.Database) + " AND table = " + chhttp.StringLiteral(target.Table) +
		" AND active FORMAT TSV"
	out, err := p.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return mergePartSnapshot{}, err
	}
	rows, err := chhttp.FormatTSVStrings(out, 3)
	if err != nil {
		return mergePartSnapshot{}, err
	}
	if len(rows) != 1 {
		return mergePartSnapshot{}, fmt.Errorf("expected one active part merge snapshot row, got %d", len(rows))
	}
	activeParts, err := chhttp.ParseUInt(rows[0][0])
	if err != nil {
		return mergePartSnapshot{}, err
	}
	totalBytes, err := chhttp.ParseUInt(rows[0][1])
	if err != nil {
		return mergePartSnapshot{}, err
	}
	largestPartBytes, err := chhttp.ParseUInt(rows[0][2])
	if err != nil {
		return mergePartSnapshot{}, err
	}
	return mergePartSnapshot{
		ActiveParts:      activeParts,
		TotalBytes:       totalBytes,
		LargestPartBytes: largestPartBytes,
	}, nil
}

func sameMergePartSnapshot(a, b mergePartSnapshot) bool {
	return a.ActiveParts == b.ActiveParts &&
		a.TotalBytes == b.TotalBytes &&
		a.LargestPartBytes == b.LargestPartBytes
}

func (p Processor) optimizeFinalPartitions(ctx context.Context, target mergeWaitTarget, partitionIDs []string) error {
	for _, partitionID := range partitionIDs {
		query := "OPTIMIZE TABLE " + target.tableSQL() + " PARTITION ID " + chhttp.StringLiteral(partitionID) + " FINAL"
		if err := p.ClickHouse.Exec(ctx, query); err != nil {
			return fmt.Errorf("optimize final partition %q for %s: %w", partitionID, target.tableSQL(), err)
		}
	}
	return nil
}

func mergeWaitSleepDuration(now time.Time, pollInterval time.Duration, deadlines ...time.Time) time.Duration {
	sleep := pollInterval
	for _, deadline := range deadlines {
		if deadline.IsZero() {
			continue
		}
		remaining := deadline.Sub(now)
		if remaining > 0 && remaining < sleep {
			sleep = remaining
		}
	}
	if sleep <= 0 {
		return time.Nanosecond
	}
	return sleep
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
