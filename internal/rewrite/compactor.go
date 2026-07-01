package rewrite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/PostHog/partforge/internal/artifact"
	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/ddl"
	"github.com/PostHog/partforge/internal/fileutil"
	"github.com/PostHog/partforge/internal/freeze"
	"github.com/PostHog/partforge/internal/manifest"
	"github.com/PostHog/partforge/internal/metrics"
	"github.com/PostHog/partforge/internal/s3copy"
)

type Compactor struct {
	S3Copy              s3copy.Copier
	ClickHouse          chhttp.Client
	WorkDir             string
	MergeTimeout        time.Duration
	MergeMaxTimeout     time.Duration
	MergeSettleMinWait  time.Duration
	MergeSettleMinParts uint64
	MergePollInterval   time.Duration
	MergeDeadline       time.Time
	OptimizeFinalAfter  time.Duration
	MergeTreeSettings   MergeTreeSettings
	RestartClickHouse   func(context.Context) error
	ReportProgress      CompactProgressReporter
	ShutdownContext     context.Context
	MergeStopContext    context.Context
}

type CompactInput struct {
	PartID          string
	Bucket          string
	FinishedKey     string
	Parts           uint64
	Rows            uint64
	Bytes           uint64
	PartitionCounts map[string]uint64
}

type CompactProgressReporter func(context.Context, CompactWorkItem, CompactProgressSnapshot) error

type CompactProgressSnapshot struct {
	InputStats       metrics.PartStats
	DestinationStats metrics.PartStats
}

type CompactWorkItem struct {
	JobID               string
	OutputPartID        string
	OutputFinishedKey   string
	DestinationDatabase string
	DestinationTable    string
	DestinationSchema   string
	Inputs              []CompactInput
}

type CompactResult struct {
	OutputPartID          string
	FinishedKey           string
	Reduced               bool
	InputStats            metrics.PartStats
	DestinationStats      metrics.PartStats
	DestinationPartitions []PartPartitionStats
	Inputs                []CompactInput
}

func (c Compactor) Compact(ctx context.Context, item CompactWorkItem) (CompactResult, error) {
	if item.JobID == "" || item.OutputPartID == "" || item.OutputFinishedKey == "" {
		return CompactResult{}, fmt.Errorf("compact work item is missing job id, output part id, or output finished key")
	}
	if item.DestinationDatabase == "" || item.DestinationTable == "" || strings.TrimSpace(item.DestinationSchema) == "" {
		return CompactResult{}, fmt.Errorf("compact work item is missing destination table or schema")
	}
	if len(item.Inputs) == 0 {
		return CompactResult{}, fmt.Errorf("compact work item has no input artifacts")
	}
	for _, input := range item.Inputs {
		if input.PartID == "" || input.Bucket == "" || input.FinishedKey == "" {
			return CompactResult{}, fmt.Errorf("compact input artifact is missing part id, bucket, or finished key")
		}
	}
	attachedInputs := append([]CompactInput(nil), item.Inputs...)

	root := filepath.Join(defaultWorkDir(c.WorkDir), item.JobID, item.OutputPartID)
	if err := os.RemoveAll(root); err != nil {
		return CompactResult{}, err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return CompactResult{}, err
	}

	p := Processor{
		S3Copy:              c.S3Copy,
		ClickHouse:          c.ClickHouse,
		WorkDir:             root,
		MergeTimeout:        compactMergeTimeout(c.MergeTimeout),
		MergeMaxTimeout:     compactMergeMaxTimeout(c.MergeMaxTimeout),
		MergeSettleMinWait:  compactMergeSettleMinWait(c.MergeSettleMinWait),
		MergeSettleMinParts: c.MergeSettleMinParts,
		MergePollInterval:   c.MergePollInterval,
		OptimizeFinalAfter:  compactOptimizeFinalAfter(c.OptimizeFinalAfter),
		MergeTreeSettings:   c.MergeTreeSettings,
		RestartClickHouse:   c.RestartClickHouse,
	}
	m := manifest.Manifest{
		Version: manifest.Version,
		JobID:   item.JobID,
		PartID:  item.OutputPartID,
		Dest:    manifest.TableRef{Database: item.DestinationDatabase, Table: item.DestinationTable},
		SQL:     manifest.SQLBundle{DestinationSchema: item.DestinationSchema},
		S3:      manifest.S3Refs{Bucket: item.Inputs[0].Bucket, FinishedKey: item.OutputFinishedKey},
	}

	phaseCtx, cancelPhase := c.phaseContext(ctx)
	defer cancelPhase()

	slog.Info("preparing compact destination table", "stage", "compact_prepare_table", "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable))
	if err := c.prepareDestinationTable(phaseCtx, item); err != nil {
		return CompactResult{}, err
	}
	if err := p.configureDestinationCompressionCodec(phaseCtx, m); err != nil {
		return CompactResult{}, err
	}
	dataPath, err := p.tableDataPath(phaseCtx, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return CompactResult{}, err
	}
	detached := filepath.Join(dataPath, "detached")
	if err := os.MkdirAll(detached, 0o755); err != nil {
		return CompactResult{}, err
	}

	var inputStats metrics.PartStats
	for idx, input := range item.Inputs {
		workDir := filepath.Join(root, "inputs", fmt.Sprintf("%06d", idx))
		stats, err := c.attachFinishedArtifact(phaseCtx, item, input, detached, workDir)
		if err != nil {
			return CompactResult{}, err
		}
		inputStats = addPartStats(inputStats, stats)
	}

	actualInputPartitions, err := p.activePartPartitionStats(phaseCtx, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return CompactResult{}, fmt.Errorf("measure compact input active part partitions: %w", err)
	}
	actualInputStats := summarizePartPartitions(actualInputPartitions)
	slog.Info("attached compact input artifacts", "stage", "compact_attach_inputs", "job_id", item.JobID, "part_id", item.OutputPartID, "input_artifacts", len(item.Inputs), "active_parts", actualInputStats.Count, "active_rows", actualInputStats.Rows, "active_bytes_on_disk", actualInputStats.Bytes)
	if err := c.reportProgress(phaseCtx, item, CompactProgressSnapshot{InputStats: inputStats, DestinationStats: actualInputStats}); err != nil {
		return CompactResult{}, err
	}
	if inputStats.Count < 2 {
		return CompactResult{OutputPartID: item.OutputPartID, InputStats: inputStats, DestinationStats: actualInputStats, DestinationPartitions: actualInputPartitions, Inputs: attachedInputs}, nil
	}

	if err := c.configureCompactMergeSettings(phaseCtx, item, actualInputStats.Bytes); err != nil {
		return CompactResult{}, err
	}
	if err := p.restartClickHouse(phaseCtx, m); err != nil {
		return CompactResult{}, err
	}
	target := mergeWaitTarget{
		JobID:    item.JobID,
		PartID:   item.OutputPartID,
		Database: item.DestinationDatabase,
		Table:    item.DestinationTable,
	}
	waitForMerges := true
	deadlineActive := false
	if mergeWaitTimeout, ok := compactMergeTimeoutUntil(c.MergeDeadline, time.Now()); ok {
		deadlineActive = true
		if mergeWaitTimeout <= 0 {
			waitForMerges = false
			slog.Info("compact merge deadline reached before destination merge wait; measuring current output", "stage", "compact_window_expired", "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable), "deadline", c.MergeDeadline)
		} else {
			p.MergeTimeout, p.MergeMaxTimeout = compactMergeTimeoutsForDeadline(p.MergeTimeout, p.MergeMaxTimeout, mergeWaitTimeout)
			slog.Info("using compact window as destination merge max timeout", "stage", stageWaitMerges, "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", target.tableSQL(), "timeout", p.MergeTimeout, "max_timeout", p.MergeMaxTimeout, "deadline", c.MergeDeadline)
		}
	}
	if waitForMerges {
		waitCtx := c.mergeWaitContext(phaseCtx)
		cancelWait := func() {}
		if deadlineActive {
			waitCtx, cancelWait = context.WithDeadline(waitCtx, c.MergeDeadline)
		}
		err := func() error {
			defer cancelWait()
			_, err := p.waitForDestinationMerges(waitCtx, m, nil, target, "compact")
			return err
		}()
		if err != nil {
			if deadlineActive && errors.Is(err, context.DeadlineExceeded) {
				slog.Info("compact merge deadline reached; measuring current output", "stage", "compact_window_expired", "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable), "deadline", c.MergeDeadline)
			} else if c.shutdownRequested() && errors.Is(err, context.Canceled) {
				slog.Info("compact merge wait interrupted by shutdown; measuring current output", "stage", "shutdown", "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable))
			} else if c.mergeStopRequested() && errors.Is(err, context.Canceled) {
				slog.Info("compact merge wait manually finalized; measuring current output", "stage", "manual_finalize_compact", "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable))
			} else {
				return CompactResult{}, err
			}
		}
	}
	cancelPhase()

	destPartitions, err := p.activePartPartitionStats(ctx, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return CompactResult{}, fmt.Errorf("measure compact output active part partitions: %w", err)
	}
	destStats := summarizePartPartitions(destPartitions)
	slog.Info("measured compact output parts", "stage", "compact_measure_output", "job_id", item.JobID, "part_id", item.OutputPartID, "input_parts", inputStats.Count, "output_parts", destStats.Count, "active_rows", destStats.Rows, "active_bytes_on_disk", destStats.Bytes)
	if err := c.reportProgress(ctx, item, CompactProgressSnapshot{InputStats: inputStats, DestinationStats: destStats}); err != nil {
		return CompactResult{}, err
	}
	if destStats.Count >= inputStats.Count {
		return CompactResult{OutputPartID: item.OutputPartID, InputStats: inputStats, DestinationStats: destStats, DestinationPartitions: destPartitions, Inputs: attachedInputs}, nil
	}

	freezeName := workerFreezeName(m, time.Now().UTC())
	if err := c.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable)+" FREEZE WITH NAME "+chhttp.StringLiteral(freezeName)); err != nil {
		return CompactResult{}, fmt.Errorf("freeze compact destination table %s: %w", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable), err)
	}
	disks, err := freeze.LocalDisks(ctx, c.ClickHouse)
	if err != nil {
		return CompactResult{}, err
	}
	frozenPartGlobs, err := frozenPartUploadGlobs(disks, freezeName)
	if err != nil {
		return CompactResult{}, err
	}
	tarDir := filepath.Join(root, "finished-tars")
	if err := p.uploadFinishedArtifact(ctx, m, tarDir, frozenPartGlobs, nil); err != nil {
		return CompactResult{}, fmt.Errorf("upload compact finished artifact %s: %w", item.OutputFinishedKey, err)
	}
	return CompactResult{
		OutputPartID:          item.OutputPartID,
		FinishedKey:           item.OutputFinishedKey,
		Reduced:               true,
		InputStats:            inputStats,
		DestinationStats:      destStats,
		DestinationPartitions: destPartitions,
		Inputs:                attachedInputs,
	}, nil
}

func (c Compactor) phaseContext(ctx context.Context) (context.Context, context.CancelFunc) {
	phaseCtx, cancel := context.WithCancel(ctx)
	if c.ShutdownContext == nil {
		return phaseCtx, cancel
	}
	go func() {
		select {
		case <-c.ShutdownContext.Done():
			cancel()
		case <-phaseCtx.Done():
		}
	}()
	return phaseCtx, cancel
}

func (c Compactor) mergeWaitContext(ctx context.Context) context.Context {
	if c.MergeStopContext == nil {
		return ctx
	}
	waitCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-c.MergeStopContext.Done():
			cancel()
		case <-waitCtx.Done():
		}
	}()
	return waitCtx
}

func (c Compactor) shutdownRequested() bool {
	return c.ShutdownContext != nil && c.ShutdownContext.Err() != nil
}

func (c Compactor) mergeStopRequested() bool {
	return c.MergeStopContext != nil && c.MergeStopContext.Err() != nil
}

func (c Compactor) reportProgress(ctx context.Context, item CompactWorkItem, snapshot CompactProgressSnapshot) error {
	if snapshot.DestinationStats.Count > snapshot.InputStats.Count {
		return fmt.Errorf("compact output active parts (%d) exceeds attached input parts (%d) for %s/%s", snapshot.DestinationStats.Count, snapshot.InputStats.Count, item.JobID, item.OutputPartID)
	}
	if c.ReportProgress == nil {
		return nil
	}
	return c.ReportProgress(ctx, item, snapshot)
}

func (c Compactor) prepareDestinationTable(ctx context.Context, item CompactWorkItem) error {
	destDDL, err := ddl.ForTable(item.DestinationSchema, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return fmt.Errorf("normalize compact destination DDL: %w", err)
	}
	if err := c.ClickHouse.Exec(ctx, "CREATE DATABASE "+chhttp.Ident(item.DestinationDatabase)); err != nil {
		return fmt.Errorf("create compact destination database %s: %w", item.DestinationDatabase, err)
	}
	if err := c.ClickHouse.Exec(ctx, destDDL); err != nil {
		return fmt.Errorf("create compact destination table: %w", err)
	}
	return nil
}

func (c Compactor) configureCompactMergeSettings(ctx context.Context, item CompactWorkItem, activeBytes uint64) error {
	mergeTreeSettings := c.MergeTreeSettings
	table := chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable)
	if mergeTreeSettings.MergeMaxBlockSize == 0 {
		return fmt.Errorf("merge_max_block_size must be greater than zero")
	}
	if mergeTreeSettings.MergeMaxBlockSizeBytes == 0 {
		return fmt.Errorf("merge_max_block_size_bytes must be greater than zero")
	}
	if mergeTreeSettings.MergeSelectingSleepMS == 0 {
		return fmt.Errorf("merge_selecting_sleep_ms must be greater than zero")
	}
	if mergeTreeSettings.PoolFreeEntriesThreshold == 0 {
		return fmt.Errorf("pool free entries threshold must be greater than zero")
	}
	mergeBytes := targetMergePoolByteSettings()
	query := "ALTER TABLE " + table +
		" MODIFY SETTING merge_max_block_size = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSize, 10) +
		", merge_max_block_size_bytes = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSizeBytes, 10) +
		", merge_selecting_sleep_ms = " + strconv.FormatUint(mergeTreeSettings.MergeSelectingSleepMS, 10) +
		", number_of_free_entries_in_pool_to_lower_max_size_of_merge = " + strconv.FormatUint(mergeTreeSettings.PoolFreeEntriesThreshold, 10) +
		", number_of_free_entries_in_pool_to_execute_mutation = " + strconv.FormatUint(mergeTreeSettings.PoolFreeEntriesThreshold, 10) +
		", number_of_free_entries_in_pool_to_execute_optimize_entire_partition = " + strconv.FormatUint(mergeTreeSettings.PoolFreeEntriesThreshold, 10) +
		", max_bytes_to_merge_at_max_space_in_pool = " + strconv.FormatUint(mergeBytes.MaxBytesAtMaxSpaceInPool, 10) +
		", max_bytes_to_merge_at_min_space_in_pool = " + strconv.FormatUint(mergeBytes.MaxBytesAtMinSpaceInPool, 10)
	if err := c.ClickHouse.Exec(ctx, query); err != nil {
		return fmt.Errorf("configure compact destination table merge settings: %w", err)
	}
	slog.Info(
		"configured compact destination merge settings",
		"stage", "compact_configure_merge_settings",
		"job_id", item.JobID,
		"part_id", item.OutputPartID,
		"destination_table", table,
		"merge_max_block_size", mergeTreeSettings.MergeMaxBlockSize,
		"merge_max_block_size_bytes", mergeTreeSettings.MergeMaxBlockSizeBytes,
		"merge_selecting_sleep_ms", mergeTreeSettings.MergeSelectingSleepMS,
		"pool_free_entries_threshold", mergeTreeSettings.PoolFreeEntriesThreshold,
		"destination_active_bytes_on_disk", activeBytes,
		"max_bytes_to_merge_at_max_space_in_pool", mergeBytes.MaxBytesAtMaxSpaceInPool,
		"max_bytes_to_merge_at_min_space_in_pool", mergeBytes.MaxBytesAtMinSpaceInPool,
	)
	return nil
}

func (c Compactor) attachFinishedArtifact(ctx context.Context, item CompactWorkItem, input CompactInput, detachedPath, workDir string) (metrics.PartStats, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return metrics.PartStats{}, err
	}
	downloadRoot := filepath.Join(workDir, "data")
	extractRoot := filepath.Join(workDir, "extracted")
	slog.Info("downloading compact input artifact", "stage", "compact_download_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "bucket", input.Bucket, "key", input.FinishedKey)
	downloadStartedAt := time.Now()
	if err := c.S3Copy.DownloadPrefix(ctx, input.Bucket, input.FinishedKey, downloadRoot); err != nil {
		return metrics.PartStats{}, fmt.Errorf("download compact input artifact s3://%s/%s: %w", input.Bucket, input.FinishedKey, err)
	}
	downloadStats, err := fileutil.StatDir(downloadRoot)
	if err != nil {
		return metrics.PartStats{}, fmt.Errorf("stat compact input artifact s3://%s/%s: %w", input.Bucket, input.FinishedKey, err)
	}
	slog.Info("downloaded compact input artifact", "stage", "compact_download_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "files", downloadStats.Files, "bytes", downloadStats.Bytes, "elapsed", time.Since(downloadStartedAt), "bytes_per_second", ratePerSecond(downloadStats.Bytes, time.Since(downloadStartedAt)))

	extractStartedAt := time.Now()
	partNames, err := artifact.ExtractFinishedTarballsFromDirContext(ctx, downloadRoot, extractRoot)
	if err != nil {
		return metrics.PartStats{}, fmt.Errorf("extract compact input artifact s3://%s/%s: %w", input.Bucket, input.FinishedKey, err)
	}
	extractElapsed := time.Since(extractStartedAt)
	slog.Info("extracted compact input artifact", "stage", "compact_extract_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "parts", len(partNames), "bytes", downloadStats.Bytes, "elapsed", extractElapsed, "bytes_per_second", ratePerSecond(downloadStats.Bytes, extractElapsed))
	if len(partNames) == 0 {
		return metrics.PartStats{}, fmt.Errorf("compact input artifact s3://%s/%s contains no part tarballs", input.Bucket, input.FinishedKey)
	}
	attachStartedAt := time.Now()
	for _, partName := range partNames {
		partStartedAt := time.Now()
		src := filepath.Join(extractRoot, partName)
		dst := filepath.Join(detachedPath, partName)
		partStats, err := fileutil.StatDir(src)
		if err != nil {
			return metrics.PartStats{}, fmt.Errorf("stat extracted compact input part %s: %w", partName, err)
		}
		if _, err := os.Stat(dst); err == nil {
			return metrics.PartStats{}, fmt.Errorf("detached compact part destination already exists: %s", dst)
		} else if !os.IsNotExist(err) {
			return metrics.PartStats{}, err
		}
		moveStartedAt := time.Now()
		if err := fileutil.MoveDir(src, dst); err != nil {
			return metrics.PartStats{}, fmt.Errorf("move compact input part %s into detached directory: %w", partName, err)
		}
		moveElapsed := time.Since(moveStartedAt)
		clickHouseStartedAt := time.Now()
		if err := c.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable)+" ATTACH PART "+chhttp.StringLiteral(partName)); err != nil {
			return metrics.PartStats{}, fmt.Errorf("attach compact input part %s: %w", partName, err)
		}
		clickHouseElapsed := time.Since(clickHouseStartedAt)
		slog.Info("attached compact input part", "stage", "compact_attach_input_part", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "part", partName, "files", partStats.Files, "bytes", partStats.Bytes, "move_elapsed", moveElapsed, "clickhouse_attach_elapsed", clickHouseElapsed, "elapsed", time.Since(partStartedAt))
	}
	slog.Info("attached compact input artifact", "stage", "compact_attach_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "parts", len(partNames), "elapsed", time.Since(attachStartedAt))
	return metrics.PartStats{Count: uint64(len(partNames)), Rows: input.Rows, Bytes: input.Bytes}, nil
}

func addPartStats(left, right metrics.PartStats) metrics.PartStats {
	return metrics.PartStats{
		Count: left.Count + right.Count,
		Rows:  left.Rows + right.Rows,
		Bytes: left.Bytes + right.Bytes,
	}
}

func CompactFinishedKeyFromInput(inputKey, outputPartID string) (string, error) {
	inputKey = strings.Trim(inputKey, "/")
	outputPartID = strings.TrimSpace(outputPartID)
	if inputKey == "" || outputPartID == "" {
		return "", fmt.Errorf("input finished key and output part id are required")
	}
	return path.Join(path.Dir(inputKey), outputPartID), nil
}

func compactMergeTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return DefaultCompactMergeTimeout
	}
	return timeout
}

func compactMergeMaxTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return DefaultCompactMergeMaxTimeout
	}
	return timeout
}

func compactMergeTimeoutUntil(deadline, now time.Time) (time.Duration, bool) {
	if deadline.IsZero() {
		return 0, false
	}
	if !now.Before(deadline) {
		return 0, true
	}
	return deadline.Sub(now), true
}

func compactMergeTimeoutsForDeadline(timeout, maxTimeout, remaining time.Duration) (time.Duration, time.Duration) {
	if remaining < 0 {
		remaining = 0
	}
	maxTimeout = remaining
	if timeout > maxTimeout {
		timeout = maxTimeout
	}
	return timeout, maxTimeout
}

func compactMergeSettleMinWait(wait time.Duration) time.Duration {
	if wait == 0 {
		return DefaultCompactMergeSettleMinWait
	}
	return wait
}

func compactOptimizeFinalAfter(wait time.Duration) time.Duration {
	if wait == 0 {
		return DefaultCompactOptimizeFinalAfter
	}
	return wait
}
