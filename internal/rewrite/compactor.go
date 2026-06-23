package rewrite

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
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

type Compactor struct {
	S3Copy              s3copy.Copier
	ClickHouse          chhttp.Client
	WorkDir             string
	MergeTimeout        time.Duration
	MergeMaxTimeout     time.Duration
	MergeSettleMinWait  time.Duration
	MergeSettleMinParts uint64
	MergePollInterval   time.Duration
	MergeTreeSettings   MergeTreeSettings
	RestartClickHouse   func(context.Context) error
	LoadMoreInputs      func(context.Context, CompactLoadState) ([]CompactInput, error)
	LoadMoreInterval    time.Duration
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

type CompactLoadState struct {
	Stats      metrics.PartStats
	Partitions []PartPartitionStats
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

	slog.Info("preparing compact destination table", "stage", "compact_prepare_table", "job_id", item.JobID, "part_id", item.OutputPartID, "destination_table", chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable))
	if err := c.prepareDestinationTable(ctx, item); err != nil {
		return CompactResult{}, err
	}
	if err := p.configureDestinationCompressionCodec(ctx, m); err != nil {
		return CompactResult{}, err
	}
	dataPath, err := p.tableDataPath(ctx, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return CompactResult{}, err
	}
	detached := filepath.Join(dataPath, "detached")
	if err := os.MkdirAll(detached, 0o755); err != nil {
		return CompactResult{}, err
	}

	for idx, input := range item.Inputs {
		workDir := filepath.Join(root, "inputs", fmt.Sprintf("%06d", idx))
		if err := c.attachFinishedArtifact(ctx, item, input, detached, workDir); err != nil {
			return CompactResult{}, err
		}
	}

	actualInputPartitions, err := p.activePartPartitionStats(ctx, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return CompactResult{}, fmt.Errorf("measure compact input active part partitions: %w", err)
	}
	actualInputStats := summarizePartPartitions(actualInputPartitions)
	inputStats := compactInputStats(attachedInputs, actualInputStats)
	slog.Info("attached compact input artifacts", "stage", "compact_attach_inputs", "job_id", item.JobID, "part_id", item.OutputPartID, "input_artifacts", len(item.Inputs), "active_parts", actualInputStats.Count, "active_rows", actualInputStats.Rows, "active_bytes_on_disk", actualInputStats.Bytes)
	if inputStats.Count < 2 {
		return CompactResult{OutputPartID: item.OutputPartID, InputStats: inputStats, DestinationStats: inputStats, DestinationPartitions: actualInputPartitions, Inputs: attachedInputs}, nil
	}

	if err := c.configureCompactMergeSettings(ctx, item, actualInputStats.Bytes); err != nil {
		return CompactResult{}, err
	}
	if err := p.restartClickHouse(ctx, m); err != nil {
		return CompactResult{}, err
	}
	lastLoadMoreAt := time.Time{}
	p.mergeWaitHook = func(ctx context.Context, target mergeWaitTarget, snapshot mergePartSnapshot, activeMerges uint64) (bool, error) {
		if c.LoadMoreInputs == nil {
			return false, nil
		}
		now := time.Now()
		interval := c.LoadMoreInterval
		if interval < 0 {
			return false, fmt.Errorf("compact load-more interval must be non-negative, got %s", interval)
		}
		if interval > 0 && !lastLoadMoreAt.IsZero() && now.Sub(lastLoadMoreAt) < interval {
			return false, nil
		}
		lastLoadMoreAt = now
		partitions, err := p.activePartPartitionStats(ctx, item.DestinationDatabase, item.DestinationTable)
		if err != nil {
			return false, fmt.Errorf("measure compact active part partitions before loading more inputs: %w", err)
		}
		inputs, err := c.LoadMoreInputs(ctx, CompactLoadState{
			Stats: metrics.PartStats{
				Count: snapshot.ActiveParts,
				Bytes: snapshot.TotalBytes,
			},
			Partitions: partitions,
		})
		if err != nil {
			return false, err
		}
		if len(inputs) == 0 {
			return false, nil
		}
		for _, input := range inputs {
			workDir := filepath.Join(root, "inputs", fmt.Sprintf("%06d", len(attachedInputs)))
			if err := c.attachFinishedArtifact(ctx, item, input, detached, workDir); err != nil {
				return false, err
			}
			attachedInputs = append(attachedInputs, input)
		}
		partitions, err = p.activePartPartitionStats(ctx, item.DestinationDatabase, item.DestinationTable)
		if err != nil {
			return false, fmt.Errorf("measure compact active part partitions after loading more inputs: %w", err)
		}
		stats := summarizePartPartitions(partitions)
		if err := c.configureCompactMergeSettings(ctx, item, stats.Bytes); err != nil {
			return false, err
		}
		slog.Info("loaded more compact input artifacts", "stage", "compact_load_more_inputs", "job_id", item.JobID, "output_part_id", item.OutputPartID, "loaded_inputs", len(inputs), "total_inputs", len(attachedInputs), "active_parts", stats.Count, "active_bytes_on_disk", stats.Bytes)
		return true, nil
	}
	target := mergeWaitTarget{
		JobID:    item.JobID,
		PartID:   item.OutputPartID,
		Database: item.DestinationDatabase,
		Table:    item.DestinationTable,
	}
	if _, err := p.waitForDestinationMerges(ctx, m, nil, target, "compact"); err != nil {
		return CompactResult{}, err
	}

	destPartitions, err := p.activePartPartitionStats(ctx, item.DestinationDatabase, item.DestinationTable)
	if err != nil {
		return CompactResult{}, fmt.Errorf("measure compact output active part partitions: %w", err)
	}
	destStats := summarizePartPartitions(destPartitions)
	inputStats = compactInputStats(attachedInputs, inputStats)
	slog.Info("measured compact output parts", "stage", "compact_measure_output", "job_id", item.JobID, "part_id", item.OutputPartID, "input_parts", inputStats.Count, "output_parts", destStats.Count, "active_rows", destStats.Rows, "active_bytes_on_disk", destStats.Bytes)
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
	maxAtMaxSpace := activeBytes
	if maxAtMaxSpace == 0 {
		maxAtMaxSpace = defaultMergeMaxBytesAtMaxSpaceInPool
	}
	maxAtMaxSpace = clampUint64(maxAtMaxSpace, minMergeMaxBytesAtMaxSpaceInPool, maxMergeMaxBytesAtMaxSpaceInPool)
	maxAtMinSpace := ceilDivUint64(maxAtMaxSpace, mergeMaxBytesAtMinSpacePoolDivisor)
	maxAtMinSpace = clampUint64(maxAtMinSpace, minMergeMaxBytesAtMinSpaceInPool, maxMergeMaxBytesAtMinSpaceInPool)
	maxAtMinSpace = minUint64(maxAtMinSpace, maxAtMaxSpace)
	if maxAtMinSpace == 0 {
		maxAtMinSpace = 1
	}
	query := "ALTER TABLE " + table +
		" MODIFY SETTING merge_max_block_size = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSize, 10) +
		", merge_max_block_size_bytes = " + strconv.FormatUint(mergeTreeSettings.MergeMaxBlockSizeBytes, 10) +
		", merge_selecting_sleep_ms = " + strconv.FormatUint(mergeTreeSettings.MergeSelectingSleepMS, 10) +
		", max_bytes_to_merge_at_max_space_in_pool = " + strconv.FormatUint(maxAtMaxSpace, 10) +
		", max_bytes_to_merge_at_min_space_in_pool = " + strconv.FormatUint(maxAtMinSpace, 10)
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
		"destination_active_bytes_on_disk", activeBytes,
		"max_bytes_to_merge_at_max_space_in_pool", maxAtMaxSpace,
		"max_bytes_to_merge_at_min_space_in_pool", maxAtMinSpace,
	)
	return nil
}

func (c Compactor) attachFinishedArtifact(ctx context.Context, item CompactWorkItem, input CompactInput, detachedPath, workDir string) error {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}
	downloadRoot := filepath.Join(workDir, "data")
	extractRoot := filepath.Join(workDir, "extracted")
	slog.Info("downloading compact input artifact", "stage", "compact_download_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "bucket", input.Bucket, "key", input.FinishedKey)
	downloadStartedAt := time.Now()
	if err := c.S3Copy.DownloadPrefix(ctx, input.Bucket, input.FinishedKey, downloadRoot); err != nil {
		return fmt.Errorf("download compact input artifact s3://%s/%s: %w", input.Bucket, input.FinishedKey, err)
	}
	downloadStats, err := fileutil.StatDir(downloadRoot)
	if err != nil {
		return fmt.Errorf("stat compact input artifact s3://%s/%s: %w", input.Bucket, input.FinishedKey, err)
	}
	slog.Info("downloaded compact input artifact", "stage", "compact_download_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "files", downloadStats.Files, "bytes", downloadStats.Bytes, "elapsed", time.Since(downloadStartedAt), "bytes_per_second", ratePerSecond(downloadStats.Bytes, time.Since(downloadStartedAt)))

	partNames, err := extractFinishedTarballs(downloadRoot, extractRoot)
	if err != nil {
		return fmt.Errorf("extract compact input artifact s3://%s/%s: %w", input.Bucket, input.FinishedKey, err)
	}
	if len(partNames) == 0 {
		return fmt.Errorf("compact input artifact s3://%s/%s contains no part tarballs", input.Bucket, input.FinishedKey)
	}
	for _, partName := range partNames {
		src := filepath.Join(extractRoot, partName)
		dst := filepath.Join(detachedPath, partName)
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("detached compact part destination already exists: %s", dst)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := fileutil.MoveDir(src, dst); err != nil {
			return fmt.Errorf("move compact input part %s into detached directory: %w", partName, err)
		}
		if err := c.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(item.DestinationDatabase, item.DestinationTable)+" ATTACH PART "+chhttp.StringLiteral(partName)); err != nil {
			return fmt.Errorf("attach compact input part %s: %w", partName, err)
		}
	}
	slog.Info("attached compact input artifact", "stage", "compact_attach_input", "job_id", item.JobID, "output_part_id", item.OutputPartID, "input_part_id", input.PartID, "parts", len(partNames))
	return nil
}

func compactInputStats(inputs []CompactInput, fallback metrics.PartStats) metrics.PartStats {
	var stats metrics.PartStats
	for _, input := range inputs {
		stats.Count += input.Parts
		stats.Rows += input.Rows
		stats.Bytes += input.Bytes
	}
	if stats.Count == 0 {
		return fallback
	}
	return stats
}

func extractFinishedTarballs(root, extractRoot string) ([]string, error) {
	tarballs, err := finishedTarballs(root)
	if err != nil {
		return nil, err
	}
	partSeen := map[string]struct{}{}
	var partNames []string
	for _, tarball := range tarballs {
		extracted, err := artifact.ExtractFinishedTar(filepath.Join(root, tarball), extractRoot)
		if err != nil {
			return nil, fmt.Errorf("extract %s: %w", tarball, err)
		}
		for _, partName := range extracted {
			if _, ok := partSeen[partName]; ok {
				return nil, fmt.Errorf("duplicate finished part %q in downloaded tarballs", partName)
			}
			partSeen[partName] = struct{}{}
			partNames = append(partNames, partName)
		}
	}
	sort.Strings(partNames)
	return partNames, nil
}

func finishedTarballs(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			return nil, fmt.Errorf("unexpected directory at finished artifact root: %s", filepath.Join(root, entry.Name()))
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("unexpected non-regular file at finished artifact root: %s", filepath.Join(root, entry.Name()))
		}
		if !strings.HasSuffix(entry.Name(), manifest.FinishedTarSuffix) {
			return nil, fmt.Errorf("unexpected non-tar file at finished artifact root: %s", filepath.Join(root, entry.Name()))
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
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

func compactMergeSettleMinWait(wait time.Duration) time.Duration {
	if wait == 0 {
		return DefaultCompactMergeSettleMinWait
	}
	return wait
}
