package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/partforge/partforge/internal/artifact"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/chproc"
	"github.com/partforge/partforge/internal/ddl"
	"github.com/partforge/partforge/internal/fileutil"
	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/manifest"
	"github.com/partforge/partforge/internal/metrics"
	"github.com/partforge/partforge/internal/parts"
	"github.com/partforge/partforge/internal/resources"
	"github.com/partforge/partforge/internal/rewrite"
	"github.com/partforge/partforge/internal/s3copy"
	"github.com/partforge/partforge/internal/state"
)

const defaultClickHouseURL = "http://127.0.0.1:8123"
const defaultStateTable = "partforge"
const defaultConfigPath = "/etc/partforge/config.json"
const defaultClickHouseClientConfigPath = "/etc/clickhouse-client/config.xml"
const defaultWorkerShutdownGracePeriod = 90 * time.Second
const defaultCompactWindow = 2 * time.Hour
const defaultCompactMaxArtifacts = 8
const defaultCompactMaxBytes uint64 = 1024 * 1024 * 1024 * 1024
const compactSourceMergeWaitCap = 5 * time.Minute
const compactMinInputParts uint64 = 2
const defaultRetryStaleAfter = 5 * time.Minute
const workerStateUpdateTimeout = 30 * time.Second
const inProgressStageUnknown = "unknown"
const settleWaitStage = "wait_merges"

var version = "dev"

type workerRunDirs struct {
	Root       string
	ClickHouse string
	Scratch    string
}

func main() {
	configureLogger()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func configureLogger() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:       slog.LevelInfo,
		ReplaceAttr: humanizeLogAttr,
	})))
}

func run() error {
	if len(os.Args) < 2 {
		usage()
		return errors.New("missing command")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "upload-freeze":
		return runUploadFreeze(ctx, os.Args[2:])
	case "worker":
		return runWorker(ctx, os.Args[2:])
	case "import-finished":
		return runImportFinished(ctx, os.Args[2:])
	case "list-jobs":
		return runListJobs(ctx, os.Args[2:])
	case "job-status":
		return runJobStatus(ctx, os.Args[2:])
	case "retry-failed":
		return runRetryFailed(ctx, os.Args[2:])
	case "set-part-state":
		return runSetPartState(ctx, os.Args[2:])
	case "reset-compact-timer":
		return runResetCompactTimer(ctx, os.Args[2:])
	case "reset-job":
		return runResetJob(ctx, os.Args[2:])
	case "reset-compaction":
		return runResetCompaction(ctx, os.Args[2:])
	case "delete-parts":
		return runDeleteParts(ctx, os.Args[2:])
	case "delete-job":
		return runDeleteJob(ctx, os.Args[2:])
	case "version":
		fmt.Println(version)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  partforge upload-freeze   [flags]
  partforge worker          [flags]
  partforge import-finished [flags]
  partforge list-jobs       [flags]
  partforge job-status      [flags]
  partforge retry-failed    [flags]
  partforge set-part-state  [flags]
  partforge reset-compact-timer [flags]
  partforge reset-job       [flags]
  partforge reset-compaction [flags]
  partforge delete-parts    [flags]
  partforge delete-job      [flags]

Commands:
  upload-freeze     Upload frozen source part directories to S3 and register DynamoDB work.
  worker            Claim DynamoDB work, rewrite source parts with local ClickHouse, and upload finished artifacts.
  import-finished   Attach finished artifacts into the final ClickHouse table with safe part renames.
  list-jobs         List job IDs found in the DynamoDB state table.
  job-status        Show part state counts, progress, and failed part errors for one job.
  retry-failed      Move failed parts back to their retryable state.
  set-part-state    Force selected part rows into a stable state for admin recovery.
  reset-compact-timer Restart selected compact rows' compact-window timer.
  reset-job         Delete generated compact rows and move original job parts back to READY.
  reset-compaction  Delete generated compact rows and move original rewritten parts back to COMPACT_READY.
  delete-parts      Force delete selected DynamoDB part rows from one job.
  delete-job        Delete one job's DynamoDB state rows, optionally including S3 artifacts.
  version           Print the build version.
`)
}

func runUploadFreeze(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("upload-freeze", flag.ExitOnError)
	var (
		configPath            = fs.String("config", defaultConfigPath, "JSON config file path")
		database              = fs.String("database", "", "source database")
		table                 = fs.String("table", "", "source table")
		freezeName            = fs.String("freeze", "", "ALTER TABLE ... FREEZE WITH NAME value")
		destinationSchemaFile = fs.String("destination-schema-file", "", "file containing full CREATE TABLE for destination")
		insertSelectFile      = fs.String("insert-select-file", "", "file containing INSERT INTO destination SELECT ... FROM source")
		clickHouseURL         = fs.String("clickhouse-url", defaultClickHouseURL, "source ClickHouse HTTP URL for SHOW CREATE TABLE")
		clickHouseUser        = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword    = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		jobID                 = fs.String("job-id", "", "optional deterministic job id override")
		bucket                = fs.String("bucket", "", "S3 bucket")
		prefix                = fs.String("prefix", "partforge", "S3 key prefix")
		stateTable            = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region                = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint            = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary           = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		s5cmdNumWorkers       = fs.Int("s5cmd-numworkers", 0, "s5cmd --numworkers per upload process; <=0 auto-scales from upload-concurrency")
		uploadConcurrency     = fs.Int("upload-concurrency", 0, "number of source parts to upload concurrently; <=0 uses detected CPU count")
		dynamoEndpoint        = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "upload-freeze"); err != nil {
		return err
	}
	if err := applyClickHouseClientConfigDefaults(clickHouseUser, clickHousePassword); err != nil {
		return err
	}

	if *database == "" || *table == "" || *freezeName == "" || *destinationSchemaFile == "" || *insertSelectFile == "" || *bucket == "" {
		return errors.New("database, table, freeze, destination-schema-file, insert-select-file, and bucket are required")
	}
	resolvedUploadConcurrency, err := resolveUploadConcurrency(*uploadConcurrency)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	sourceTable := chhttp.TableSQL(*database, *table)
	slog.Info(
		"upload-freeze started",
		"stage", "start",
		"source_table", sourceTable,
		"freeze", *freezeName,
		"bucket", *bucket,
		"prefix", *prefix,
	)

	slog.Info("reading SQL files", "stage", "read_sql_files", "destination_schema_file", *destinationSchemaFile, "insert_select_file", *insertSelectFile)
	destinationSchema, err := readRequiredFile(*destinationSchemaFile)
	if err != nil {
		return err
	}
	insertSelect, err := readRequiredFile(*insertSelectFile)
	if err != nil {
		return err
	}
	slog.Info("read SQL files", "stage", "read_sql_files", "destination_schema_bytes", len(destinationSchema), "insert_select_bytes", len(insertSelect))

	ch := chhttp.Client{
		URL:      *clickHouseURL,
		User:     *clickHouseUser,
		Password: *clickHousePassword,
	}
	slog.Info("loading source table schema", "stage", "load_source_schema", "source_table", sourceTable, "clickhouse_url", *clickHouseURL)
	sourceSchema, err := ch.QueryString(ctx, "SHOW CREATE TABLE "+sourceTable+" FORMAT TSVRaw")
	if err != nil {
		return fmt.Errorf("show create source table: %w", err)
	}
	sourceSchema = strings.TrimSpace(sourceSchema)
	slog.Info("loaded source table schema", "stage", "load_source_schema", "source_table", sourceTable, "source_schema_bytes", len(sourceSchema))

	slog.Info("validating source schema and destination table", "stage", "validate_schemas")
	if _, err := ddl.NormalizeCreateTable(sourceSchema); err != nil {
		return fmt.Errorf("source schema is not supported by worker: %w", err)
	}
	destinationTableRef, err := destinationTableRefFromSchema(destinationSchema)
	if err != nil {
		return err
	}
	slog.Info("validated source schema and destination table", "stage", "validate_schemas", "destination_schema_table", chhttp.TableSQL(destinationTableRef.Database, destinationTableRef.Table))

	slog.Info("discovering local ClickHouse disks", "stage", "discover_disks")
	disks, err := freeze.LocalDisks(ctx, ch)
	if err != nil {
		return err
	}
	slog.Info("discovered local ClickHouse disks", "stage", "discover_disks", "disks", len(disks), "disk_paths", formatDiskPaths(disks))

	slog.Info("scanning frozen parts", "stage", "scan_freeze", "freeze", *freezeName)
	scannedParts, err := freeze.ScanDisks(disks, *freezeName)
	if err != nil {
		return err
	}
	slog.Info("found frozen parts", "stage", "scan_freeze", "parts", len(scannedParts), "parts_by_disk", formatPartCountsByDisk(disks, scannedParts))

	resolvedJobID := *jobID
	if resolvedJobID == "" {
		resolvedJobID = manifest.DeriveJobID(*database, *table, *freezeName, sourceSchema, destinationSchema, insertSelect)
	}
	slog.Info("resolved job id", "stage", "resolve_job", "job_id", resolvedJobID)

	slog.Info("initializing DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	slog.Info("initialized DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	var uploadedBytes uint64
	uploadedParts := 0
	effectiveConcurrency := min(resolvedUploadConcurrency, len(scannedParts))
	resolvedS5cmdNumWorkers := resolveS5cmdNumWorkers(*s5cmdNumWorkers, effectiveConcurrency)
	copier := s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint, NumWorkers: resolvedS5cmdNumWorkers}
	slog.Info(
		"uploading frozen source parts",
		"stage", "upload_parts",
		"job_id", resolvedJobID,
		"parts_total", len(scannedParts),
		"upload_concurrency", *uploadConcurrency,
		"resolved_upload_concurrency", resolvedUploadConcurrency,
		"effective_upload_concurrency", effectiveConcurrency,
		"s5cmd_numworkers", resolvedS5cmdNumWorkers,
	)
	tasks := make([]uploadPartTask, 0, len(scannedParts))
	for idx, sourcePart := range scannedParts {
		tasks = append(tasks, uploadPartTask{Index: idx + 1, SourcePart: sourcePart})
	}
	uploadParams := uploadFreezePartParams{
		JobID:             resolvedJobID,
		FreezeName:        *freezeName,
		Source:            manifest.TableRef{Database: *database, Table: *table},
		Dest:              destinationTableRef,
		SourceSchema:      sourceSchema,
		DestinationSchema: destinationSchema,
		InsertSelect:      insertSelect,
		Bucket:            *bucket,
		Prefix:            *prefix,
		PartsTotal:        len(scannedParts),
		StateStore:        stateStore,
		Copier:            copier,
	}
	err = uploadPartsInParallel(ctx, tasks, effectiveConcurrency, func(ctx context.Context, workerID int, task uploadPartTask) (uploadPartResult, error) {
		return uploadFreezePart(ctx, workerID, task, uploadParams)
	}, func(result uploadPartResult) {
		uploadedParts++
		uploadedBytes += result.Bytes
		elapsed := time.Since(startedAt)
		slog.Info(
			"source part upload progress",
			"stage", "upload_parts",
			"job_id", resolvedJobID,
			"completed_parts", uploadedParts,
			"parts_total", len(scannedParts),
			"part_index", result.Index,
			"part_id", result.PartID,
			"disk", result.SourcePart.Disk,
			"part", result.SourcePart.RelativePath,
			"uploaded_bytes", uploadedBytes,
			"overall_parts_per_second", countRatePerSecond(uploadedParts, elapsed),
			"overall_bytes_per_second", ratePerSecond(uploadedBytes, elapsed),
		)
	})
	if err != nil {
		return err
	}

	elapsed := time.Since(startedAt)
	slog.Info(
		"upload-freeze complete",
		"stage", "complete",
		"job_id", resolvedJobID,
		"parts", len(scannedParts),
		"uploaded_bytes", uploadedBytes,
		"elapsed", elapsed,
		"parts_per_second", countRatePerSecond(len(scannedParts), elapsed),
		"bytes_per_second", ratePerSecond(uploadedBytes, elapsed),
	)
	return nil
}

type uploadPartTask struct {
	Index      int
	SourcePart freeze.Part
}

type uploadPartResult struct {
	Index         int
	SourcePart    freeze.Part
	PartID        string
	SourceKey     string
	FinishedKey   string
	Files         uint64
	Bytes         uint64
	UploadElapsed time.Duration
}

type uploadFreezePartParams struct {
	JobID             string
	FreezeName        string
	Source            manifest.TableRef
	Dest              manifest.TableRef
	SourceSchema      string
	DestinationSchema string
	InsertSelect      string
	Bucket            string
	Prefix            string
	PartsTotal        int
	StateStore        *state.Store
	Copier            s3copy.Copier
}

type uploadPartFunc func(context.Context, int, uploadPartTask) (uploadPartResult, error)

func uploadPartsInParallel(ctx context.Context, tasks []uploadPartTask, concurrency int, upload uploadPartFunc, onResult func(uploadPartResult)) error {
	if concurrency < 1 {
		return errors.New("upload concurrency must be at least 1")
	}
	if upload == nil {
		return errors.New("upload function is required")
	}
	if len(tasks) == 0 {
		return nil
	}
	if concurrency > len(tasks) {
		concurrency = len(tasks)
	}

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	taskCh := make(chan uploadPartTask)
	resultCh := make(chan uploadPartResult)
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	for workerID := 1; workerID <= concurrency; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskCh {
				if uploadCtx.Err() != nil {
					return
				}
				result, err := upload(uploadCtx, workerID, task)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				select {
				case resultCh <- result:
				case <-uploadCtx.Done():
					return
				}
			}
		}(workerID)
	}

	go func() {
		defer close(taskCh)
		for _, task := range tasks {
			select {
			case taskCh <- task:
			case <-uploadCtx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	completed := 0
	for completed < len(tasks) {
		select {
		case result, ok := <-resultCh:
			if !ok {
				select {
				case err := <-errCh:
					return err
				default:
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				return fmt.Errorf("part upload workers stopped after %d of %d parts", completed, len(tasks))
			}
			completed++
			if onResult != nil {
				onResult(result)
			}
		case err := <-errCh:
			cancel()
			for range resultCh {
			}
			return err
		case <-ctx.Done():
			cancel()
			for range resultCh {
			}
			return ctx.Err()
		}
	}

	cancel()
	for range resultCh {
	}
	return nil
}

func uploadFreezePart(ctx context.Context, workerID int, task uploadPartTask, params uploadFreezePartParams) (uploadPartResult, error) {
	sourcePart := task.SourcePart
	partID := manifest.DerivePartID(sourcePart.Disk, sourcePart.RelativePath, sourcePart.Name, params.SourceSchema, params.DestinationSchema, params.InsertSelect)
	sourceKey := manifest.SourcePartPrefix(params.Prefix, params.JobID, partID)
	finishedKey := manifest.FinishedPartPrefix(params.Prefix, params.JobID, partID)
	createdAt := time.Now().UTC()

	m := manifest.Manifest{
		Version:   manifest.Version,
		JobID:     params.JobID,
		PartID:    partID,
		Freeze:    params.FreezeName,
		Source:    params.Source,
		Dest:      params.Dest,
		Part:      manifest.SourcePart{Disk: sourcePart.Disk, Name: sourcePart.Name, RelativePath: sourcePart.RelativePath},
		SQL:       manifest.SQLBundle{SourceSchema: params.SourceSchema, DestinationSchema: params.DestinationSchema, InsertSelect: params.InsertSelect},
		S3:        manifest.S3Refs{Bucket: params.Bucket, SourceKey: sourceKey, FinishedKey: finishedKey},
		CreatedAt: createdAt,
	}

	if err := artifact.WriteManifest(sourcePart.Path, m); err != nil {
		return uploadPartResult{}, fmt.Errorf("write source manifest for %s:%s: %w", sourcePart.Disk, sourcePart.RelativePath, err)
	}
	partStats, err := fileutil.StatDir(sourcePart.Path)
	if err != nil {
		return uploadPartResult{}, fmt.Errorf("stat source part %s:%s: %w", sourcePart.Disk, sourcePart.RelativePath, err)
	}

	slog.Info(
		"uploading source part",
		"stage", "upload_parts",
		"job_id", params.JobID,
		"worker_id", workerID,
		"part_index", task.Index,
		"parts_total", params.PartsTotal,
		"part_id", partID,
		"disk", sourcePart.Disk,
		"part", sourcePart.RelativePath,
		"files", partStats.Files,
		"bytes", partStats.Bytes,
		"source_key", sourceKey,
	)
	uploadStartedAt := time.Now()
	if err := params.Copier.UploadDir(ctx, sourcePart.Path, params.Bucket, sourceKey); err != nil {
		return uploadPartResult{}, fmt.Errorf("upload source part %s:%s to s3://%s/%s: %w", sourcePart.Disk, sourcePart.RelativePath, params.Bucket, sourceKey, err)
	}
	uploadElapsed := time.Since(uploadStartedAt)
	slog.Info(
		"uploaded source part",
		"stage", "upload_parts",
		"job_id", params.JobID,
		"worker_id", workerID,
		"part_index", task.Index,
		"parts_total", params.PartsTotal,
		"part_id", partID,
		"disk", sourcePart.Disk,
		"part", sourcePart.RelativePath,
		"bytes", partStats.Bytes,
		"upload_elapsed", uploadElapsed,
		"part_bytes_per_second", ratePerSecond(partStats.Bytes, uploadElapsed),
	)

	partState := state.NewPart(params.JobID, partID, params.Bucket, sourceKey, finishedKey, createdAt)
	slog.Info("registering source part", "stage", "register_parts", "job_id", params.JobID, "worker_id", workerID, "part_id", partID, "source_key", sourceKey, "finished_key", finishedKey)
	if err := params.StateStore.CreatePart(ctx, partState); err != nil {
		return uploadPartResult{}, fmt.Errorf("create state for %s: %w", sourceKey, err)
	}
	slog.Info(
		"registered source part",
		"stage", "register_parts",
		"job_id", params.JobID,
		"worker_id", workerID,
		"part_index", task.Index,
		"parts_total", params.PartsTotal,
		"part_id", partID,
		"disk", sourcePart.Disk,
		"part", sourcePart.RelativePath,
		"source_key", sourceKey,
		"finished_key", finishedKey,
	)

	return uploadPartResult{
		Index:         task.Index,
		SourcePart:    sourcePart,
		PartID:        partID,
		SourceKey:     sourceKey,
		FinishedKey:   finishedKey,
		Files:         partStats.Files,
		Bytes:         partStats.Bytes,
		UploadElapsed: uploadElapsed,
	}, nil
}

func formatDiskPaths(disks []freeze.Disk) string {
	parts := make([]string, 0, len(disks))
	for _, disk := range disks {
		parts = append(parts, disk.Name+"="+disk.Path)
	}
	return strings.Join(parts, ",")
}

func formatPartCountsByDisk(disks []freeze.Disk, parts []freeze.Part) string {
	counts := make(map[string]int, len(disks))
	for _, disk := range disks {
		counts[disk.Name] = 0
	}
	for _, part := range parts {
		counts[part.Disk]++
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, fmt.Sprintf("%s=%d", name, counts[name]))
	}
	return strings.Join(out, ",")
}

func ratePerSecond(bytes uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(bytes) / elapsed.Seconds()
}

func countRatePerSecond(count int, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func resolveUploadConcurrency(configured int) (int, error) {
	if configured > 0 {
		return configured, nil
	}
	limits, err := resources.DetectLimits()
	if err != nil {
		return 0, fmt.Errorf("detect upload concurrency: %w", err)
	}
	return uploadConcurrencyFromCPUs(limits.CPUs)
}

func uploadConcurrencyFromCPUs(cpus int) (int, error) {
	if cpus < 1 {
		return 0, fmt.Errorf("detected CPU count must be at least 1, got %d", cpus)
	}
	return cpus, nil
}

func resolveS5cmdNumWorkers(configured, uploadConcurrency int) int {
	if configured > 0 {
		return configured
	}
	if uploadConcurrency < 1 {
		uploadConcurrency = 1
	}
	workers := 256 / uploadConcurrency
	if workers < 1 {
		return 1
	}
	return workers
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	var (
		configPath              = fs.String("config", defaultConfigPath, "JSON config file path")
		region                  = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint              = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary             = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		stateTable              = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		dynamoEndpoint          = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		clickHouseURL           = fs.String("clickhouse-url", defaultClickHouseURL, "local ClickHouse HTTP URL")
		clickHouseUser          = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword      = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		clickHouseBinary        = fs.String("clickhouse-binary", "clickhouse", "clickhouse binary path")
		clickHouseConfigFile    = fs.String("clickhouse-config-file", "/etc/clickhouse-server/config.xml", "clickhouse-server config file")
		once                    = fs.Bool("once", false, "process one part and exit")
		pollInterval            = fs.Duration("poll-interval", 10*time.Second, "how long to wait before checking for ready work again")
		workerID                = fs.String("worker-id", "", "worker identity recorded on claimed parts")
		workDir                 = fs.String("work-dir", "/tmp/partforge", "worker scratch directory")
		defaultCompressionCodec = fs.String("default-compression-codec", resources.DefaultCompressionCodec, "destination table default_compression_codec applied before insert-select starts")
		mergeIdleTimeout        = fs.Duration("merge-idle-timeout", rewrite.DefaultMergeTimeout, "how long destination merges may be idle before freezing current destination parts")
		mergeMaxRuntime         = fs.Duration("merge-max-runtime", rewrite.DefaultMergeMaxTimeout, "hard cap for a destination merge wait even while ClickHouse keeps making progress")
		compact                 = fs.Bool("compact", true, "run opportunistic compaction when no READY source work is available")
		compactWindow           = fs.Duration("compact-window", defaultCompactWindow, "how long COMPACT_READY artifacts remain eligible for compaction before being promoted to FINISHED; 0 finalizes as soon as no useful compaction is available")
		compactMergeIdleTimeout = fs.Duration("compact-merge-idle-timeout", rewrite.DefaultCompactMergeTimeout, "how long compact destination merges may be idle before freezing current compact output")
		compactMergeMaxRuntime  = fs.Duration("compact-merge-max-runtime", rewrite.DefaultCompactMergeMaxTimeout, "hard cap for a compact merge wait even while ClickHouse keeps making progress")
		compactMaxBytes         = fs.Uint64("compact-max-bytes", defaultCompactMaxBytes, "maximum summed input bytes_on_disk for one compaction batch; 0 disables the byte cap")
		metricsAddr             = fs.String("metrics-addr", ":2112", "Prometheus metrics listen address; empty disables metrics")
		metricsPath             = fs.String("metrics-path", "/metrics", "Prometheus metrics HTTP path")
		stateProgressInterval   = fs.Duration("state-progress-interval", 15*time.Second, "how often to write live per-part progress heartbeats to DynamoDB; <=0 disables progress writes")
		shutdownGracePeriod     = fs.Duration("shutdown-grace-period", defaultWorkerShutdownGracePeriod, "how long to let an active part finish after shutdown is requested before canceling it and returning it to READY; <=0 cancels immediately")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "worker"); err != nil {
		return err
	}
	if err := applyClickHouseClientConfigDefaults(clickHouseUser, clickHousePassword); err != nil {
		return err
	}
	if strings.TrimSpace(*defaultCompressionCodec) == "" {
		return fmt.Errorf("default-compression-codec must not be empty")
	}
	if *mergeIdleTimeout < 0 {
		return fmt.Errorf("merge-idle-timeout must be non-negative, got %s", *mergeIdleTimeout)
	}
	if *mergeMaxRuntime < 0 {
		return fmt.Errorf("merge-max-runtime must be non-negative, got %s", *mergeMaxRuntime)
	}
	if *compactWindow < 0 {
		return fmt.Errorf("compact-window must be non-negative, got %s", *compactWindow)
	}
	if *compactMergeIdleTimeout < 0 {
		return fmt.Errorf("compact-merge-idle-timeout must be non-negative, got %s", *compactMergeIdleTimeout)
	}
	if *compactMergeMaxRuntime < 0 {
		return fmt.Errorf("compact-merge-max-runtime must be non-negative, got %s", *compactMergeMaxRuntime)
	}
	slog.Info(
		"worker started",
		"stage", "start",
		"once", *once,
		"state_table", *stateTable,
		"work_dir", *workDir,
		"clickhouse_url", *clickHouseURL,
		"shutdown_grace_period", *shutdownGracePeriod,
		"compact", *compact,
	)
	slog.Info("initializing DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	slog.Info("initialized DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	resolvedWorkerID, err := resolveWorkerID(*workerID)
	if err != nil {
		return err
	}
	slog.Info("resolved worker id", "stage", "resolve_worker", "worker_id", resolvedWorkerID)

	slog.Info("detecting worker resource limits", "stage", "configure_insert_settings")
	workerLimits, err := resources.DetectLimits()
	if err != nil {
		return fmt.Errorf("detect worker resource limits: %w", err)
	}
	insertSettings, err := resources.InsertSelectSettings(workerLimits)
	if err != nil {
		return fmt.Errorf("derive clickhouse insert settings: %w", err)
	}
	mergeTreeSettings, err := resources.MergeTreeSettingsForLimits(workerLimits)
	if err != nil {
		return fmt.Errorf("derive clickhouse merge settings: %w", err)
	}
	mergeBackgroundPoolSize, err := resources.MergeBackgroundPoolSize(workerLimits)
	if err != nil {
		return fmt.Errorf("derive clickhouse merge background pool size: %w", err)
	}
	sourceMergeIdleTimeout, sourceMergeMaxRuntime := sourceMergeWaitTimeouts(*mergeIdleTimeout, *mergeMaxRuntime, *compact)
	sourceMergeSettleMinWait := derivedMergeSettleMinWait(sourceMergeIdleTimeout, rewrite.DefaultMergeSettleMinWait)
	compactStaleAfter := compactLeaseStaleAfter(*compactMergeMaxRuntime, *shutdownGracePeriod)
	compactHeartbeatInterval := compactLeaseHeartbeatInterval(compactStaleAfter)
	slog.Info(
		"configured clickhouse resource settings",
		"cpus", workerLimits.CPUs,
		"memory_bytes", workerLimits.MemoryBytes,
		"memory_bytes_raw", workerLimits.MemoryBytes,
		"max_threads", insertSettings["max_threads"],
		"max_insert_threads", insertSettings["max_insert_threads"],
		"max_memory_usage", insertSettings["max_memory_usage"],
		"max_memory_usage_raw", insertSettings["max_memory_usage"],
		"min_insert_block_size_rows", insertSettings["min_insert_block_size_rows"],
		"min_insert_block_size_bytes", insertSettings["min_insert_block_size_bytes"],
		"min_insert_block_size_bytes_raw", insertSettings["min_insert_block_size_bytes"],
		"default_compression_codec", *defaultCompressionCodec,
		"merge_background_pool_size", mergeBackgroundPoolSize,
		"merge_max_block_size", mergeTreeSettings.MergeMaxBlockSize,
		"merge_max_block_size_bytes", mergeTreeSettings.MergeMaxBlockSizeBytes,
		"merge_selecting_sleep_ms", mergeTreeSettings.MergeSelectingSleepMS,
		"background_merges_mutations_scheduling_policy", mergeTreeSettings.MergeSchedulingPolicy,
		"merge_idle_timeout", sourceMergeIdleTimeout,
		"merge_max_runtime", sourceMergeMaxRuntime,
		"merge_settle_min_wait", sourceMergeSettleMinWait,
		"source_merge_compact_cap", compactSourceMergeWaitCap,
		"compact_window", *compactWindow,
		"compact_retry_cooldown", compactRetryCooldown(*compactWindow),
		"compact_claim_splay_max", compactClaimSplayMax(*compactWindow),
		"compact_merge_idle_timeout", *compactMergeIdleTimeout,
		"compact_merge_max_runtime", *compactMergeMaxRuntime,
		"compact_merge_settle_min_wait", derivedMergeSettleMinWait(*compactMergeIdleTimeout, rewrite.DefaultCompactMergeSettleMinWait),
		"compact_lease_stale_after", compactStaleAfter,
		"compact_heartbeat_interval", compactHeartbeatInterval,
		"compact_load_more_interval", compactLoadMoreInterval(*compactWindow),
		"compact_max_artifacts", defaultCompactMaxArtifacts,
		"compact_max_bytes", *compactMaxBytes,
		"compact_min_input_parts", compactMinInputParts,
	)

	var recorder metrics.Recorder = metrics.Noop{}
	if *metricsAddr != "" {
		slog.Info("starting metrics server", "stage", "start_metrics", "addr", *metricsAddr, "path", *metricsPath)
		prom := metrics.NewPrometheus()
		if _, err := metrics.StartServer(ctx, *metricsAddr, *metricsPath, prom.Handler()); err != nil {
			return fmt.Errorf("start metrics server: %w", err)
		}
		recorder = prom
	}

	for {
		if ctx.Err() != nil {
			slog.Info("worker shutdown requested; stopping before claiming more work", "stage", "shutdown")
			return nil
		}
		slog.Info("claiming next ready part", "stage", "claim_work", "worker_id", resolvedWorkerID)
		part, err := stateStore.ClaimNextReady(ctx, resolvedWorkerID, time.Now().UTC())
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("worker shutdown requested while claiming work", "stage", "shutdown")
				return nil
			}
			return err
		}
		if part == nil {
			if *compact {
				didCompactWork, err := runWorkerCompaction(ctx, workerCompactionConfig{
					StateStore:               stateStore,
					WorkerID:                 resolvedWorkerID,
					WorkDir:                  *workDir,
					ClickHouseURL:            *clickHouseURL,
					ClickHouseUser:           *clickHouseUser,
					ClickHousePassword:       *clickHousePassword,
					ClickHouseBinary:         *clickHouseBinary,
					ClickHouseConfigFile:     *clickHouseConfigFile,
					S5cmdBinary:              *s5cmdBinary,
					S3Endpoint:               *s3Endpoint,
					DefaultCompressionCodec:  *defaultCompressionCodec,
					MergeBackgroundPoolSize:  mergeBackgroundPoolSize,
					MergeSchedulingPolicy:    mergeTreeSettings.MergeSchedulingPolicy,
					MergeMaxBlockSize:        mergeTreeSettings.MergeMaxBlockSize,
					MergeMaxBlockSizeBytes:   mergeTreeSettings.MergeMaxBlockSizeBytes,
					MergeSelectingSleepMS:    mergeTreeSettings.MergeSelectingSleepMS,
					CompactWindow:            *compactWindow,
					CompactMergeIdleTimeout:  *compactMergeIdleTimeout,
					CompactMergeMaxRuntime:   *compactMergeMaxRuntime,
					CompactLeaseStaleAfter:   compactStaleAfter,
					CompactHeartbeatInterval: compactHeartbeatInterval,
					CompactMaxBytes:          *compactMaxBytes,
					ShutdownGracePeriod:      *shutdownGracePeriod,
				})
				if err != nil {
					return err
				}
				if didCompactWork {
					if *once {
						return nil
					}
					continue
				}
			}
			if *once {
				slog.Info("no ready or compactable work available")
				return nil
			}
			slog.Info("no ready part available; sleeping", "stage", "claim_work", "poll_interval", *pollInterval)
			if err := sleepOrDone(ctx, *pollInterval); err != nil {
				if ctx.Err() != nil {
					slog.Info("worker shutdown requested while idle", "stage", "shutdown")
					return nil
				}
				return err
			}
			continue
		}
		slog.Info(
			"claimed ready part",
			"stage", "claim_work",
			"job_id", part.JobID,
			"part_id", part.PartID,
			"attempt", part.Attempts,
			"source_key", part.SourceKey,
		)

		workItem := rewrite.WorkItem{
			Bucket:    part.Bucket,
			SourceKey: part.SourceKey,
			JobID:     part.JobID,
			PartID:    part.PartID,
			Attempt:   part.Attempts,
		}
		processCtx, partShutdown := workerProcessContext(ctx, *shutdownGracePeriod, part.JobID, part.PartID)
		result, err := func() (rewrite.ProcessResult, error) {
			runDirs, err := createWorkerRunDirs(*workDir)
			if err != nil {
				return rewrite.ProcessResult{}, err
			}
			slog.Info(
				"created worker run directory",
				"stage", "prepare_work_dir",
				"work_dir", *workDir,
				"run_dir", runDirs.Root,
				"clickhouse_data_dir", runDirs.ClickHouse,
				"scratch_dir", runDirs.Scratch,
				"job_id", part.JobID,
				"part_id", part.PartID,
			)

			var server *chproc.Server
			defer func() {
				if server != nil {
					slog.Info("stopping local ClickHouse server", "stage", "stop_clickhouse", "job_id", part.JobID, "part_id", part.PartID)
					if err := server.Stop(); err != nil {
						slog.Warn("failed to stop local ClickHouse server", "stage", "stop_clickhouse", "job_id", part.JobID, "part_id", part.PartID, "error", err)
					}
				}
				if err := os.RemoveAll(runDirs.Root); err != nil {
					slog.Warn("failed to remove worker run directory", "run_dir", runDirs.Root, "job_id", part.JobID, "part_id", part.PartID, "error", err)
				}
			}()

			startServer := func(ctx context.Context, tuning chproc.Tuning) (*chproc.Server, error) {
				return chproc.Start(ctx, chproc.Config{
					Binary:     *clickHouseBinary,
					ConfigFile: *clickHouseConfigFile,
					DataDir:    runDirs.ClickHouse,
					URL:        *clickHouseURL,
					User:       *clickHouseUser,
					Password:   *clickHousePassword,
					Timeout:    90 * time.Second,
					Tuning:     tuning,
				})
			}

			slog.Info("starting local ClickHouse server", "stage", "start_clickhouse", "binary", *clickHouseBinary, "config_file", *clickHouseConfigFile, "clickhouse_data_dir", runDirs.ClickHouse, "job_id", part.JobID, "part_id", part.PartID)
			server, err = startServer(processCtx, chproc.Tuning{})
			if err != nil {
				return rewrite.ProcessResult{}, err
			}

			ch := chhttp.Client{URL: *clickHouseURL, User: *clickHouseUser, Password: *clickHousePassword}
			processor := rewrite.Processor{
				S3Copy:              s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint},
				ClickHouse:          ch,
				WorkDir:             runDirs.Scratch,
				MergeTimeout:        sourceMergeIdleTimeout,
				MergeMaxTimeout:     sourceMergeMaxRuntime,
				MergeSettleMinWait:  sourceMergeSettleMinWait,
				MergeSettleMinParts: rewrite.DefaultMergeSettleMinParts,
				Metrics:             recorder,
				InsertSettings:      insertSettings,
				ProgressInterval:    *stateProgressInterval,
				MergeTreeSettings: rewrite.MergeTreeSettings{
					MergeMaxBlockSize:       mergeTreeSettings.MergeMaxBlockSize,
					MergeMaxBlockSizeBytes:  mergeTreeSettings.MergeMaxBlockSizeBytes,
					MergeSelectingSleepMS:   mergeTreeSettings.MergeSelectingSleepMS,
					DefaultCompressionCodec: *defaultCompressionCodec,
				},
			}
			processor.RestartClickHouse = func(ctx context.Context) error {
				if server == nil {
					return errors.New("local ClickHouse server is not running")
				}
				slog.Info("stopping local ClickHouse server for restart", "stage", "restart_clickhouse", "job_id", part.JobID, "part_id", part.PartID)
				if err := server.Stop(); err != nil {
					return fmt.Errorf("stop clickhouse before restart: %w", err)
				}
				server = nil
				slog.Info("starting local ClickHouse server after restart", "stage", "restart_clickhouse", "binary", *clickHouseBinary, "config_file", *clickHouseConfigFile, "clickhouse_data_dir", runDirs.ClickHouse, "job_id", part.JobID, "part_id", part.PartID, "background_pool_size", mergeBackgroundPoolSize, "background_merges_mutations_scheduling_policy", mergeTreeSettings.MergeSchedulingPolicy)
				restarted, err := startServer(ctx, chproc.Tuning{BackgroundPoolSize: mergeBackgroundPoolSize, MergeSchedulingPolicy: mergeTreeSettings.MergeSchedulingPolicy})
				if err != nil {
					return err
				}
				server = restarted
				return nil
			}
			if *stateProgressInterval > 0 {
				processor.ReportProgress = func(ctx context.Context, m manifest.Manifest, snapshot rewrite.ProgressSnapshot) error {
					return stateStore.UpdateRewriteProgress(ctx, m.JobID, m.PartID, resolvedWorkerID, stateProgress(snapshot), time.Now().UTC())
				}
			}
			return processor.ProcessPart(processCtx, workItem)
		}()
		shutdownRequested := partShutdown.Requested()
		shutdownForced := partShutdown.Forced()
		partShutdown.Stop()
		if err != nil {
			if shutdownForced {
				slog.Warn("part processing exceeded shutdown grace period; releasing part back to ready", "stage", "shutdown", "job_id", part.JobID, "part_id", part.PartID, "error", err)
				stateCtx, cancel := workerStateUpdateContext()
				releaseErr := stateStore.ReleaseInProgress(stateCtx, *part, resolvedWorkerID, time.Now().UTC())
				cancel()
				if releaseErr != nil {
					return fmt.Errorf("shutdown release part %s/%s: %w", part.JobID, part.PartID, releaseErr)
				}
				slog.Info("released part back to ready after shutdown grace period", "stage", "shutdown", "job_id", part.JobID, "part_id", part.PartID)
				return nil
			}
			slog.Info("part processing failed; marking failed", "stage", "mark_failed", "job_id", part.JobID, "part_id", part.PartID, "error", err)
			stateCtx, cancel := workerStateUpdateContext()
			markErr := stateStore.MarkFailed(stateCtx, *part, resolvedWorkerID, err, time.Now().UTC())
			cancel()
			if markErr != nil {
				return fmt.Errorf("process part %s/%s: %w; additionally failed to mark failed: %v", part.JobID, part.PartID, err, markErr)
			}
			return err
		}
		slog.Info("marking part compact-ready", "stage", "mark_compact_ready", "job_id", part.JobID, "part_id", part.PartID, "finished_key", result.FinishedKey, "output_parts", result.DestinationStats.Count, "output_bytes", result.DestinationStats.Bytes)
		stateCtx, cancel := workerStateUpdateContext()
		err = stateStore.MarkCompactReady(stateCtx, *part, resolvedWorkerID, result.FinishedKey, result.DestinationDatabase, result.DestinationTable, result.DestinationSchema, state.PartStats{
			Count: result.DestinationStats.Count,
			Rows:  result.DestinationStats.Rows,
			Bytes: result.DestinationStats.Bytes,
		}, partitionCountsFromRewrite(result.DestinationPartitions), time.Now().UTC())
		cancel()
		if err != nil {
			return err
		}
		slog.Info("part marked compact-ready", "stage", "mark_compact_ready", "job_id", part.JobID, "part_id", part.PartID, "finished_key", result.FinishedKey)
		if shutdownRequested {
			slog.Info("worker shutdown requested; stopping after completed part", "stage", "shutdown", "job_id", part.JobID, "part_id", part.PartID)
			return nil
		}
		if *once {
			return nil
		}
	}
}

func createWorkerRunDirs(workDir string) (workerRunDirs, error) {
	root := strings.TrimSpace(workDir)
	if root == "" {
		root = "/tmp/partforge"
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return workerRunDirs{}, fmt.Errorf("resolve worker work-dir %s: %w", workDir, err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return workerRunDirs{}, fmt.Errorf("create worker work-dir %s: %w", abs, err)
	}
	runRoot, err := os.MkdirTemp(abs, "run-")
	if err != nil {
		return workerRunDirs{}, fmt.Errorf("create worker run directory under %s: %w", abs, err)
	}
	dirs := workerRunDirs{
		Root:       runRoot,
		ClickHouse: filepath.Join(runRoot, "clickhouse"),
		Scratch:    filepath.Join(runRoot, "scratch"),
	}
	for _, dir := range []string{dirs.ClickHouse, dirs.Scratch} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			_ = os.RemoveAll(runRoot)
			return workerRunDirs{}, fmt.Errorf("create worker run subdirectory %s: %w", dir, err)
		}
	}
	return dirs, nil
}

type workerCompactionConfig struct {
	StateStore               *state.Store
	WorkerID                 string
	WorkDir                  string
	ClickHouseURL            string
	ClickHouseUser           string
	ClickHousePassword       string
	ClickHouseBinary         string
	ClickHouseConfigFile     string
	S5cmdBinary              string
	S3Endpoint               string
	DefaultCompressionCodec  string
	MergeBackgroundPoolSize  int
	MergeSchedulingPolicy    string
	MergeMaxBlockSize        uint64
	MergeMaxBlockSizeBytes   uint64
	MergeSelectingSleepMS    uint64
	CompactWindow            time.Duration
	CompactMergeIdleTimeout  time.Duration
	CompactMergeMaxRuntime   time.Duration
	CompactLeaseStaleAfter   time.Duration
	CompactHeartbeatInterval time.Duration
	CompactMaxBytes          uint64
	ShutdownGracePeriod      time.Duration
}

func runWorkerCompaction(ctx context.Context, cfg workerCompactionConfig) (bool, error) {
	retryCooldown := compactRetryCooldown(cfg.CompactWindow)
	if delay := compactClaimSplay(cfg.CompactWindow); delay > 0 {
		slog.Info("waiting before compact claim", "stage", "claim_compact_splay", "worker_id", cfg.WorkerID, "delay", delay)
		if err := sleepOrDone(ctx, delay); err != nil {
			if ctx.Err() != nil {
				slog.Info("worker shutdown requested while waiting to claim compact work", "stage", "shutdown")
				return false, nil
			}
			return false, err
		}
	}
	if cfg.CompactLeaseStaleAfter > 0 {
		now := time.Now().UTC()
		released, err := cfg.StateStore.ReleaseStaleCompactingParts(ctx, now, cfg.CompactLeaseStaleAfter, time.Time{})
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("worker shutdown requested while releasing stale compact work", "stage", "shutdown")
				return false, nil
			}
			return false, err
		}
		if released > 0 {
			slog.Warn("released stale compacting parts", "stage", "release_stale_compact", "worker_id", cfg.WorkerID, "released", released, "stale_after", cfg.CompactLeaseStaleAfter)
		}
	}
	finalization := compactFinalizationResult{}
	if cfg.CompactWindow > 0 {
		var err error
		finalization, err = finalizeCompactReadyJobs(ctx, cfg.StateStore, cfg.CompactWindow, time.Now().UTC())
		if err != nil {
			return false, err
		}
		if finalization.Finalized > 0 {
			slog.Info("finalized compact-ready artifacts", "stage", "finalize_compact", "artifacts", finalization.Finalized)
			return true, nil
		}
		if len(finalization.ExpiredJobIDs) > 0 {
			slog.Info("skipping compact claims for jobs past compact window", "stage", "claim_compact", "worker_id", cfg.WorkerID, "jobs", len(finalization.ExpiredJobIDs))
		}
	}
	slog.Info("claiming compact-ready batch", "stage", "claim_compact", "worker_id", cfg.WorkerID)
	batch, err := cfg.StateStore.ClaimNextCompactBatch(ctx, cfg.WorkerID, time.Now().UTC(), state.CompactClaimOptions{
		MaxArtifacts:   defaultCompactMaxArtifacts,
		MaxBytes:       cfg.CompactMaxBytes,
		MinInputParts:  compactMinInputParts,
		ExcludedJobIDs: finalization.ExpiredJobIDs,
	})
	if err != nil {
		if ctx.Err() != nil {
			slog.Info("worker shutdown requested while claiming compact work", "stage", "shutdown")
			return false, nil
		}
		return false, err
	}
	if batch == nil {
		finalization, err := finalizeCompactReadyJobs(ctx, cfg.StateStore, cfg.CompactWindow, time.Now().UTC())
		if err != nil {
			return false, err
		}
		if finalization.Finalized > 0 {
			slog.Info("finalized compact-ready artifacts", "stage", "finalize_compact", "artifacts", finalization.Finalized)
			return true, nil
		}
		return false, nil
	}

	inputIDs := compactBatchPartIDs(batch.Parts)
	outputPartID := manifest.DeriveCompactPartID(inputIDs, batch.Generation)
	outputFinishedKey, err := rewrite.CompactFinishedKeyFromInput(batch.Parts[0].FinishedKey, outputPartID)
	if err != nil {
		return false, err
	}
	slog.Info(
		"claimed compact-ready batch",
		"stage", "claim_compact",
		"job_id", batch.JobID,
		"output_part_id", outputPartID,
		"input_artifacts", len(batch.Parts),
		"input_parts", batch.InputPartCount,
		"input_rows", batch.InputRows,
		"input_bytes", batch.InputBytes,
		"generation", batch.Generation,
	)

	claimedParts := append([]state.Part(nil), batch.Parts...)
	var claimedPartsMu sync.Mutex
	currentBatch := func() state.CompactBatch {
		claimedPartsMu.Lock()
		defer claimedPartsMu.Unlock()
		out := *batch
		out.Parts = append([]state.Part(nil), claimedParts...)
		return out
	}
	workItem := rewrite.CompactWorkItem{
		JobID:               batch.JobID,
		OutputPartID:        outputPartID,
		OutputFinishedKey:   outputFinishedKey,
		DestinationDatabase: batch.Parts[0].DestinationDatabase,
		DestinationTable:    batch.Parts[0].DestinationTable,
		DestinationSchema:   batch.Parts[0].DestinationSchema,
		Inputs:              compactInputs(batch.Parts),
	}
	loadMoreInputs := func(ctx context.Context, current rewrite.CompactLoadState) ([]rewrite.CompactInput, error) {
		claimedPartsMu.Lock()
		claimedCount := len(claimedParts)
		claimedPartsMu.Unlock()
		remainingArtifacts := defaultCompactMaxArtifacts - claimedCount
		if remainingArtifacts <= 0 {
			return nil, nil
		}
		remainingBytes := uint64(0)
		if cfg.CompactMaxBytes > 0 {
			if current.Stats.Bytes >= cfg.CompactMaxBytes {
				return nil, nil
			}
			remainingBytes = cfg.CompactMaxBytes - current.Stats.Bytes
		}
		requiredPartitions := partitionIDsFromRewrite(current.Partitions)
		if len(requiredPartitions) == 0 {
			return nil, nil
		}
		expired, err := compactJobDeadlineExpired(ctx, cfg.StateStore, batch.JobID, cfg.CompactWindow, time.Now().UTC())
		if err != nil || expired {
			return nil, err
		}
		more, err := cfg.StateStore.ClaimNextCompactBatch(ctx, cfg.WorkerID, time.Now().UTC(), state.CompactClaimOptions{
			MaxArtifacts:         remainingArtifacts,
			MaxBytes:             remainingBytes,
			StrictMaxBytes:       remainingBytes > 0,
			MinInputParts:        1,
			JobID:                batch.JobID,
			Bucket:               batch.Parts[0].Bucket,
			DestinationDatabase:  batch.Parts[0].DestinationDatabase,
			DestinationTable:     batch.Parts[0].DestinationTable,
			DestinationSchema:    batch.Parts[0].DestinationSchema,
			RequiredPartitionIDs: requiredPartitions,
		})
		if err != nil || more == nil {
			return nil, err
		}
		claimedPartsMu.Lock()
		claimedParts = append(claimedParts, more.Parts...)
		claimedTotal := len(claimedParts)
		claimedPartsMu.Unlock()
		slog.Info("claimed more compact-ready artifacts for running compaction", "stage", "claim_more_compact", "job_id", batch.JobID, "output_part_id", outputPartID, "claimed_more", len(more.Parts), "claimed_total", claimedTotal, "current_active_parts", current.Stats.Count, "current_active_bytes", current.Stats.Bytes, "current_partitions", len(requiredPartitions))
		return compactInputs(more.Parts), nil
	}
	processCtx, compactShutdown := workerProcessContext(ctx, cfg.ShutdownGracePeriod, batch.JobID, outputPartID)
	processCtx, cancelProcess := context.WithCancel(processCtx)
	heartbeatErrCh := startCompactHeartbeat(processCtx, cfg.StateStore, currentBatch, cfg.WorkerID, cfg.CompactHeartbeatInterval, cancelProcess)
	result, err := processCompactBatch(processCtx, cfg, workItem, currentBatch, loadMoreInputs)
	cancelProcess()
	if heartbeatErr := waitCompactHeartbeat(heartbeatErrCh); heartbeatErr != nil {
		if err == nil || errors.Is(err, context.Canceled) {
			err = heartbeatErr
		} else {
			err = fmt.Errorf("%w; additionally compact heartbeat failed: %v", err, heartbeatErr)
		}
	}
	shutdownRequested := compactShutdown.Requested()
	shutdownForced := compactShutdown.Forced()
	compactShutdown.Stop()
	if err != nil {
		stateCtx, cancel := workerStateUpdateContext()
		releaseErr := cfg.StateStore.ReleaseCompactBatch(stateCtx, currentBatch(), cfg.WorkerID, time.Time{}, time.Now().UTC())
		cancel()
		if shutdownForced {
			if releaseErr != nil {
				return true, fmt.Errorf("shutdown release compact batch %s/%s: %w", batch.JobID, outputPartID, releaseErr)
			}
			slog.Info("released compact batch after shutdown grace period", "stage", "shutdown", "job_id", batch.JobID, "output_part_id", outputPartID)
			return true, nil
		}
		if releaseErr != nil {
			return true, fmt.Errorf("compact batch %s/%s failed: %w; additionally failed to release compact batch: %v", batch.JobID, outputPartID, err, releaseErr)
		}
		return true, err
	}
	if !result.Reduced {
		stateCtx, cancel := workerStateUpdateContext()
		releaseErr := cfg.StateStore.ReleaseCompactBatch(stateCtx, currentBatch(), cfg.WorkerID, time.Now().UTC().Add(retryCooldown), time.Now().UTC())
		cancel()
		if releaseErr != nil {
			return true, releaseErr
		}
		slog.Info("compact batch did not reduce active part count; released with cooldown", "stage", "compact_no_reduction", "job_id", batch.JobID, "output_part_id", outputPartID, "input_parts", result.InputStats.Count, "output_parts", result.DestinationStats.Count, "cooldown", retryCooldown)
		return true, nil
	}

	outputInputIDs := compactResultInputPartIDs(result.Inputs)
	if len(outputInputIDs) == 0 {
		outputInputIDs = inputIDs
	}
	output := state.NewCompactPart(batch.JobID, outputPartID, batch.Parts[0].Bucket, outputFinishedKey, workItem.DestinationDatabase, workItem.DestinationTable, workItem.DestinationSchema, outputInputIDs, batch.Generation, state.PartStats{
		Count: result.DestinationStats.Count,
		Rows:  result.DestinationStats.Rows,
		Bytes: result.DestinationStats.Bytes,
	}, partitionCountsFromRewrite(result.DestinationPartitions), time.Now().UTC())
	stateCtx, cancel := workerStateUpdateContext()
	err = cfg.StateStore.CompleteCompaction(stateCtx, currentBatch(), output, cfg.WorkerID, time.Now().UTC())
	cancel()
	if err != nil {
		releaseCtx, releaseCancel := workerStateUpdateContext()
		releaseErr := cfg.StateStore.ReleaseCompactBatch(releaseCtx, currentBatch(), cfg.WorkerID, time.Time{}, time.Now().UTC())
		releaseCancel()
		if releaseErr != nil {
			return true, fmt.Errorf("complete compaction %s/%s: %w; additionally failed to release compact batch: %v", batch.JobID, outputPartID, err, releaseErr)
		}
		return true, err
	}
	slog.Info("completed compact batch", "stage", "complete_compact", "job_id", batch.JobID, "output_part_id", outputPartID, "finished_key", outputFinishedKey, "input_artifacts", len(claimedParts), "input_parts", result.InputStats.Count, "output_parts", result.DestinationStats.Count, "output_bytes", result.DestinationStats.Bytes)
	if shutdownRequested {
		slog.Info("worker shutdown requested; stopping after completed compaction", "stage", "shutdown", "job_id", batch.JobID, "output_part_id", outputPartID)
	}
	return true, nil
}

func processCompactBatch(ctx context.Context, cfg workerCompactionConfig, item rewrite.CompactWorkItem, compactBatch func() state.CompactBatch, loadMoreInputs func(context.Context, rewrite.CompactLoadState) ([]rewrite.CompactInput, error)) (rewrite.CompactResult, error) {
	runDirs, err := createWorkerRunDirs(cfg.WorkDir)
	if err != nil {
		return rewrite.CompactResult{}, err
	}
	slog.Info("created compact worker run directory", "stage", "compact_prepare_work_dir", "work_dir", cfg.WorkDir, "run_dir", runDirs.Root, "clickhouse_data_dir", runDirs.ClickHouse, "scratch_dir", runDirs.Scratch, "job_id", item.JobID, "output_part_id", item.OutputPartID)

	var server *chproc.Server
	defer func() {
		if server != nil {
			slog.Info("stopping local ClickHouse server", "stage", "compact_stop_clickhouse", "job_id", item.JobID, "output_part_id", item.OutputPartID)
			if err := server.Stop(); err != nil {
				slog.Warn("failed to stop local ClickHouse server", "stage", "compact_stop_clickhouse", "job_id", item.JobID, "output_part_id", item.OutputPartID, "error", err)
			}
		}
		if err := os.RemoveAll(runDirs.Root); err != nil {
			slog.Warn("failed to remove compact worker run directory", "run_dir", runDirs.Root, "job_id", item.JobID, "output_part_id", item.OutputPartID, "error", err)
		}
	}()

	startServer := func(ctx context.Context, tuning chproc.Tuning) (*chproc.Server, error) {
		return chproc.Start(ctx, chproc.Config{
			Binary:     cfg.ClickHouseBinary,
			ConfigFile: cfg.ClickHouseConfigFile,
			DataDir:    runDirs.ClickHouse,
			URL:        cfg.ClickHouseURL,
			User:       cfg.ClickHouseUser,
			Password:   cfg.ClickHousePassword,
			Timeout:    90 * time.Second,
			Tuning:     tuning,
		})
	}
	slog.Info("starting local ClickHouse server for compaction", "stage", "compact_start_clickhouse", "binary", cfg.ClickHouseBinary, "config_file", cfg.ClickHouseConfigFile, "clickhouse_data_dir", runDirs.ClickHouse, "job_id", item.JobID, "output_part_id", item.OutputPartID)
	server, err = startServer(ctx, chproc.Tuning{})
	if err != nil {
		return rewrite.CompactResult{}, err
	}

	ch := chhttp.Client{URL: cfg.ClickHouseURL, User: cfg.ClickHouseUser, Password: cfg.ClickHousePassword}
	compactor := rewrite.Compactor{
		S3Copy:              s3copy.Copier{Binary: cfg.S5cmdBinary, Endpoint: cfg.S3Endpoint},
		ClickHouse:          ch,
		WorkDir:             runDirs.Scratch,
		MergeTimeout:        cfg.CompactMergeIdleTimeout,
		MergeMaxTimeout:     cfg.CompactMergeMaxRuntime,
		MergeSettleMinWait:  derivedMergeSettleMinWait(cfg.CompactMergeIdleTimeout, rewrite.DefaultCompactMergeSettleMinWait),
		MergeSettleMinParts: rewrite.DefaultMergeSettleMinParts,
		LoadMoreInputs:      loadMoreInputs,
		LoadMoreInterval:    compactLoadMoreInterval(cfg.CompactWindow),
		MergeTreeSettings: rewrite.MergeTreeSettings{
			MergeMaxBlockSize:       cfg.MergeMaxBlockSize,
			MergeMaxBlockSizeBytes:  cfg.MergeMaxBlockSizeBytes,
			MergeSelectingSleepMS:   cfg.MergeSelectingSleepMS,
			DefaultCompressionCodec: cfg.DefaultCompressionCodec,
		},
	}
	compactor.ReportProgress = func(ctx context.Context, item rewrite.CompactWorkItem, snapshot rewrite.CompactProgressSnapshot) error {
		stateCtx, cancel := workerStateUpdateContext()
		defer cancel()
		return cfg.StateStore.UpdateCompactProgress(stateCtx, compactBatch(), item.OutputPartID, cfg.WorkerID, state.PartStats{
			Count: snapshot.InputStats.Count,
			Rows:  snapshot.InputStats.Rows,
			Bytes: snapshot.InputStats.Bytes,
		}, state.PartStats{
			Count: snapshot.DestinationStats.Count,
			Rows:  snapshot.DestinationStats.Rows,
			Bytes: snapshot.DestinationStats.Bytes,
		}, time.Now().UTC())
	}
	compactor.RestartClickHouse = func(ctx context.Context) error {
		if server == nil {
			return errors.New("local ClickHouse server is not running")
		}
		slog.Info("stopping local ClickHouse server for compact restart", "stage", "compact_restart_clickhouse", "job_id", item.JobID, "output_part_id", item.OutputPartID)
		if err := server.Stop(); err != nil {
			return fmt.Errorf("stop clickhouse before compact restart: %w", err)
		}
		server = nil
		slog.Info("starting local ClickHouse server after compact restart", "stage", "compact_restart_clickhouse", "binary", cfg.ClickHouseBinary, "config_file", cfg.ClickHouseConfigFile, "clickhouse_data_dir", runDirs.ClickHouse, "job_id", item.JobID, "output_part_id", item.OutputPartID, "background_pool_size", cfg.MergeBackgroundPoolSize, "background_merges_mutations_scheduling_policy", cfg.MergeSchedulingPolicy)
		restarted, err := startServer(ctx, chproc.Tuning{BackgroundPoolSize: cfg.MergeBackgroundPoolSize, MergeSchedulingPolicy: cfg.MergeSchedulingPolicy})
		if err != nil {
			return err
		}
		server = restarted
		return nil
	}
	return compactor.Compact(ctx, item)
}

func compactBatchPartIDs(parts []state.Part) []string {
	ids := make([]string, 0, len(parts))
	for _, part := range parts {
		ids = append(ids, part.PartID)
	}
	sort.Strings(ids)
	return ids
}

func compactInputs(parts []state.Part) []rewrite.CompactInput {
	inputs := make([]rewrite.CompactInput, 0, len(parts))
	for _, part := range parts {
		inputs = append(inputs, rewrite.CompactInput{
			PartID:          part.PartID,
			Bucket:          part.Bucket,
			FinishedKey:     part.FinishedKey,
			Parts:           part.DestinationActivePartCount,
			Rows:            part.DestinationActivePartRows,
			Bytes:           part.DestinationActivePartBytes,
			PartitionCounts: cloneUint64Map(part.DestinationActivePartitionCounts),
		})
	}
	return inputs
}

func compactResultInputPartIDs(inputs []rewrite.CompactInput) []string {
	ids := make([]string, 0, len(inputs))
	for _, input := range inputs {
		ids = append(ids, input.PartID)
	}
	sort.Strings(ids)
	return ids
}

func partitionCountsFromRewrite(partitions []rewrite.PartPartitionStats) map[string]uint64 {
	counts := make(map[string]uint64, len(partitions))
	for _, partition := range partitions {
		if strings.TrimSpace(partition.PartitionID) == "" || partition.Parts == 0 {
			continue
		}
		counts[partition.PartitionID] = partition.Parts
	}
	if len(counts) == 0 {
		return nil
	}
	return counts
}

func partitionIDsFromRewrite(partitions []rewrite.PartPartitionStats) []string {
	ids := make([]string, 0, len(partitions))
	for _, partition := range partitions {
		if strings.TrimSpace(partition.PartitionID) == "" || partition.Parts == 0 {
			continue
		}
		ids = append(ids, partition.PartitionID)
	}
	sort.Strings(ids)
	return ids
}

func cloneUint64Map(values map[string]uint64) map[string]uint64 {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(values))
	for key, value := range values {
		if strings.TrimSpace(key) == "" || value == 0 {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func derivedMergeSettleMinWait(idleTimeout, maxWait time.Duration) time.Duration {
	if idleTimeout <= 0 {
		return 0
	}
	wait := idleTimeout / 4
	if wait < time.Second {
		wait = time.Second
	}
	if maxWait > 0 && wait > maxWait {
		wait = maxWait
	}
	return wait
}

func sourceMergeWaitTimeouts(idleTimeout, maxRuntime time.Duration, compactEnabled bool) (time.Duration, time.Duration) {
	if idleTimeout == 0 {
		idleTimeout = rewrite.DefaultMergeTimeout
	}
	if maxRuntime == 0 {
		maxRuntime = rewrite.DefaultMergeMaxTimeout
	}
	if compactEnabled {
		if idleTimeout > compactSourceMergeWaitCap {
			idleTimeout = compactSourceMergeWaitCap
		}
		if maxRuntime > compactSourceMergeWaitCap {
			maxRuntime = compactSourceMergeWaitCap
		}
	}
	if maxRuntime < idleTimeout {
		maxRuntime = idleTimeout
	}
	return idleTimeout, maxRuntime
}

func compactRetryCooldown(compactWindow time.Duration) time.Duration {
	return time.Minute
}

func compactLoadMoreInterval(compactWindow time.Duration) time.Duration {
	cooldown := compactRetryCooldown(compactWindow)
	interval := cooldown / 60
	if interval < 5*time.Second {
		return 5 * time.Second
	}
	if interval > 30*time.Second {
		return 30 * time.Second
	}
	return interval
}

func compactLeaseStaleAfter(maxRuntime, shutdownGracePeriod time.Duration) time.Duration {
	if maxRuntime <= 0 {
		maxRuntime = rewrite.DefaultCompactMergeMaxTimeout
	}
	if shutdownGracePeriod < 0 {
		shutdownGracePeriod = 0
	}
	staleAfter := maxRuntime + shutdownGracePeriod
	if staleAfter < 5*time.Minute {
		return 5 * time.Minute
	}
	return staleAfter
}

func compactLeaseHeartbeatInterval(staleAfter time.Duration) time.Duration {
	if staleAfter <= 0 {
		return time.Minute
	}
	interval := staleAfter / 20
	if interval < 30*time.Second {
		return 30 * time.Second
	}
	if interval > 5*time.Minute {
		return 5 * time.Minute
	}
	return interval
}

func compactClaimSplay(compactWindow time.Duration) time.Duration {
	maxDelay := compactClaimSplayMax(compactWindow)
	if maxDelay <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(maxDelay) + 1))
}

func compactClaimSplayMax(compactWindow time.Duration) time.Duration {
	if compactWindow <= 0 {
		return 0
	}
	maxDelay := compactRetryCooldown(compactWindow) / 360
	if maxDelay < 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	if maxDelay > 5*time.Second {
		return 5 * time.Second
	}
	return maxDelay
}

type compactFinalizationResult struct {
	Finalized     int
	ExpiredJobIDs map[string]struct{}
}

func finalizeCompactReadyJobs(ctx context.Context, store *state.Store, compactWindow time.Duration, now time.Time) (compactFinalizationResult, error) {
	result := compactFinalizationResult{ExpiredJobIDs: map[string]struct{}{}}
	jobIDs, err := store.ListJobIDs(ctx)
	if err != nil {
		return result, err
	}
	for _, jobID := range jobIDs {
		parts, err := store.ListJobParts(ctx, jobID)
		if err != nil {
			return result, err
		}
		expired, err := compactWindowExpired(parts, compactWindow, now)
		if err != nil {
			return result, err
		}
		if expired {
			result.ExpiredJobIDs[jobID] = struct{}{}
		}
		compactReady, ok, err := finalizableCompactReadyParts(parts, compactWindow, now)
		if err != nil {
			return result, err
		}
		if !ok {
			continue
		}
		for _, part := range compactReady {
			if err := store.MarkCompactReadyFinished(ctx, part, now); err != nil {
				return result, err
			}
			result.Finalized++
		}
	}
	return result, nil
}

func finalizableCompactReadyParts(parts []state.Part, compactWindow time.Duration, now time.Time) ([]state.Part, bool, error) {
	var compactReady []state.Part
	for _, part := range parts {
		switch part.Status {
		case state.StatusReady, state.StatusInProgress, state.StatusCompacting, state.StatusFailed:
			return nil, false, nil
		case state.StatusCompactReady:
			compactReady = append(compactReady, part)
		}
	}
	if len(compactReady) == 0 {
		return nil, false, nil
	}
	if compactWindow <= 0 {
		return compactReady, true, nil
	}
	finalizeAfter, ok, reason := compactFinalizeAfter(parts, compactWindow, now)
	if !ok {
		return nil, false, errors.New(reason)
	}
	if now.Before(finalizeAfter) {
		return nil, false, nil
	}
	return compactReady, true, nil
}

func compactJobDeadlineExpired(ctx context.Context, store *state.Store, jobID string, compactWindow time.Duration, now time.Time) (bool, error) {
	if compactWindow <= 0 {
		return false, nil
	}
	parts, err := store.ListJobParts(ctx, jobID)
	if err != nil {
		return false, err
	}
	return compactWindowExpired(parts, compactWindow, now)
}

func compactWindowExpired(parts []state.Part, compactWindow time.Duration, now time.Time) (bool, error) {
	if compactWindow <= 0 {
		return true, nil
	}
	finalizeAfter, ok, reason := compactFinalizeAfter(parts, compactWindow, now)
	if !ok {
		if reason == "no current compact-ready or compacting timestamp found" {
			return false, nil
		}
		return false, errors.New(reason)
	}
	return !now.Before(finalizeAfter), nil
}

func compactReadySince(part state.Part) (time.Time, error) {
	for _, candidate := range []struct {
		name  string
		value string
	}{
		{name: "compact_ready_at", value: part.CompactReadyAt},
		{name: "progress_updated_at", value: part.ProgressUpdatedAt},
		{name: "updated_at", value: part.UpdatedAt},
	} {
		if strings.TrimSpace(candidate.value) == "" {
			continue
		}
		t, err := time.Parse(time.RFC3339Nano, candidate.value)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse %s for compact-ready part %s: %w", candidate.name, part.PartID, err)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("compact-ready part %s has no compact_ready_at, progress_updated_at, or updated_at", part.PartID)
}

func runImportFinished(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import-finished", flag.ExitOnError)
	var (
		configPath         = fs.String("config", defaultConfigPath, "JSON config file path")
		database           = fs.String("database", "", "final destination database")
		table              = fs.String("table", "", "final destination table")
		jobID              = fs.String("job-id", "", "job id to import")
		partID             = fs.String("part-id", "", "optional finished part id to import")
		stateTable         = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region             = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint         = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary        = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		dynamoEndpoint     = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		clickHouseURL      = fs.String("clickhouse-url", defaultClickHouseURL, "destination ClickHouse HTTP URL")
		clickHouseUser     = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		workDir            = fs.String("work-dir", "", "import scratch directory; empty uses the destination ClickHouse disk")
		requireEmpty       = fs.Bool("require-empty", true, "fail if the destination table already has active parts")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "import-finished"); err != nil {
		return err
	}
	if err := applyClickHouseClientConfigDefaults(clickHouseUser, clickHousePassword); err != nil {
		return err
	}
	if *database == "" || *table == "" || *jobID == "" {
		return errors.New("database, table, and job-id are required")
	}

	slog.Info(
		"import-finished started",
		"stage", "start",
		"job_id", *jobID,
		"part_id", *partID,
		"destination_table", chhttp.TableSQL(*database, *table),
		"work_dir", *workDir,
		"require_empty", *requireEmpty,
	)
	slog.Info("initializing DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	slog.Info("initialized DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	slog.Info("listing finished parts", "stage", "list_finished_parts", "job_id", *jobID)
	finishedParts, err := stateStore.ListFinishedParts(ctx, *jobID)
	if err != nil {
		return err
	}
	slog.Info("listed finished parts", "stage", "list_finished_parts", "job_id", *jobID, "finished_parts", len(finishedParts))
	finishedParts, err = selectImportFinishedParts(finishedParts, *partID)
	if err != nil {
		return err
	}
	slog.Info("selected finished parts for import", "stage", "list_finished_parts", "job_id", *jobID, "part_id", *partID, "import_parts", len(finishedParts))
	artifacts := make([]parts.FinishedArtifact, 0, len(finishedParts))
	partsByID := make(map[string]state.Part, len(finishedParts))
	for _, part := range finishedParts {
		artifacts = append(artifacts, parts.FinishedArtifact{
			Bucket: part.Bucket,
			Key:    part.FinishedKey,
			PartID: part.PartID,
		})
		partsByID[part.PartID] = part
	}

	importer := parts.Importer{
		S3Copy:     s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint},
		ClickHouse: chhttp.Client{URL: *clickHouseURL, User: *clickHouseUser, Password: *clickHousePassword},
		WorkDir:    *workDir,
	}
	return importer.ImportJob(ctx, parts.ImportJob{
		Artifacts:    artifacts,
		JobID:        *jobID,
		Database:     *database,
		Table:        *table,
		RequireEmpty: *requireEmpty,
		MarkImporting: func(ctx context.Context, artifact parts.FinishedArtifact) error {
			part, ok := partsByID[artifact.PartID]
			if !ok {
				return fmt.Errorf("missing state for part %s", artifact.PartID)
			}
			return stateStore.MarkImporting(ctx, part, time.Now().UTC())
		},
		MarkImported: func(ctx context.Context, artifact parts.FinishedArtifact) error {
			part, ok := partsByID[artifact.PartID]
			if !ok {
				return fmt.Errorf("missing state for part %s", artifact.PartID)
			}
			return stateStore.MarkImported(ctx, part, time.Now().UTC())
		},
		MarkImportFailed: func(ctx context.Context, artifact parts.FinishedArtifact, cause error) error {
			part, ok := partsByID[artifact.PartID]
			if !ok {
				return fmt.Errorf("missing state for part %s", artifact.PartID)
			}
			return stateStore.MarkImportFailed(ctx, part, cause, time.Now().UTC())
		},
	})
}

func runListJobs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-jobs", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "list-jobs"); err != nil {
		return err
	}
	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobIDs, err := stateStore.ListJobIDs(ctx)
	if err != nil {
		return err
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, map[string][]string{"jobs": jobIDs})
	}
	for _, jobID := range jobIDs {
		fmt.Println(jobID)
	}
	return nil
}

func runJobStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("job-status", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id to inspect")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		compactWindow  = fs.Duration("compact-window", defaultCompactWindow, "worker compact window used to report compact finalization ETA")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
		showParts      = fs.Bool("parts", false, "include per-part state rows")
		showDetails    = fs.Bool("details", false, "include per-part rewrite stage timing details")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "job-status"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	if *compactWindow < 0 {
		return fmt.Errorf("compact-window must be non-negative, got %s", *compactWindow)
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	summary := summarizeJobWithOptions(*jobID, jobParts, jobSummaryOptions{
		Now:           time.Now().UTC(),
		CompactWindow: *compactWindow,
	})
	if *jsonOutput {
		out := jobStatusOutput{Summary: summary}
		if *showParts || *showDetails {
			out.Parts = jobParts
		}
		return writeJSON(os.Stdout, out)
	}
	printJobSummary(os.Stdout, summary)
	if *showParts {
		printPartRows(os.Stdout, jobParts)
	}
	if *showDetails {
		printPartDetails(os.Stdout, jobParts)
	}
	return nil
}

func selectImportFinishedParts(finishedParts []state.Part, partID string) ([]state.Part, error) {
	partID = strings.TrimSpace(partID)
	if partID == "" {
		return finishedParts, nil
	}
	for _, part := range finishedParts {
		if part.PartID == partID {
			return []state.Part{part}, nil
		}
	}
	return nil, fmt.Errorf("finished part %s was not found in job", partID)
}

func runRetryFailed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("retry-failed", flag.ExitOnError)
	var (
		configPath        = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID             = fs.String("job-id", "", "job id containing failed parts")
		partID            = fs.String("part-id", "", "specific part id to retry")
		all               = fs.Bool("all", false, "retry all failed parts in the job")
		stale             = fs.Bool("stale", false, "retry IN_PROGRESS parts with stale persisted progress")
		staleAfter        = fs.Duration("stale-after", defaultRetryStaleAfter, "minimum age of progress_updated_at for -stale")
		includeInProgress = fs.Bool("include-in-progress", false, "also retry IN_PROGRESS parts by returning them to READY")
		force             = fs.Bool("force", false, "retry selected parts regardless of current state")
		stateTable        = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region            = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint    = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput        = fs.Bool("json", false, "print JSON output")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "retry-failed"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	selectors := 0
	if *all {
		selectors++
	}
	if *partID != "" {
		selectors++
	}
	if *stale {
		selectors++
	}
	if selectors != 1 {
		return errors.New("exactly one of -all, -part-id, or -stale is required")
	}
	if *stale && *staleAfter <= 0 {
		return errors.New("stale-after must be greater than zero")
	}
	if *force && (*includeInProgress || *stale) {
		return errors.New("force cannot be combined with include-in-progress or stale")
	}
	if *stale && *includeInProgress {
		return errors.New("stale cannot be combined with include-in-progress")
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	retryParts, err := selectRetryParts(jobParts, retryPartSelection{
		All:               *all,
		Force:             *force,
		IncludeInProgress: *includeInProgress,
		Stale:             *stale,
		StaleAfter:        *staleAfter,
		Now:               time.Now().UTC(),
		PartID:            *partID,
	})
	if err != nil {
		return err
	}

	var results []retryResult
	for _, part := range retryParts {
		var target state.Status
		if *force {
			target, err = stateStore.ForceRetryPart(ctx, part, time.Now().UTC())
		} else if *stale {
			target, err = stateStore.RetryStaleInProgressPart(ctx, part, time.Now().UTC())
		} else if part.Status == state.StatusInProgress {
			target, err = stateStore.RetryInProgressPart(ctx, part, time.Now().UTC())
		} else {
			target, err = stateStore.RetryFailedPart(ctx, part, time.Now().UTC())
		}
		if err != nil {
			return err
		}
		results = append(results, retryResult{
			PartID: part.PartID,
			From:   string(part.Status),
			To:     string(target),
		})
	}

	out := retryFailedOutput{
		JobID:      *jobID,
		Forced:     *force,
		Stale:      *stale,
		StaleAfter: staleAfterString(*stale, *staleAfter),
		Retried:    len(results),
		Parts:      results,
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printRetryResults(os.Stdout, out)
	return nil
}

func runResetJob(ctx context.Context, args []string) error {
	return runResetState(ctx, args, resetModeJob)
}

func runSetPartState(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("set-part-state", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id containing parts to update")
		partIDs        partIDListFlag
		status         = fs.String("status", "", "update parts currently in this exact state, e.g. COMPACTING")
		toStatus       = fs.String("to-status", "", "target stable state: READY, COMPACT_READY, or FINISHED")
		force          = fs.Bool("force", false, "required to update selected part rows")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	fs.Var(&partIDs, "part-id", "specific part id to update; may be repeated")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "set-part-state"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	target := state.Status(strings.TrimSpace(*toStatus))
	if target == "" {
		return errors.New("to-status is required")
	}
	if !adminSettableStatus(target) {
		return fmt.Errorf("to-status must be one of READY, COMPACT_READY, or FINISHED, got %q", target)
	}
	if !*force {
		return errors.New("set-part-state requires -force")
	}
	selectors := 0
	if strings.TrimSpace(*status) != "" {
		selectors++
	}
	if len(partIDs) > 0 {
		selectors++
	}
	if selectors != 1 {
		return errors.New("exactly one of -status or -part-id is required")
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	selectedParts, err := selectSetPartStateParts(jobParts, setPartStateSelection{
		PartIDs: []string(partIDs),
		Status:  state.Status(strings.TrimSpace(*status)),
	})
	if err != nil {
		return err
	}
	if len(selectedParts) == 0 {
		return fmt.Errorf("no parts matched set-part-state selection for job %s", *jobID)
	}

	results := make([]setPartStateResult, 0, len(selectedParts))
	now := time.Now().UTC()
	for _, part := range selectedParts {
		if err := stateStore.ForceSetPartStatus(ctx, part, target, now); err != nil {
			return err
		}
		results = append(results, setPartStateResult{
			PartID: part.PartID,
			From:   string(part.Status),
			To:     string(target),
		})
	}

	out := setPartStateOutput{
		JobID:    *jobID,
		ToStatus: string(target),
		Updated:  len(results),
		Parts:    results,
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printSetPartStateResult(os.Stdout, out)
	return nil
}

func runResetCompactTimer(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reset-compact-timer", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id whose compact timer should be reset")
		force          = fs.Bool("force", false, "required to reset compact timer")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "reset-compact-timer"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	if !*force {
		return errors.New("reset-compact-timer requires -force")
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	if len(jobParts) == 0 {
		return fmt.Errorf("job %s has no parts", *jobID)
	}

	now := time.Now().UTC()
	for _, part := range jobParts {
		if err := stateStore.ResetCompactTimer(ctx, part, now); err != nil {
			return err
		}
	}

	out := resetCompactTimerOutput{
		JobID:   *jobID,
		ResetAt: now.Format(time.RFC3339Nano),
		Updated: len(jobParts),
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printResetCompactTimerResult(os.Stdout, out)
	return nil
}

func runResetCompaction(ctx context.Context, args []string) error {
	return runResetState(ctx, args, resetModeCompaction)
}

func runResetState(ctx context.Context, args []string, mode resetMode) error {
	fs := flag.NewFlagSet(string(mode), flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id to reset")
		force          = fs.Bool("force", false, "required to reset selected job state")
		deleteS3       = fs.Bool("delete-s3", false, "also delete reset artifact S3 prefixes")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint     = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary    = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, string(mode)); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	if !*force {
		return fmt.Errorf("%s requires -force", mode)
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	plan, err := buildResetPlan(*jobID, jobParts, mode)
	if err != nil {
		return err
	}
	s3Prefixes := resetS3Prefixes(plan)

	if len(plan.GeneratedParts) > 0 {
		if err := stateStore.DeleteJobParts(ctx, plan.GeneratedParts); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	for _, part := range plan.OriginalParts {
		switch mode {
		case resetModeJob:
			err = stateStore.ResetOriginalPartToReady(ctx, part, now)
		case resetModeCompaction:
			err = stateStore.ResetOriginalPartToCompactReady(ctx, part, now)
		default:
			err = fmt.Errorf("unknown reset mode %q", mode)
		}
		if err != nil {
			return err
		}
	}

	var deletedPrefixes []jobS3Prefix
	if *deleteS3 {
		copier := s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint}
		for _, prefix := range s3Prefixes {
			slog.Info("deleting reset S3 prefix", "stage", "reset_delete_s3", "job_id", *jobID, "mode", mode, "bucket", prefix.Bucket, "prefix", prefix.Prefix)
			if err := copier.DeletePrefixIfExists(ctx, prefix.Bucket, prefix.Prefix); err != nil {
				return fmt.Errorf("delete s3://%s/%s: %w", prefix.Bucket, prefix.Prefix, err)
			}
			deletedPrefixes = append(deletedPrefixes, prefix)
		}
	}

	out := resetStateOutput{
		JobID:              *jobID,
		Mode:               string(mode),
		TargetStatus:       string(plan.TargetStatus),
		OriginalsReset:     len(plan.OriginalParts),
		CompactRowsDeleted: len(plan.GeneratedParts),
		DeleteS3:           *deleteS3,
		S3PrefixesDeleted:  deletedPrefixes,
		Parts:              resetStatePartResults(plan),
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printResetStateResult(os.Stdout, out)
	return nil
}

func runDeleteParts(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete-parts", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id containing parts to delete")
		partIDs        partIDListFlag
		status         = fs.String("status", "", "delete parts in this exact state, e.g. IMPORTED")
		all            = fs.Bool("all", false, "delete every part row in the job")
		force          = fs.Bool("force", false, "required to delete selected part rows")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	fs.Var(&partIDs, "part-id", "specific part id to delete; may be repeated")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "delete-parts"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	if !*force {
		return errors.New("delete-parts requires -force")
	}
	selectors := 0
	if *all {
		selectors++
	}
	if strings.TrimSpace(*status) != "" {
		selectors++
	}
	if len(partIDs) > 0 {
		selectors++
	}
	if selectors != 1 {
		return errors.New("exactly one of -all, -status, or -part-id is required")
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	deleteParts, err := selectDeleteParts(jobParts, deletePartSelection{
		All:     *all,
		PartIDs: []string(partIDs),
		Status:  state.Status(strings.TrimSpace(*status)),
	})
	if err != nil {
		return err
	}
	if len(deleteParts) == 0 {
		return fmt.Errorf("no parts matched delete selection for job %s", *jobID)
	}
	if err := stateStore.DeleteJobParts(ctx, deleteParts); err != nil {
		return err
	}

	results := make([]deletePartResult, 0, len(deleteParts))
	for _, part := range deleteParts {
		results = append(results, deletePartResult{
			PartID: part.PartID,
			Status: string(part.Status),
		})
	}
	out := deletePartsOutput{
		JobID:   *jobID,
		Deleted: len(results),
		Parts:   results,
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printDeletePartsResult(os.Stdout, out)
	return nil
}

func runDeleteJob(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete-job", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id to delete")
		deleteS3       = fs.Bool("delete-s3", false, "also delete this job's S3 artifacts")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint     = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary    = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "delete-job"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}

	slog.Info("delete-job started", "stage", "start", "job_id", *jobID, "delete_s3", *deleteS3)
	slog.Info("initializing DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	slog.Info("initialized DynamoDB state store", "stage", "init_state", "state_table", *stateTable)
	slog.Info("listing job parts", "stage", "list_job_parts", "job_id", *jobID)
	jobParts, err := stateStore.ListJobParts(ctx, *jobID)
	if err != nil {
		return err
	}
	if len(jobParts) == 0 {
		return fmt.Errorf("job %s has no state rows", *jobID)
	}
	slog.Info("listed job parts", "stage", "list_job_parts", "job_id", *jobID, "parts", len(jobParts))

	var deletedPrefixes []jobS3Prefix
	if *deleteS3 {
		deletedPrefixes, err = jobS3Prefixes(*jobID, jobParts)
		if err != nil {
			return err
		}
		copier := s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint}
		for _, prefix := range deletedPrefixes {
			slog.Info("deleting job S3 prefix", "stage", "delete_s3", "job_id", *jobID, "bucket", prefix.Bucket, "prefix", prefix.Prefix)
			if err := copier.DeletePrefix(ctx, prefix.Bucket, prefix.Prefix); err != nil {
				return fmt.Errorf("delete s3://%s/%s: %w", prefix.Bucket, prefix.Prefix, err)
			}
			slog.Info("deleted job S3 prefix", "stage", "delete_s3", "job_id", *jobID, "bucket", prefix.Bucket, "prefix", prefix.Prefix)
		}
	}

	slog.Info("deleting job state rows", "stage", "delete_state", "job_id", *jobID, "parts", len(jobParts))
	if err := stateStore.DeleteJobParts(ctx, jobParts); err != nil {
		return err
	}
	slog.Info("deleted job state rows", "stage", "delete_state", "job_id", *jobID, "parts", len(jobParts))

	out := deleteJobOutput{
		JobID:             *jobID,
		StatePartsDeleted: len(jobParts),
		DeleteS3:          *deleteS3,
		S3PrefixesDeleted: deletedPrefixes,
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printDeleteJobResult(os.Stdout, out)
	return nil
}

func readRequiredFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	if len(strings.TrimSpace(string(b))) == 0 {
		return "", fmt.Errorf("%s is empty", path)
	}
	return string(b), nil
}

func destinationTableRefFromSchema(schema string) (manifest.TableRef, error) {
	schemaDatabase, schemaTable, hasDatabase, err := ddl.TableName(schema)
	if err != nil {
		return manifest.TableRef{}, fmt.Errorf("parse destination schema table name: %w", err)
	}
	if !hasDatabase {
		return manifest.TableRef{}, fmt.Errorf("destination schema CREATE TABLE must include a database-qualified table name")
	}
	return manifest.TableRef{Database: schemaDatabase, Table: schemaTable}, nil
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	return fs.Parse(filterUnknownFlags(fs, args))
}

func filterUnknownFlags(fs *flag.FlagSet, args []string) []string {
	known := map[string]struct{}{}
	fs.VisitAll(func(f *flag.Flag) {
		known[f.Name] = struct{}{}
	})

	filtered := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			filtered = append(filtered, args[i:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			filtered = append(filtered, arg)
			continue
		}

		name, hasInlineValue := flagNameAndValue(arg)
		if _, ok := known[name]; ok {
			filtered = append(filtered, arg)
			continue
		}

		fmt.Fprintf(os.Stderr, "warning: flag is not recognised; continuing anyway: %s\n", arg)
		if !hasInlineValue && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			i++
		}
	}
	return filtered
}

func flagNameAndValue(arg string) (string, bool) {
	trimmed := strings.TrimLeft(arg, "-")
	name, _, hasValue := strings.Cut(trimmed, "=")
	return name, hasValue
}

func applyConfigDefaults(fs *flag.FlagSet, path, command string) error {
	cfg, err := readConfig(path)
	if err != nil {
		return err
	}
	if len(cfg) == 0 {
		return nil
	}
	values := map[string]any{}
	for k, v := range cfg {
		if k == "commands" {
			continue
		}
		values[k] = v
	}
	if commands, ok := cfg["commands"].(map[string]any); ok {
		if commandValues, ok := commands[command].(map[string]any); ok {
			for k, v := range commandValues {
				values[k] = v
			}
		}
	}

	var firstErr error
	fs.VisitAll(func(f *flag.Flag) {
		if firstErr != nil || flagWasSet(fs, f.Name) {
			return
		}
		value, ok := configValue(values, f.Name)
		if !ok {
			return
		}
		if err := fs.Set(f.Name, value); err != nil {
			firstErr = fmt.Errorf("apply config %s for -%s: %w", path, f.Name, err)
		}
	})
	return firstErr
}

type clickHouseClientCredentials struct {
	User     string `xml:"user"`
	Password string `xml:"password"`
}

func applyClickHouseClientConfigDefaults(user, password *string) error {
	return applyClickHouseClientConfigDefaultsFrom(defaultClickHouseClientConfigPath, user, password)
}

func applyClickHouseClientConfigDefaultsFrom(path string, user, password *string) error {
	if user == nil || password == nil {
		return errors.New("clickhouse credential defaults require user and password targets")
	}
	needsUser := strings.TrimSpace(*user) == ""
	needsPassword := *password == ""
	if !needsUser && !needsPassword {
		return nil
	}

	creds, err := readClickHouseClientCredentials(path)
	if err != nil {
		return err
	}
	if needsUser && creds.User != "" {
		*user = creds.User
	}
	if needsPassword && creds.Password != "" {
		*password = creds.Password
	}
	if strings.TrimSpace(*user) == "" && *password != "" {
		*user = "default"
	}
	return nil
}

func readClickHouseClientCredentials(path string) (clickHouseClientCredentials, error) {
	if strings.TrimSpace(path) == "" {
		return clickHouseClientCredentials{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return clickHouseClientCredentials{}, nil
		}
		return clickHouseClientCredentials{}, fmt.Errorf("read clickhouse client config %s: %w", path, err)
	}
	var creds clickHouseClientCredentials
	if err := xml.Unmarshal(b, &creds); err != nil {
		return clickHouseClientCredentials{}, fmt.Errorf("parse clickhouse client config %s: %w", path, err)
	}
	creds.User = strings.TrimSpace(creds.User)
	creds.Password = strings.TrimSpace(creds.Password)
	return creds, nil
}

func readConfig(path string) (map[string]any, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && path == defaultConfigPath {
			return nil, nil
		}
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return cfg, nil
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	wasSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			wasSet = true
		}
	})
	return wasSet
}

func configValue(values map[string]any, flagName string) (string, bool) {
	raw, ok := values[flagName]
	if !ok {
		raw, ok = values[strings.ReplaceAll(flagName, "-", "_")]
	}
	if !ok || raw == nil {
		return "", false
	}
	switch v := raw.(type) {
	case string:
		return v, true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v)), true
		}
		return fmt.Sprintf("%g", v), true
	default:
		return "", false
	}
}

func resolveWorkerID(configured string) (string, error) {
	if strings.TrimSpace(configured) != "" {
		return configured, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve worker hostname: %w", err)
	}
	if strings.TrimSpace(hostname) == "" {
		return "", errors.New("resolved empty worker hostname")
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid()), nil
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type workerPartShutdown struct {
	requested <-chan struct{}
	forced    <-chan struct{}
	stop      func()
}

func (s workerPartShutdown) Requested() bool {
	select {
	case <-s.requested:
		return true
	default:
		return false
	}
}

func (s workerPartShutdown) Forced() bool {
	select {
	case <-s.forced:
		return true
	default:
		return false
	}
}

func (s workerPartShutdown) Stop() {
	if s.stop != nil {
		s.stop()
	}
}

func workerProcessContext(shutdownCtx context.Context, gracePeriod time.Duration, jobID, partID string) (context.Context, workerPartShutdown) {
	processCtx, cancel := context.WithCancel(context.Background())
	requested := make(chan struct{})
	forced := make(chan struct{})
	done := make(chan struct{})
	var doneOnce sync.Once

	stop := func() {
		doneOnce.Do(func() {
			close(done)
			cancel()
		})
	}

	go func() {
		select {
		case <-shutdownCtx.Done():
			close(requested)
			slog.Info(
				"worker shutdown requested; waiting for current part",
				"stage", "shutdown",
				"job_id", jobID,
				"part_id", partID,
				"shutdown_grace_period", gracePeriod,
			)
			if gracePeriod <= 0 {
				close(forced)
				slog.Warn("worker shutdown grace period expired; canceling current part", "stage", "shutdown", "job_id", jobID, "part_id", partID)
				cancel()
				return
			}
			timer := time.NewTimer(gracePeriod)
			defer timer.Stop()
			select {
			case <-done:
			case <-timer.C:
				close(forced)
				slog.Warn("worker shutdown grace period expired; canceling current part", "stage", "shutdown", "job_id", jobID, "part_id", partID)
				cancel()
			}
		case <-done:
		}
	}()

	return processCtx, workerPartShutdown{requested: requested, forced: forced, stop: stop}
}

func workerStateUpdateContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), workerStateUpdateTimeout)
}

func startCompactHeartbeat(ctx context.Context, store *state.Store, batch func() state.CompactBatch, workerID string, interval time.Duration, cancelProcess context.CancelFunc) <-chan error {
	errCh := make(chan error, 1)
	if interval <= 0 {
		close(errCh)
		return errCh
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		defer close(errCh)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stateCtx, cancel := workerStateUpdateContext()
				err := store.HeartbeatCompactBatch(stateCtx, batch(), workerID, time.Now().UTC())
				cancel()
				if err != nil {
					errCh <- err
					cancelProcess()
					return
				}
			}
		}
	}()
	return errCh
}

func waitCompactHeartbeat(errCh <-chan error) error {
	if errCh == nil {
		return nil
	}
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func stateProgress(snapshot rewrite.ProgressSnapshot) state.RewriteProgress {
	var progress state.RewriteProgress
	if snapshot.QueryProgress != nil {
		progress.QueryProgress = &state.QueryProgress{
			ReadRows:     snapshot.QueryProgress.ReadRows,
			ReadBytes:    snapshot.QueryProgress.ReadBytes,
			WrittenRows:  snapshot.QueryProgress.WrittenRows,
			WrittenBytes: snapshot.QueryProgress.WrittenBytes,
		}
	}
	if snapshot.SourceActivePartStats != nil {
		progress.SourceActivePartStats = &state.PartStats{
			Count: snapshot.SourceActivePartStats.Count,
			Rows:  snapshot.SourceActivePartStats.Rows,
			Bytes: snapshot.SourceActivePartStats.Bytes,
		}
	}
	if snapshot.DestinationActivePartStats != nil {
		progress.DestinationActivePartStats = &state.PartStats{
			Count: snapshot.DestinationActivePartStats.Count,
			Rows:  snapshot.DestinationActivePartStats.Rows,
			Bytes: snapshot.DestinationActivePartStats.Bytes,
		}
	}
	if snapshot.DestinationFailedMerges != nil {
		progress.DestinationFailedMerges = snapshot.DestinationFailedMerges
	}
	if snapshot.StageProgress != nil {
		completed := make(map[string]int64, len(snapshot.StageProgress.CompletedStageDurations))
		for stage, duration := range snapshot.StageProgress.CompletedStageDurations {
			completed[stage] = duration.Milliseconds()
		}
		progress.StageProgress = &state.RewriteStageProgress{
			Stage:                     snapshot.StageProgress.Stage,
			StageStartedAt:            snapshot.StageProgress.StageStartedAt,
			StageElapsedMs:            snapshot.StageProgress.StageElapsed.Milliseconds(),
			TotalElapsedMs:            snapshot.StageProgress.TotalElapsed.Milliseconds(),
			CompletedStageDurationsMs: completed,
		}
	}
	return progress
}

type jobSummary struct {
	JobID                        string                 `json:"job_id"`
	Status                       string                 `json:"status"`
	Total                        int                    `json:"total"`
	Counts                       map[state.Status]int   `json:"counts"`
	StatePartStats               []statusPartStats      `json:"-"`
	InProgressStages             []inProgressStageCount `json:"in_progress_stages,omitempty"`
	Compact                      *compactJobSummary     `json:"compact,omitempty"`
	RewriteCompleted             int                    `json:"rewrite_completed"`
	RewritePercent               float64                `json:"rewrite_percent"`
	ImportCompleted              int                    `json:"import_completed"`
	ImportPercent                float64                `json:"import_percent"`
	InputClickHouseParts         uint64                 `json:"input_clickhouse_parts"`
	CurrentOutputClickHouseParts uint64                 `json:"current_output_clickhouse_parts"`
	ReadRows                     uint64                 `json:"read_rows"`
	ReadBytes                    uint64                 `json:"read_bytes"`
	WrittenRows                  uint64                 `json:"written_rows"`
	WrittenBytes                 uint64                 `json:"written_bytes"`
	FailedMerges                 uint64                 `json:"failed_merges"`
	FailedParts                  []failedPart           `json:"failed_parts,omitempty"`
}

type jobSummaryOptions struct {
	Now           time.Time
	CompactWindow time.Duration
}

type compactJobSummary struct {
	ReadyParts             int           `json:"ready_parts"`
	CompactingParts        int           `json:"compacting_parts"`
	SoloRetryCooldownParts int           `json:"solo_retry_cooldown_parts"`
	Window                 string        `json:"window"`
	FinalizeStatus         string        `json:"finalize_status"`
	FinalizeAfter          string        `json:"finalize_after,omitempty"`
	FinalizeIn             string        `json:"finalize_in,omitempty"`
	BlockedBy              []statusCount `json:"blocked_by,omitempty"`
	BlockedByMessage       string        `json:"blocked_by_message,omitempty"`
	Reason                 string        `json:"reason,omitempty"`
}

type statusCount struct {
	Status state.Status `json:"status"`
	Count  int          `json:"count"`
}

type inProgressStageCount struct {
	Stage                 string `json:"stage"`
	Count                 int    `json:"count"`
	InputClickHouseParts  uint64 `json:"-"`
	OutputClickHouseParts uint64 `json:"-"`
}

type statusPartStats struct {
	Status                state.Status `json:"status"`
	Count                 int          `json:"count"`
	InputClickHouseParts  uint64       `json:"input_clickhouse_parts"`
	OutputClickHouseParts uint64       `json:"output_clickhouse_parts"`
}

type failedPart struct {
	PartID    string `json:"part_id"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updated_at"`
	Error     string `json:"error"`
}

type jobStatusOutput struct {
	Summary jobSummary   `json:"summary"`
	Parts   []state.Part `json:"parts,omitempty"`
}

type retryFailedOutput struct {
	JobID      string        `json:"job_id"`
	Forced     bool          `json:"forced"`
	Stale      bool          `json:"stale,omitempty"`
	StaleAfter string        `json:"stale_after,omitempty"`
	Retried    int           `json:"retried"`
	Parts      []retryResult `json:"parts"`
}

type retryResult struct {
	PartID string `json:"part_id"`
	From   string `json:"from"`
	To     string `json:"to"`
}

type setPartStateOutput struct {
	JobID    string               `json:"job_id"`
	ToStatus string               `json:"to_status"`
	Updated  int                  `json:"updated"`
	Parts    []setPartStateResult `json:"parts"`
}

type setPartStateResult struct {
	PartID string `json:"part_id"`
	From   string `json:"from"`
	To     string `json:"to"`
}

type resetCompactTimerOutput struct {
	JobID   string `json:"job_id"`
	ResetAt string `json:"reset_at"`
	Updated int    `json:"updated"`
}

type resetMode string

const (
	resetModeJob        resetMode = "reset-job"
	resetModeCompaction resetMode = "reset-compaction"
)

type resetPlan struct {
	JobID          string
	Mode           resetMode
	TargetStatus   state.Status
	OriginalParts  []state.Part
	GeneratedParts []state.Part
}

type resetStateOutput struct {
	JobID              string            `json:"job_id"`
	Mode               string            `json:"mode"`
	TargetStatus       string            `json:"target_status"`
	OriginalsReset     int               `json:"originals_reset"`
	CompactRowsDeleted int               `json:"compact_rows_deleted"`
	DeleteS3           bool              `json:"delete_s3"`
	S3PrefixesDeleted  []jobS3Prefix     `json:"s3_prefixes_deleted,omitempty"`
	Parts              []resetPartResult `json:"parts"`
}

type resetPartResult struct {
	PartID string `json:"part_id"`
	From   string `json:"from"`
	To     string `json:"to,omitempty"`
	Action string `json:"action"`
}

type deletePartsOutput struct {
	JobID   string             `json:"job_id"`
	Deleted int                `json:"deleted"`
	Parts   []deletePartResult `json:"parts"`
}

type deletePartResult struct {
	PartID string `json:"part_id"`
	Status string `json:"status"`
}

type deleteJobOutput struct {
	JobID             string        `json:"job_id"`
	StatePartsDeleted int           `json:"state_parts_deleted"`
	DeleteS3          bool          `json:"delete_s3"`
	S3PrefixesDeleted []jobS3Prefix `json:"s3_prefixes_deleted,omitempty"`
}

type jobS3Prefix struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"`
}

func summarizeJob(jobID string, parts []state.Part) jobSummary {
	return summarizeJobWithOptions(jobID, parts, jobSummaryOptions{
		Now:           time.Now().UTC(),
		CompactWindow: defaultCompactWindow,
	})
}

func summarizeJobWithOptions(jobID string, parts []state.Part, opts jobSummaryOptions) jobSummary {
	counts := make(map[state.Status]int, len(statusOrder()))
	for _, status := range statusOrder() {
		counts[status] = 0
	}

	partsByID := make(map[string]state.Part, len(parts))
	for _, part := range parts {
		partsByID[part.PartID] = part
	}

	var failed []failedPart
	var inputClickHouseParts, currentOutputClickHouseParts, readRows, readBytes, writtenRows, writtenBytes, failedMerges uint64
	stageCounts := map[string]int{}
	stageInputParts := map[string]uint64{}
	stageOutputParts := map[string]uint64{}
	stateInputParts := map[state.Status]uint64{}
	stateOutputParts := map[state.Status]uint64{}
	seenCompactProgress := map[string]struct{}{}
	for _, part := range parts {
		counts[part.Status]++
		if !isGeneratedCompactPart(part) {
			inputClickHouseParts += originalInputPartCount(part)
		}
		partInputParts, partOutputParts := partInputOutputPartCounts(part, partsByID)
		if key, ok := compactProgressRollupKey(part); ok {
			if _, seen := seenCompactProgress[key]; seen {
				partInputParts = 0
				partOutputParts = 0
			} else {
				seenCompactProgress[key] = struct{}{}
			}
		}
		stateInputParts[part.Status] += partInputParts
		stateOutputParts[part.Status] += partOutputParts
		if isCurrentOutputPartStatus(part.Status) {
			currentOutputClickHouseParts += partOutputParts
		}
		readRows += part.ReadRows
		readBytes += part.ReadBytes
		writtenRows += part.WrittenRows
		writtenBytes += part.WrittenBytes
		failedMerges += part.DestinationFailedMerges
		if part.Status == state.StatusInProgress {
			stage := strings.TrimSpace(part.RewriteStage)
			if stage == "" {
				stage = inProgressStageUnknown
			}
			stageCounts[stage]++
			stageInputParts[stage] += partInputParts
			stageOutputParts[stage] += partOutputParts
		}
		if part.Status == state.StatusFailed {
			failed = append(failed, failedPart{
				PartID:    part.PartID,
				Status:    string(part.Status),
				UpdatedAt: part.UpdatedAt,
				Error:     part.Error,
			})
		}
	}
	sort.Slice(failed, func(i, j int) bool {
		return failed[i].PartID < failed[j].PartID
	})

	total := len(parts)
	rewriteCompleted := counts[state.StatusCompactReady] + counts[state.StatusCompacting] + counts[state.StatusSuperseded] + counts[state.StatusFinished] + counts[state.StatusImporting] + counts[state.StatusImported]
	importCompleted := counts[state.StatusImported]
	return jobSummary{
		JobID:                        jobID,
		Status:                       overallStatus(total, counts),
		Total:                        total,
		Counts:                       counts,
		StatePartStats:               statePartStats(counts, stateInputParts, stateOutputParts),
		InProgressStages:             inProgressStageCounts(stageCounts, stageInputParts, stageOutputParts),
		Compact:                      compactSummary(parts, counts, opts),
		RewriteCompleted:             rewriteCompleted,
		RewritePercent:               percent(rewriteCompleted, total),
		ImportCompleted:              importCompleted,
		ImportPercent:                percent(importCompleted, total),
		InputClickHouseParts:         inputClickHouseParts,
		CurrentOutputClickHouseParts: currentOutputClickHouseParts,
		ReadRows:                     readRows,
		ReadBytes:                    readBytes,
		WrittenRows:                  writtenRows,
		WrittenBytes:                 writtenBytes,
		FailedMerges:                 failedMerges,
		FailedParts:                  failed,
	}
}

func statePartStats(counts map[state.Status]int, inputParts, outputParts map[state.Status]uint64) []statusPartStats {
	stats := make([]statusPartStats, 0, len(statusOrder()))
	for _, status := range statusOrder() {
		stats = append(stats, statusPartStats{
			Status:                status,
			Count:                 counts[status],
			InputClickHouseParts:  inputParts[status],
			OutputClickHouseParts: outputParts[status],
		})
	}
	return stats
}

func inProgressStageCounts(counts map[string]int, inputParts, outputParts map[string]uint64) []inProgressStageCount {
	if len(counts) == 0 {
		return nil
	}

	orderedStages := make([]string, 0, len(counts))
	seen := map[string]struct{}{}
	for _, stage := range rewrite.StageOrder() {
		if _, ok := counts[stage]; !ok {
			continue
		}
		orderedStages = append(orderedStages, stage)
		seen[stage] = struct{}{}
	}

	remaining := make([]string, 0, len(counts)-len(orderedStages))
	for stage := range counts {
		if _, ok := seen[stage]; ok {
			continue
		}
		remaining = append(remaining, stage)
	}
	sort.Strings(remaining)
	orderedStages = append(orderedStages, remaining...)

	stages := make([]inProgressStageCount, 0, len(orderedStages))
	for _, stage := range orderedStages {
		stages = append(stages, inProgressStageCount{
			Stage:                 stage,
			Count:                 counts[stage],
			InputClickHouseParts:  inputParts[stage],
			OutputClickHouseParts: outputParts[stage],
		})
	}
	return stages
}

func compactSummary(parts []state.Part, counts map[state.Status]int, opts jobSummaryOptions) *compactJobSummary {
	readyParts := counts[state.StatusCompactReady]
	compactingParts := counts[state.StatusCompacting]
	if readyParts == 0 && compactingParts == 0 {
		return nil
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	compactWindow := opts.CompactWindow
	if compactWindow < 0 {
		compactWindow = 0
	}
	summary := &compactJobSummary{
		ReadyParts:      readyParts,
		CompactingParts: compactingParts,
		Window:          compactWindow.String(),
	}
	for _, part := range parts {
		if part.Status != state.StatusCompactReady {
			continue
		}
		if until, ok := compactCooldownUntil(part); ok && until.After(now) {
			summary.SoloRetryCooldownParts++
		}
	}
	blockers := compactFinalizationBlockers(counts)
	summary.BlockedBy = blockers
	summary.BlockedByMessage = formatStatusCounts(blockers)
	if readyParts == 0 {
		summary.FinalizeStatus = "waiting_for_compacting"
		summary.Reason = "waiting for compacting work to return compact-ready or finish"
		return summary
	}
	finalizeAfter, ok, reason := compactFinalizeAfter(parts, compactWindow, now)
	if !ok {
		summary.FinalizeStatus = "unknown"
		summary.Reason = reason
		return summary
	}
	summary.FinalizeAfter = finalizeAfter.UTC().Format(time.RFC3339Nano)
	summary.FinalizeIn = formatRemaining(finalizeAfter, now)
	if len(blockers) > 0 {
		summary.FinalizeStatus = "blocked"
		summary.Reason = "active work must finish before compact-ready parts can be finalized"
		return summary
	}
	if now.Before(finalizeAfter) {
		summary.FinalizeStatus = "waiting"
		summary.Reason = "job compact window has not elapsed"
		return summary
	}
	summary.FinalizeStatus = "ready"
	summary.FinalizeIn = "0s"
	summary.Reason = "next idle worker can finalize compact-ready parts"
	return summary
}

func compactFinalizationBlockers(counts map[state.Status]int) []statusCount {
	var blockers []statusCount
	for _, status := range []state.Status{state.StatusReady, state.StatusInProgress, state.StatusCompacting, state.StatusFailed} {
		if counts[status] > 0 {
			blockers = append(blockers, statusCount{Status: status, Count: counts[status]})
		}
	}
	return blockers
}

func compactFinalizeAfter(parts []state.Part, compactWindow time.Duration, now time.Time) (time.Time, bool, string) {
	if compactWindow <= 0 {
		return now, true, ""
	}
	var compactActivityAt time.Time
	for _, part := range parts {
		if !compactWindowAnchorStatus(part.Status) {
			continue
		}
		readyAt, err := compactReadySince(part)
		if err != nil {
			return time.Time{}, false, err.Error()
		}
		if compactActivityAt.IsZero() || readyAt.After(compactActivityAt) {
			compactActivityAt = readyAt
		}
	}
	if compactActivityAt.IsZero() {
		return time.Time{}, false, "no current compact-ready or compacting timestamp found"
	}
	return compactActivityAt.Add(compactWindow), true, ""
}

func compactWindowAnchorStatus(status state.Status) bool {
	switch status {
	case state.StatusCompactReady, state.StatusCompacting:
		return true
	default:
		return false
	}
}

func compactCooldownUntil(part state.Part) (time.Time, bool) {
	if strings.TrimSpace(part.CompactCooldownUntil) == "" {
		return time.Time{}, false
	}
	until, err := time.Parse(time.RFC3339Nano, part.CompactCooldownUntil)
	if err != nil {
		return time.Time{}, false
	}
	return until, true
}

func formatStatusCounts(counts []statusCount) string {
	if len(counts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(counts))
	for _, count := range counts {
		parts = append(parts, fmt.Sprintf("%s=%d", count.Status, count.Count))
	}
	return strings.Join(parts, ", ")
}

func formatRemaining(until, now time.Time) string {
	remaining := until.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	return remaining.Round(time.Second).String()
}

func formatElapsedSince(since, now time.Time) string {
	elapsed := now.Sub(since)
	if elapsed < 0 {
		elapsed = 0
	}
	return elapsed.Round(time.Second).String()
}

func originalInputPartCount(part state.Part) uint64 {
	if part.SourceActivePartCount > 0 {
		return part.SourceActivePartCount
	}
	return 1
}

func partInputOutputPartCounts(part state.Part, partsByID map[string]state.Part) (uint64, uint64) {
	if part.Status == state.StatusSuperseded {
		return 0, 0
	}
	if part.Status == state.StatusCompacting && part.CompactInputPartCount > 0 {
		return part.CompactInputPartCount, part.CompactOutputPartCount
	}
	inputParts := partInputPartCount(part, partsByID)
	outputParts := partOutputPartCount(part)
	return inputParts, outputParts
}

func partInputPartCount(part state.Part, partsByID map[string]state.Part) uint64 {
	if isGeneratedCompactPart(part) {
		var inputParts uint64
		for _, inputID := range part.CompactInputPartIDs {
			input := partsByID[inputID]
			if input.DestinationActivePartCount > 0 {
				inputParts += input.DestinationActivePartCount
				continue
			}
			inputParts++
		}
		if inputParts > 0 {
			return inputParts
		}
	}
	return originalInputPartCount(part)
}

func partOutputPartCount(part state.Part) uint64 {
	if part.Status == state.StatusReady {
		return 0
	}
	return part.DestinationActivePartCount
}

func compactProgressRollupKey(part state.Part) (string, bool) {
	if part.Status != state.StatusCompacting || strings.TrimSpace(part.CompactOutputPartID) == "" || part.CompactInputPartCount == 0 {
		return "", false
	}
	return strings.Join([]string{part.JobID, part.WorkerID, part.CompactOutputPartID}, "\x00"), true
}

func isCurrentOutputPartStatus(status state.Status) bool {
	switch status {
	case state.StatusInProgress, state.StatusCompactReady, state.StatusCompacting, state.StatusFinished, state.StatusImporting, state.StatusImported, state.StatusFailed:
		return true
	default:
		return false
	}
}

func printJobSummary(out *os.File, summary jobSummary) {
	fmt.Fprintf(out, "job_id: %s\n", summary.JobID)
	fmt.Fprintf(out, "status: %s\n", summary.Status)
	fmt.Fprintf(out, "parts: %d\n", summary.Total)
	fmt.Fprintf(out, "rewrite_complete: %d/%d %.1f%%\n", summary.RewriteCompleted, summary.Total, summary.RewritePercent)
	fmt.Fprintf(out, "import_complete: %d/%d %.1f%%\n", summary.ImportCompleted, summary.Total, summary.ImportPercent)
	fmt.Fprintf(out, "input_clickhouse_parts: %d\n", summary.InputClickHouseParts)
	fmt.Fprintf(out, "current_output_clickhouse_parts: %d\n", summary.CurrentOutputClickHouseParts)
	fmt.Fprintf(out, "read: %d rows %s\n", summary.ReadRows, formatBytes(summary.ReadBytes))
	fmt.Fprintf(out, "written: %d rows %s\n", summary.WrittenRows, formatBytes(summary.WrittenBytes))
	fmt.Fprintf(out, "failed_merges: %d\n", summary.FailedMerges)
	if summary.Compact != nil {
		printCompactSummary(out, summary.Compact)
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nSTATE\tCOUNT")
	for _, status := range statusOrder() {
		fmt.Fprintf(tw, "%s\t%d\n", status, summary.Counts[status])
	}
	_ = tw.Flush()

	if len(summary.InProgressStages) > 0 {
		fmt.Fprintln(out, "\nIN_PROGRESS STAGES")
		tw = tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "STAGE\tCOUNT")
		for _, stage := range summary.InProgressStages {
			fmt.Fprintf(tw, "%s\t%d\n", stage.Stage, stage.Count)
		}
		_ = tw.Flush()
	}

	if len(summary.FailedParts) > 0 {
		fmt.Fprintln(out, "\nFAILED PARTS")
		tw = tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "PART_ID\tUPDATED_AT\tERROR")
		for _, failed := range summary.FailedParts {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", failed.PartID, failed.UpdatedAt, failed.Error)
		}
		_ = tw.Flush()
	}
}

func printCompactSummary(out *os.File, compact *compactJobSummary) {
	fmt.Fprintf(
		out,
		"compact: ready=%d compacting=%d solo_retry_cooldown=%d window=%s\n",
		compact.ReadyParts,
		compact.CompactingParts,
		compact.SoloRetryCooldownParts,
		compact.Window,
	)
	switch compact.FinalizeStatus {
	case "blocked":
		fmt.Fprintf(out, "compact_finalize: blocked by %s", compact.BlockedByMessage)
		if compact.FinalizeAfter != "" {
			fmt.Fprintf(out, "; eligible after %s", compact.FinalizeAfter)
			if compact.FinalizeIn != "" {
				fmt.Fprintf(out, " (in %s)", compact.FinalizeIn)
			}
		}
		fmt.Fprintln(out)
	case "waiting":
		fmt.Fprintf(out, "compact_finalize: waiting until %s", compact.FinalizeAfter)
		if compact.FinalizeIn != "" {
			fmt.Fprintf(out, " (in %s)", compact.FinalizeIn)
		}
		fmt.Fprintln(out)
	case "ready":
		fmt.Fprintln(out, "compact_finalize: ready now")
	case "waiting_for_compacting":
		fmt.Fprintln(out, "compact_finalize: waiting for compacting work to finish")
	case "unknown":
		fmt.Fprintf(out, "compact_finalize: unknown")
		if compact.Reason != "" {
			fmt.Fprintf(out, " (%s)", compact.Reason)
		}
		fmt.Fprintln(out)
	}
}

func printPartRows(out *os.File, parts []state.Part) {
	if len(parts) == 0 {
		return
	}
	partsByID := make(map[string]state.Part, len(parts))
	for _, part := range parts {
		partsByID[part.PartID] = part
	}
	now := time.Now().UTC()
	fmt.Fprintln(out, "\nPARTS")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PART_ID\tSTATUS\tATTEMPTS\tWORKER\tREAD_ROWS\tREAD_SIZE\tWRITTEN_ROWS\tWRITTEN_SIZE\tSOURCE_ROWS\tDEST_ROWS\tINPUT_CH_PARTS\tOUTPUT_CH_PARTS\tFAILED_MERGES\tSETTLE_WAIT\tCOMPACT_READY_FOR\tCOMPACT_COOLDOWN\tPROGRESS_AT\tUPDATED_AT\tERROR")
	for _, part := range parts {
		inputParts, outputParts := partInputOutputPartCounts(part, partsByID)
		fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%d\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			part.PartID,
			part.Status,
			part.Attempts,
			part.WorkerID,
			part.ReadRows,
			formatBytes(part.ReadBytes),
			part.WrittenRows,
			formatBytes(part.WrittenBytes),
			part.SourceActivePartRows,
			part.DestinationActivePartRows,
			inputParts,
			outputParts,
			part.DestinationFailedMerges,
			formatSettleWait(part),
			formatCompactReadyFor(part, now),
			formatCompactCooldown(part, now),
			part.ProgressUpdatedAt,
			part.UpdatedAt,
			part.Error,
		)
	}
	_ = tw.Flush()
}

func formatSettleWait(part state.Part) string {
	if durationMs, ok := part.RewriteStageDurationsMs[settleWaitStage]; ok {
		return formatDurationMs(durationMs)
	}
	if part.RewriteStage == settleWaitStage {
		return formatDurationMs(part.RewriteStageElapsedMs)
	}
	return ""
}

func formatCompactReadyFor(part state.Part, now time.Time) string {
	if part.Status != state.StatusCompactReady {
		return ""
	}
	readyAt, err := compactReadySince(part)
	if err != nil {
		return "unknown"
	}
	return formatElapsedSince(readyAt, now)
}

func formatCompactCooldown(part state.Part, now time.Time) string {
	if part.Status != state.StatusCompactReady {
		return ""
	}
	until, ok := compactCooldownUntil(part)
	if !ok || !until.After(now) {
		return ""
	}
	return formatRemaining(until, now)
}

func printPartDetails(out *os.File, parts []state.Part) {
	if len(parts) == 0 {
		return
	}
	fmt.Fprintln(out, "\nPART DETAILS")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PART_ID\tSTATUS\tATTEMPTS\tWORKER\tSTAGE\tSTAGE_ELAPSED\tTOTAL_ELAPSED\tSTAGE_STARTED\tPROGRESS_AT\tUPDATED_AT\tSTAGE_DURATIONS\tERROR")
	for _, part := range parts {
		stageElapsed := ""
		totalElapsed := ""
		if part.RewriteStage != "" {
			stageElapsed = formatDurationMs(part.RewriteStageElapsedMs)
			totalElapsed = formatDurationMs(part.RewriteTotalElapsedMs)
		}
		fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			part.PartID,
			part.Status,
			part.Attempts,
			part.WorkerID,
			part.RewriteStage,
			stageElapsed,
			totalElapsed,
			part.RewriteStageStartedAt,
			part.ProgressUpdatedAt,
			part.UpdatedAt,
			formatStageDurations(part.RewriteStageDurationsMs),
			part.Error,
		)
	}
	_ = tw.Flush()
}

func formatStageDurations(durations map[string]int64) string {
	if len(durations) == 0 {
		return ""
	}
	stages := make([]string, 0, len(durations))
	for stage := range durations {
		stages = append(stages, stage)
	}
	sort.Strings(stages)
	parts := make([]string, 0, len(stages))
	for _, stage := range stages {
		parts = append(parts, stage+"="+formatDurationMs(durations[stage]))
	}
	return strings.Join(parts, ",")
}

func formatDurationMs(ms int64) string {
	if ms < 0 {
		ms = 0
	}
	return (time.Duration(ms) * time.Millisecond).String()
}

type retryPartSelection struct {
	All               bool
	Force             bool
	IncludeInProgress bool
	Stale             bool
	StaleAfter        time.Duration
	Now               time.Time
	PartID            string
}

type deletePartSelection struct {
	All     bool
	PartIDs []string
	Status  state.Status
}

type setPartStateSelection struct {
	PartIDs []string
	Status  state.Status
}

type partIDListFlag []string

func (f *partIDListFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *partIDListFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("part-id must not be empty")
	}
	*f = append(*f, value)
	return nil
}

func selectDeleteParts(parts []state.Part, selection deletePartSelection) ([]state.Part, error) {
	if selection.All {
		return append([]state.Part(nil), parts...), nil
	}
	if len(selection.PartIDs) > 0 {
		return selectDeletePartsByID(parts, selection.PartIDs)
	}
	if selection.Status != "" {
		if !knownStatus(selection.Status) {
			return nil, fmt.Errorf("unknown status %q", selection.Status)
		}
		selected := make([]state.Part, 0)
		for _, part := range parts {
			if part.Status == selection.Status {
				selected = append(selected, part)
			}
		}
		return selected, nil
	}
	return nil, errors.New("delete part selection is empty")
}

func selectSetPartStateParts(parts []state.Part, selection setPartStateSelection) ([]state.Part, error) {
	if len(selection.PartIDs) > 0 {
		return selectDeletePartsByID(parts, selection.PartIDs)
	}
	if selection.Status != "" {
		if !knownStatus(selection.Status) {
			return nil, fmt.Errorf("unknown status %q", selection.Status)
		}
		selected := make([]state.Part, 0)
		for _, part := range parts {
			if part.Status == selection.Status {
				selected = append(selected, part)
			}
		}
		return selected, nil
	}
	return nil, errors.New("set-part-state selection is empty")
}

func selectDeletePartsByID(parts []state.Part, partIDs []string) ([]state.Part, error) {
	byID := make(map[string]state.Part, len(parts))
	for _, part := range parts {
		byID[part.PartID] = part
	}
	selected := make([]state.Part, 0, len(partIDs))
	seen := map[string]struct{}{}
	for _, partID := range partIDs {
		partID = strings.TrimSpace(partID)
		if partID == "" {
			return nil, errors.New("part-id must not be empty")
		}
		if _, ok := seen[partID]; ok {
			continue
		}
		part, ok := byID[partID]
		if !ok {
			return nil, fmt.Errorf("part %s was not found in job", partID)
		}
		seen[partID] = struct{}{}
		selected = append(selected, part)
	}
	return selected, nil
}

func knownStatus(status state.Status) bool {
	for _, known := range statusOrder() {
		if status == known {
			return true
		}
	}
	return false
}

func adminSettableStatus(status state.Status) bool {
	switch status {
	case state.StatusReady, state.StatusCompactReady, state.StatusFinished:
		return true
	default:
		return false
	}
}

func selectRetryParts(parts []state.Part, selection retryPartSelection) ([]state.Part, error) {
	if selection.Stale {
		return selectStaleRetryParts(parts, selection.Now, selection.StaleAfter)
	}
	if selection.All {
		if selection.Force {
			return append([]state.Part(nil), parts...), nil
		}
		var selected []state.Part
		for _, part := range parts {
			if part.Status == state.StatusFailed || (selection.IncludeInProgress && part.Status == state.StatusInProgress) {
				selected = append(selected, part)
			}
		}
		return selected, nil
	}
	for _, part := range parts {
		if part.PartID == selection.PartID {
			if selection.Force {
				return []state.Part{part}, nil
			}
			if part.Status == state.StatusFailed || (selection.IncludeInProgress && part.Status == state.StatusInProgress) {
				return []state.Part{part}, nil
			}
			if selection.IncludeInProgress {
				return nil, fmt.Errorf("part %s is %s, expected %s or %s", selection.PartID, part.Status, state.StatusFailed, state.StatusInProgress)
			}
			return nil, fmt.Errorf("part %s is %s, expected %s", selection.PartID, part.Status, state.StatusFailed)
		}
	}
	return nil, fmt.Errorf("part %s was not found in job", selection.PartID)
}

func selectStaleRetryParts(parts []state.Part, now time.Time, staleAfter time.Duration) ([]state.Part, error) {
	if now.IsZero() {
		return nil, errors.New("current time is required for stale retry selection")
	}
	if staleAfter <= 0 {
		return nil, errors.New("stale-after must be greater than zero")
	}
	cutoff := now.Add(-staleAfter)
	selected := make([]state.Part, 0)
	for _, part := range parts {
		if part.Status != state.StatusInProgress || strings.TrimSpace(part.ProgressUpdatedAt) == "" {
			continue
		}
		progressAt, err := time.Parse(time.RFC3339Nano, part.ProgressUpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse progress_updated_at for part %s: %w", part.PartID, err)
		}
		if progressAt.Before(cutoff) {
			selected = append(selected, part)
		}
	}
	return selected, nil
}

func staleAfterString(stale bool, staleAfter time.Duration) string {
	if !stale {
		return ""
	}
	return staleAfter.String()
}

func buildResetPlan(jobID string, parts []state.Part, mode resetMode) (resetPlan, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return resetPlan{}, errors.New("job id is required")
	}
	if len(parts) == 0 {
		return resetPlan{}, fmt.Errorf("job %s has no state rows", jobID)
	}

	byID := make(map[string]state.Part, len(parts))
	var originals []state.Part
	var generated []state.Part
	for _, part := range parts {
		if part.JobID != jobID {
			return resetPlan{}, fmt.Errorf("part %s belongs to job %q, expected %q", part.PartID, part.JobID, jobID)
		}
		if _, ok := byID[part.PartID]; ok {
			return resetPlan{}, fmt.Errorf("job %s contains duplicate part id %s", jobID, part.PartID)
		}
		if part.Status == state.StatusImporting || part.Status == state.StatusImported || strings.TrimSpace(part.ImportingAt) != "" || strings.TrimSpace(part.ImportedAt) != "" {
			return resetPlan{}, fmt.Errorf("part %s is %s; reset is unsafe after import has started", part.PartID, part.Status)
		}
		byID[part.PartID] = part
		if isGeneratedCompactPart(part) {
			if len(part.CompactInputPartIDs) == 0 {
				return resetPlan{}, fmt.Errorf("generated compact part %s has no compact_input_part_ids", part.PartID)
			}
			generated = append(generated, part)
		} else {
			originals = append(originals, part)
		}
	}
	if len(originals) == 0 {
		return resetPlan{}, fmt.Errorf("job %s has no original source parts to reset", jobID)
	}
	if err := validateResetLineage(byID, generated); err != nil {
		return resetPlan{}, err
	}
	target := state.StatusReady
	if mode == resetModeCompaction {
		target = state.StatusCompactReady
		for _, part := range originals {
			if err := validateResetCompactionOriginal(part); err != nil {
				return resetPlan{}, err
			}
		}
	} else if mode != resetModeJob {
		return resetPlan{}, fmt.Errorf("unknown reset mode %q", mode)
	}

	sort.Slice(originals, func(i, j int) bool {
		return originals[i].PartID < originals[j].PartID
	})
	sort.Slice(generated, func(i, j int) bool {
		return generated[i].PartID < generated[j].PartID
	})
	return resetPlan{
		JobID:          jobID,
		Mode:           mode,
		TargetStatus:   target,
		OriginalParts:  originals,
		GeneratedParts: generated,
	}, nil
}

func isGeneratedCompactPart(part state.Part) bool {
	return len(part.CompactInputPartIDs) > 0 || part.CompactGeneration > 0
}

func validateResetLineage(byID map[string]state.Part, generated []state.Part) error {
	for _, output := range generated {
		seenInputs := map[string]struct{}{}
		for _, inputID := range output.CompactInputPartIDs {
			inputID = strings.TrimSpace(inputID)
			if inputID == "" {
				return fmt.Errorf("generated compact part %s has an empty compact input id", output.PartID)
			}
			if inputID == output.PartID {
				return fmt.Errorf("generated compact part %s lists itself as an input", output.PartID)
			}
			if _, ok := seenInputs[inputID]; ok {
				return fmt.Errorf("generated compact part %s lists input %s more than once", output.PartID, inputID)
			}
			input, ok := byID[inputID]
			if !ok {
				return fmt.Errorf("generated compact part %s references missing input part %s", output.PartID, inputID)
			}
			if input.SupersededBy != "" && input.SupersededBy != output.PartID {
				return fmt.Errorf("part %s is superseded by %s but is listed as input to %s", input.PartID, input.SupersededBy, output.PartID)
			}
			seenInputs[inputID] = struct{}{}
		}
	}

	for _, part := range byID {
		if strings.TrimSpace(part.SupersededBy) == "" {
			continue
		}
		output, ok := byID[part.SupersededBy]
		if !ok {
			continue
		}
		if !isGeneratedCompactPart(output) {
			return fmt.Errorf("part %s is superseded by non-compact part %s", part.PartID, part.SupersededBy)
		}
		if !containsPartID(output.CompactInputPartIDs, part.PartID) {
			return fmt.Errorf("part %s is superseded by %s but is not listed as its compact input", part.PartID, part.SupersededBy)
		}
	}

	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(partID string) error {
		if visiting[partID] {
			return fmt.Errorf("compact lineage contains a cycle at part %s", partID)
		}
		if visited[partID] {
			return nil
		}
		part := byID[partID]
		visiting[partID] = true
		if isGeneratedCompactPart(part) {
			for _, inputID := range part.CompactInputPartIDs {
				if err := visit(inputID); err != nil {
					return err
				}
			}
		}
		visiting[partID] = false
		visited[partID] = true
		return nil
	}
	for _, output := range generated {
		if err := visit(output.PartID); err != nil {
			return err
		}
	}
	return nil
}

func containsPartID(partIDs []string, partID string) bool {
	for _, candidate := range partIDs {
		if candidate == partID {
			return true
		}
	}
	return false
}

func validateResetCompactionOriginal(part state.Part) error {
	if strings.TrimSpace(part.FinishedKey) == "" {
		return fmt.Errorf("original part %s has no finished_key for reset-compaction", part.PartID)
	}
	if strings.TrimSpace(part.DestinationDatabase) == "" || strings.TrimSpace(part.DestinationTable) == "" || strings.TrimSpace(part.DestinationSchema) == "" {
		return fmt.Errorf("original part %s has not completed rewrite metadata required for reset-compaction", part.PartID)
	}
	if part.DestinationActivePartCount > 0 && len(part.DestinationActivePartitionCounts) == 0 {
		return fmt.Errorf("original part %s has no destination partition counts required for reset-compaction", part.PartID)
	}
	return nil
}

func resetS3Prefixes(plan resetPlan) []jobS3Prefix {
	seen := map[jobS3Prefix]struct{}{}
	add := func(part state.Part) {
		if strings.TrimSpace(part.Bucket) == "" || strings.TrimSpace(part.FinishedKey) == "" {
			return
		}
		seen[jobS3Prefix{Bucket: part.Bucket, Prefix: part.FinishedKey}] = struct{}{}
	}
	for _, part := range plan.GeneratedParts {
		add(part)
	}
	if plan.Mode == resetModeJob {
		for _, part := range plan.OriginalParts {
			add(part)
		}
	}

	prefixes := make([]jobS3Prefix, 0, len(seen))
	for prefix := range seen {
		prefixes = append(prefixes, prefix)
	}
	sort.Slice(prefixes, func(i, j int) bool {
		if prefixes[i].Bucket == prefixes[j].Bucket {
			return prefixes[i].Prefix < prefixes[j].Prefix
		}
		return prefixes[i].Bucket < prefixes[j].Bucket
	})
	return prefixes
}

func resetStatePartResults(plan resetPlan) []resetPartResult {
	results := make([]resetPartResult, 0, len(plan.GeneratedParts)+len(plan.OriginalParts))
	for _, part := range plan.GeneratedParts {
		results = append(results, resetPartResult{
			PartID: part.PartID,
			From:   string(part.Status),
			Action: "delete_compact_row",
		})
	}
	for _, part := range plan.OriginalParts {
		results = append(results, resetPartResult{
			PartID: part.PartID,
			From:   string(part.Status),
			To:     string(plan.TargetStatus),
			Action: "reset_original",
		})
	}
	return results
}

func jobS3Prefixes(jobID string, parts []state.Part) ([]jobS3Prefix, error) {
	if strings.TrimSpace(jobID) == "" {
		return nil, errors.New("job id is required")
	}
	if strings.Contains(jobID, "/") || strings.ContainsAny(jobID, "*?[]{}") {
		return nil, fmt.Errorf("job id %q is not safe for S3 prefix deletion", jobID)
	}

	seen := map[jobS3Prefix]struct{}{}
	for _, part := range parts {
		if part.JobID != jobID {
			return nil, fmt.Errorf("part %s belongs to job %q, expected %q", part.PartID, part.JobID, jobID)
		}
		for _, key := range []string{part.SourceKey, part.FinishedKey} {
			prefix, err := jobPrefixFromKey(jobID, key)
			if err != nil {
				return nil, err
			}
			seen[jobS3Prefix{Bucket: part.Bucket, Prefix: prefix}] = struct{}{}
		}
	}

	prefixes := make([]jobS3Prefix, 0, len(seen))
	for prefix := range seen {
		prefixes = append(prefixes, prefix)
	}
	sort.Slice(prefixes, func(i, j int) bool {
		if prefixes[i].Bucket == prefixes[j].Bucket {
			return prefixes[i].Prefix < prefixes[j].Prefix
		}
		return prefixes[i].Bucket < prefixes[j].Bucket
	})
	return prefixes, nil
}

func jobPrefixFromKey(jobID, key string) (string, error) {
	cleanKey := strings.Trim(key, "/")
	if cleanKey == "" {
		return "", errors.New("S3 key is required")
	}
	segments := strings.Split(cleanKey, "/")
	for i := 0; i+1 < len(segments); i++ {
		if segments[i] == "jobs" && segments[i+1] == jobID {
			if i+2 >= len(segments) {
				return "", fmt.Errorf("S3 key %q does not include data below job %q", key, jobID)
			}
			return strings.Join(segments[:i+2], "/"), nil
		}
	}
	return "", fmt.Errorf("S3 key %q does not contain job segment %q", key, jobID)
}

func printRetryResults(out *os.File, result retryFailedOutput) {
	fmt.Fprintf(out, "job_id: %s\n", result.JobID)
	fmt.Fprintf(out, "forced: %t\n", result.Forced)
	if result.Stale {
		fmt.Fprintf(out, "stale_after: %s\n", result.StaleAfter)
	}
	fmt.Fprintf(out, "retried: %d\n", result.Retried)
	if len(result.Parts) == 0 {
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nPART_ID\tFROM\tTO")
	for _, part := range result.Parts {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", part.PartID, part.From, part.To)
	}
	_ = tw.Flush()
}

func printSetPartStateResult(out *os.File, result setPartStateOutput) {
	fmt.Fprintf(out, "job_id: %s\n", result.JobID)
	fmt.Fprintf(out, "to_status: %s\n", result.ToStatus)
	fmt.Fprintf(out, "updated: %d\n", result.Updated)
	if len(result.Parts) == 0 {
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nPART_ID\tFROM\tTO")
	for _, part := range result.Parts {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", part.PartID, part.From, part.To)
	}
	_ = tw.Flush()
}

func printResetCompactTimerResult(out *os.File, result resetCompactTimerOutput) {
	fmt.Fprintf(out, "job_id: %s\n", result.JobID)
	fmt.Fprintf(out, "reset_at: %s\n", result.ResetAt)
	fmt.Fprintf(out, "updated: %d\n", result.Updated)
}

func printResetStateResult(out *os.File, result resetStateOutput) {
	fmt.Fprintf(out, "job_id: %s\n", result.JobID)
	fmt.Fprintf(out, "mode: %s\n", result.Mode)
	fmt.Fprintf(out, "target_status: %s\n", result.TargetStatus)
	fmt.Fprintf(out, "originals_reset: %d\n", result.OriginalsReset)
	fmt.Fprintf(out, "compact_rows_deleted: %d\n", result.CompactRowsDeleted)
	fmt.Fprintf(out, "delete_s3: %t\n", result.DeleteS3)
	if len(result.Parts) > 0 {
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "\nPART_ID\tACTION\tFROM\tTO")
		for _, part := range result.Parts {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", part.PartID, part.Action, part.From, part.To)
		}
		_ = tw.Flush()
	}
	if len(result.S3PrefixesDeleted) == 0 {
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nS3_PREFIXES_DELETED")
	fmt.Fprintln(tw, "BUCKET\tPREFIX")
	for _, prefix := range result.S3PrefixesDeleted {
		fmt.Fprintf(tw, "%s\t%s\n", prefix.Bucket, prefix.Prefix)
	}
	_ = tw.Flush()
}

func printDeleteJobResult(out *os.File, result deleteJobOutput) {
	fmt.Fprintf(out, "job_id: %s\n", result.JobID)
	fmt.Fprintf(out, "state_parts_deleted: %d\n", result.StatePartsDeleted)
	fmt.Fprintf(out, "delete_s3: %t\n", result.DeleteS3)
	if len(result.S3PrefixesDeleted) == 0 {
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nS3_PREFIXES_DELETED")
	fmt.Fprintln(tw, "BUCKET\tPREFIX")
	for _, prefix := range result.S3PrefixesDeleted {
		fmt.Fprintf(tw, "%s\t%s\n", prefix.Bucket, prefix.Prefix)
	}
	_ = tw.Flush()
}

func printDeletePartsResult(out *os.File, result deletePartsOutput) {
	fmt.Fprintf(out, "job_id: %s\n", result.JobID)
	fmt.Fprintf(out, "deleted: %d\n", result.Deleted)
	if len(result.Parts) == 0 {
		return
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nPART_ID\tSTATUS")
	for _, part := range result.Parts {
		fmt.Fprintf(tw, "%s\t%s\n", part.PartID, part.Status)
	}
	_ = tw.Flush()
}

func writeJSON(out *os.File, value any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func statusOrder() []state.Status {
	return []state.Status{
		state.StatusReady,
		state.StatusInProgress,
		state.StatusCompactReady,
		state.StatusCompacting,
		state.StatusSuperseded,
		state.StatusFinished,
		state.StatusImporting,
		state.StatusImported,
		state.StatusFailed,
	}
}

func overallStatus(total int, counts map[state.Status]int) string {
	switch {
	case total == 0:
		return "EMPTY"
	case counts[state.StatusFailed] > 0:
		return "FAILED"
	case counts[state.StatusImported]+counts[state.StatusSuperseded] == total:
		return "IMPORTED"
	case counts[state.StatusImporting] > 0:
		return "IMPORTING"
	case counts[state.StatusFinished]+counts[state.StatusImporting]+counts[state.StatusImported]+counts[state.StatusSuperseded] == total:
		return "READY_FOR_IMPORT"
	case counts[state.StatusCompacting] > 0 || counts[state.StatusCompactReady] > 0:
		return "COMPACTING"
	case counts[state.StatusInProgress] > 0:
		return "REWRITING"
	default:
		return "READY"
	}
}

func percent(done, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(done) * 100 / float64(total)
}
