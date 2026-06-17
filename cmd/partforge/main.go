package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"log/slog"
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

var version = "dev"

func main() {
	configureLogger()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func configureLogger() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
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
  partforge delete-job      [flags]

Commands:
  upload-freeze     Upload frozen source part directories to S3 and register DynamoDB work.
  worker            Claim DynamoDB work, rewrite source parts with local ClickHouse, and upload finished artifacts.
  import-finished   Attach finished artifacts into the final ClickHouse table with safe part renames.
  list-jobs         List job IDs found in the DynamoDB state table.
  job-status        Show part state counts, progress, and failed part errors for one job.
  retry-failed      Move failed parts back to their retryable state.
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
	if err := fs.Parse(args); err != nil {
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
		configPath            = fs.String("config", defaultConfigPath, "JSON config file path")
		region                = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint            = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary           = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		stateTable            = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		dynamoEndpoint        = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		clickHouseURL         = fs.String("clickhouse-url", defaultClickHouseURL, "local ClickHouse HTTP URL")
		clickHouseUser        = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword    = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		startClickHouse       = fs.Bool("start-clickhouse", true, "start clickhouse-server as a child process")
		clickHouseBinary      = fs.String("clickhouse-binary", "clickhouse", "clickhouse binary path")
		clickHouseConfigFile  = fs.String("clickhouse-config-file", "/etc/clickhouse-server/config.xml", "clickhouse-server config file")
		once                  = fs.Bool("once", false, "process one part and exit")
		pollInterval          = fs.Duration("poll-interval", 10*time.Second, "how long to wait before checking for ready work again")
		workerID              = fs.String("worker-id", "", "worker identity recorded on claimed parts")
		workDir               = fs.String("work-dir", "/tmp/partforge", "worker scratch directory")
		mergeTimeout          = fs.Duration("merge-timeout", 10*time.Minute, "maximum time to wait for destination merges")
		metricsAddr           = fs.String("metrics-addr", ":2112", "Prometheus metrics listen address; empty disables metrics")
		metricsPath           = fs.String("metrics-path", "/metrics", "Prometheus metrics HTTP path")
		stateProgressInterval = fs.Duration("state-progress-interval", 15*time.Second, "how often to write live per-part progress to DynamoDB; <=0 disables progress writes")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "worker"); err != nil {
		return err
	}
	if err := applyClickHouseClientConfigDefaults(clickHouseUser, clickHousePassword); err != nil {
		return err
	}
	slog.Info(
		"worker started",
		"stage", "start",
		"once", *once,
		"state_table", *stateTable,
		"work_dir", *workDir,
		"start_clickhouse", *startClickHouse,
		"clickhouse_url", *clickHouseURL,
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
	slog.Info(
		"configured clickhouse insert-select settings",
		"cpus", workerLimits.CPUs,
		"memory_bytes", workerLimits.MemoryBytes,
		"max_threads", insertSettings["max_threads"],
		"max_insert_threads", insertSettings["max_insert_threads"],
		"max_memory_usage", insertSettings["max_memory_usage"],
	)

	var server *chproc.Server
	if *startClickHouse {
		clickHouseDataDir, err := workerClickHouseDataDir(*workDir)
		if err != nil {
			return err
		}
		slog.Info("starting local ClickHouse server", "stage", "start_clickhouse", "binary", *clickHouseBinary, "config_file", *clickHouseConfigFile, "clickhouse_data_dir", clickHouseDataDir)
		server, err = chproc.Start(ctx, chproc.Config{
			Binary:     *clickHouseBinary,
			ConfigFile: *clickHouseConfigFile,
			DataDir:    clickHouseDataDir,
			URL:        *clickHouseURL,
			User:       *clickHouseUser,
			Password:   *clickHousePassword,
			Timeout:    90 * time.Second,
		})
		if err != nil {
			return err
		}
		defer server.Stop()
	}

	var recorder metrics.Recorder = metrics.Noop{}
	if *metricsAddr != "" {
		slog.Info("starting metrics server", "stage", "start_metrics", "addr", *metricsAddr, "path", *metricsPath)
		prom := metrics.NewPrometheus()
		if _, err := metrics.StartServer(ctx, *metricsAddr, *metricsPath, prom.Handler()); err != nil {
			return fmt.Errorf("start metrics server: %w", err)
		}
		recorder = prom
	}

	ch := chhttp.Client{URL: *clickHouseURL, User: *clickHouseUser, Password: *clickHousePassword}
	processor := rewrite.Processor{
		S3Copy:           s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint},
		ClickHouse:       ch,
		WorkDir:          *workDir,
		MergeTimeout:     *mergeTimeout,
		Metrics:          recorder,
		InsertSettings:   insertSettings,
		ProgressInterval: *stateProgressInterval,
	}
	if *stateProgressInterval > 0 {
		processor.ReportProgress = func(ctx context.Context, m manifest.Manifest, snapshot rewrite.ProgressSnapshot) error {
			return stateStore.UpdateRewriteProgress(ctx, m.JobID, m.PartID, resolvedWorkerID, stateProgress(snapshot), time.Now().UTC())
		}
	}

	for {
		slog.Info("claiming next ready part", "stage", "claim_work", "worker_id", resolvedWorkerID)
		part, err := stateStore.ClaimNextReady(ctx, resolvedWorkerID, time.Now().UTC())
		if err != nil {
			return err
		}
		if part == nil {
			if *once {
				slog.Info("no ready part available")
				return nil
			}
			slog.Info("no ready part available; sleeping", "stage", "claim_work", "poll_interval", *pollInterval)
			if err := sleepOrDone(ctx, *pollInterval); err != nil {
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
		result, err := processor.ProcessPart(ctx, workItem)
		if err != nil {
			slog.Info("part processing failed; marking failed", "stage", "mark_failed", "job_id", part.JobID, "part_id", part.PartID, "error", err)
			if markErr := stateStore.MarkFailed(ctx, *part, resolvedWorkerID, err, time.Now().UTC()); markErr != nil {
				return fmt.Errorf("process part %s/%s: %w; additionally failed to mark failed: %v", part.JobID, part.PartID, err, markErr)
			}
			return err
		}
		slog.Info("marking part finished", "stage", "mark_finished", "job_id", part.JobID, "part_id", part.PartID, "finished_key", result.FinishedKey)
		if err := stateStore.MarkFinished(ctx, *part, resolvedWorkerID, result.FinishedKey, time.Now().UTC()); err != nil {
			return err
		}
		slog.Info("part marked finished", "stage", "mark_finished", "job_id", part.JobID, "part_id", part.PartID, "finished_key", result.FinishedKey)
		if *once {
			return nil
		}
	}
}

func workerClickHouseDataDir(workDir string) (string, error) {
	root := strings.TrimSpace(workDir)
	if root == "" {
		root = "/tmp/partforge"
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve worker work-dir %s: %w", workDir, err)
	}
	return filepath.Join(abs, "clickhouse"), nil
}

func runImportFinished(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import-finished", flag.ExitOnError)
	var (
		configPath         = fs.String("config", defaultConfigPath, "JSON config file path")
		database           = fs.String("database", "", "final destination database")
		table              = fs.String("table", "", "final destination table")
		jobID              = fs.String("job-id", "", "job id to import")
		stateTable         = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region             = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		s3Endpoint         = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary        = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
		dynamoEndpoint     = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		clickHouseURL      = fs.String("clickhouse-url", defaultClickHouseURL, "destination ClickHouse HTTP URL")
		clickHouseUser     = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		workDir            = fs.String("work-dir", "/tmp/partforge-import", "import scratch directory")
		requireEmpty       = fs.Bool("require-empty", true, "fail if the destination table already has active parts")
	)
	if err := fs.Parse(args); err != nil {
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
	if err := fs.Parse(args); err != nil {
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
		jsonOutput     = fs.Bool("json", false, "print JSON output")
		showParts      = fs.Bool("parts", false, "include per-part state rows")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "job-status"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
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
	summary := summarizeJob(*jobID, jobParts)
	if *jsonOutput {
		out := jobStatusOutput{Summary: summary}
		if *showParts {
			out.Parts = jobParts
		}
		return writeJSON(os.Stdout, out)
	}
	printJobSummary(os.Stdout, summary)
	if *showParts {
		printPartRows(os.Stdout, jobParts)
	}
	return nil
}

func runRetryFailed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("retry-failed", flag.ExitOnError)
	var (
		configPath     = fs.String("config", defaultConfigPath, "JSON config file path")
		jobID          = fs.String("job-id", "", "job id containing failed parts")
		partID         = fs.String("part-id", "", "specific failed part id to retry")
		all            = fs.Bool("all", false, "retry all failed parts in the job")
		force          = fs.Bool("force", false, "with -all, retry every part in the job, including parts that succeeded")
		stateTable     = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region         = fs.String("aws-region", "", "AWS region for DynamoDB; empty resolves from AWS config, IMDS, then us-east-1")
		dynamoEndpoint = fs.String("dynamodb-endpoint", "", "optional DynamoDB endpoint, e.g. LocalStack")
		jsonOutput     = fs.Bool("json", false, "print JSON output")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := applyConfigDefaults(fs, *configPath, "retry-failed"); err != nil {
		return err
	}
	if *jobID == "" {
		return errors.New("job-id is required")
	}
	if (*all && *partID != "") || (!*all && *partID == "") {
		return errors.New("exactly one of -all or -part-id is required")
	}
	if *force && !*all {
		return errors.New("force requires -all")
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
	retryParts, err := selectRetryParts(jobParts, *all, *force, *partID)
	if err != nil {
		return err
	}

	var results []retryResult
	for _, part := range retryParts {
		var target state.Status
		if *force {
			target, err = stateStore.ForceRetryPart(ctx, part, time.Now().UTC())
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
		JobID:   *jobID,
		Forced:  *force,
		Retried: len(results),
		Parts:   results,
	}
	if *jsonOutput {
		return writeJSON(os.Stdout, out)
	}
	printRetryResults(os.Stdout, out)
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
	if err := fs.Parse(args); err != nil {
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
	return progress
}

type jobSummary struct {
	JobID            string               `json:"job_id"`
	Status           string               `json:"status"`
	Total            int                  `json:"total"`
	Counts           map[state.Status]int `json:"counts"`
	RewriteCompleted int                  `json:"rewrite_completed"`
	RewritePercent   float64              `json:"rewrite_percent"`
	ImportCompleted  int                  `json:"import_completed"`
	ImportPercent    float64              `json:"import_percent"`
	ReadRows         uint64               `json:"read_rows"`
	ReadBytes        uint64               `json:"read_bytes"`
	WrittenRows      uint64               `json:"written_rows"`
	WrittenBytes     uint64               `json:"written_bytes"`
	FailedParts      []failedPart         `json:"failed_parts,omitempty"`
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
	JobID   string        `json:"job_id"`
	Forced  bool          `json:"forced"`
	Retried int           `json:"retried"`
	Parts   []retryResult `json:"parts"`
}

type retryResult struct {
	PartID string `json:"part_id"`
	From   string `json:"from"`
	To     string `json:"to"`
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
	counts := make(map[state.Status]int, len(statusOrder()))
	for _, status := range statusOrder() {
		counts[status] = 0
	}

	var failed []failedPart
	var readRows, readBytes, writtenRows, writtenBytes uint64
	for _, part := range parts {
		counts[part.Status]++
		readRows += part.ReadRows
		readBytes += part.ReadBytes
		writtenRows += part.WrittenRows
		writtenBytes += part.WrittenBytes
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
	rewriteCompleted := counts[state.StatusFinished] + counts[state.StatusImporting] + counts[state.StatusImported]
	importCompleted := counts[state.StatusImported]
	return jobSummary{
		JobID:            jobID,
		Status:           overallStatus(total, counts),
		Total:            total,
		Counts:           counts,
		RewriteCompleted: rewriteCompleted,
		RewritePercent:   percent(rewriteCompleted, total),
		ImportCompleted:  importCompleted,
		ImportPercent:    percent(importCompleted, total),
		ReadRows:         readRows,
		ReadBytes:        readBytes,
		WrittenRows:      writtenRows,
		WrittenBytes:     writtenBytes,
		FailedParts:      failed,
	}
}

func printJobSummary(out *os.File, summary jobSummary) {
	fmt.Fprintf(out, "job_id: %s\n", summary.JobID)
	fmt.Fprintf(out, "status: %s\n", summary.Status)
	fmt.Fprintf(out, "parts: %d\n", summary.Total)
	fmt.Fprintf(out, "rewrite_complete: %d/%d %.1f%%\n", summary.RewriteCompleted, summary.Total, summary.RewritePercent)
	fmt.Fprintf(out, "import_complete: %d/%d %.1f%%\n", summary.ImportCompleted, summary.Total, summary.ImportPercent)
	fmt.Fprintf(out, "read: %d rows %d bytes\n", summary.ReadRows, summary.ReadBytes)
	fmt.Fprintf(out, "written: %d rows %d bytes\n", summary.WrittenRows, summary.WrittenBytes)

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "\nSTATE\tCOUNT")
	for _, status := range statusOrder() {
		fmt.Fprintf(tw, "%s\t%d\n", status, summary.Counts[status])
	}
	_ = tw.Flush()

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

func printPartRows(out *os.File, parts []state.Part) {
	if len(parts) == 0 {
		return
	}
	fmt.Fprintln(out, "\nPARTS")
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PART_ID\tSTATUS\tATTEMPTS\tWORKER\tREAD_ROWS\tREAD_BYTES\tWRITTEN_ROWS\tWRITTEN_BYTES\tSOURCE_ROWS\tDEST_ROWS\tPROGRESS_AT\tUPDATED_AT\tERROR")
	for _, part := range parts {
		fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\n",
			part.PartID,
			part.Status,
			part.Attempts,
			part.WorkerID,
			part.ReadRows,
			part.ReadBytes,
			part.WrittenRows,
			part.WrittenBytes,
			part.SourceActivePartRows,
			part.DestinationActivePartRows,
			part.ProgressUpdatedAt,
			part.UpdatedAt,
			part.Error,
		)
	}
	_ = tw.Flush()
}

func selectRetryParts(parts []state.Part, all, force bool, partID string) ([]state.Part, error) {
	if all {
		if force {
			return append([]state.Part(nil), parts...), nil
		}
		var failed []state.Part
		for _, part := range parts {
			if part.Status == state.StatusFailed {
				failed = append(failed, part)
			}
		}
		return failed, nil
	}
	for _, part := range parts {
		if part.PartID == partID {
			if part.Status != state.StatusFailed {
				return nil, fmt.Errorf("part %s is %s, expected %s", partID, part.Status, state.StatusFailed)
			}
			return []state.Part{part}, nil
		}
	}
	return nil, fmt.Errorf("part %s was not found in job", partID)
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

func writeJSON(out *os.File, value any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func statusOrder() []state.Status {
	return []state.Status{
		state.StatusReady,
		state.StatusInProgress,
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
	case counts[state.StatusImported] == total:
		return "IMPORTED"
	case counts[state.StatusImporting] > 0:
		return "IMPORTING"
	case counts[state.StatusFinished]+counts[state.StatusImporting]+counts[state.StatusImported] == total:
		return "READY_FOR_IMPORT"
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
