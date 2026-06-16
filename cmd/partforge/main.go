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
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/partforge/partforge/internal/artifact"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/chproc"
	"github.com/partforge/partforge/internal/ddl"
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
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
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

Commands:
  upload-freeze     Upload frozen source part directories to S3 and register DynamoDB work.
  worker            Claim DynamoDB work, rewrite source parts with local ClickHouse, and upload finished artifacts.
  import-finished   Attach finished artifacts into the final ClickHouse table with safe part renames.
  list-jobs         List job IDs found in the DynamoDB state table.
  job-status        Show part state counts, progress, and failed part errors for one job.
  retry-failed      Move failed parts back to their retryable state.
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
		destinationDatabase   = fs.String("destination-database", "", "destination database referenced by the insert-select")
		destinationTable      = fs.String("destination-table", "", "destination table referenced by the insert-select")
		destinationSchemaFile = fs.String("destination-schema-file", "", "file containing full CREATE TABLE for destination")
		insertSelectFile      = fs.String("insert-select-file", "", "file containing INSERT INTO destination SELECT ... FROM source")
		clickHouseURL         = fs.String("clickhouse-url", defaultClickHouseURL, "source ClickHouse HTTP URL for SHOW CREATE TABLE")
		clickHouseUser        = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword    = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		jobID                 = fs.String("job-id", "", "optional deterministic job id override")
		bucket                = fs.String("bucket", "", "S3 bucket")
		prefix                = fs.String("prefix", "partforge", "S3 key prefix")
		stateTable            = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region                = fs.String("aws-region", "us-east-1", "AWS region")
		s3Endpoint            = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		s5cmdBinary           = fs.String("s5cmd-binary", "s5cmd", "s5cmd binary path")
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

	if *database == "" || *table == "" || *freezeName == "" || *destinationDatabase == "" || *destinationTable == "" ||
		*destinationSchemaFile == "" || *insertSelectFile == "" || *bucket == "" {
		return errors.New("database, table, freeze, destination-database, destination-table, destination-schema-file, insert-select-file, and bucket are required")
	}

	destinationSchema, err := readRequiredFile(*destinationSchemaFile)
	if err != nil {
		return err
	}
	insertSelect, err := readRequiredFile(*insertSelectFile)
	if err != nil {
		return err
	}

	ch := chhttp.Client{
		URL:      *clickHouseURL,
		User:     *clickHouseUser,
		Password: *clickHousePassword,
	}
	sourceSchema, err := ch.QueryString(ctx, "SHOW CREATE TABLE "+chhttp.TableSQL(*database, *table)+" FORMAT TSVRaw")
	if err != nil {
		return fmt.Errorf("show create source table: %w", err)
	}
	sourceSchema = strings.TrimSpace(sourceSchema)
	if _, err := ddl.NormalizeCreateTable(sourceSchema); err != nil {
		return fmt.Errorf("source schema is not supported by worker: %w", err)
	}
	if _, err := ddl.NormalizeCreateTable(destinationSchema); err != nil {
		return fmt.Errorf("destination schema is not supported by worker: %w", err)
	}

	disks, err := freeze.LocalDisks(ctx, ch)
	if err != nil {
		return err
	}
	scannedParts, err := freeze.ScanDisks(disks, *freezeName)
	if err != nil {
		return err
	}

	resolvedJobID := *jobID
	if resolvedJobID == "" {
		resolvedJobID = manifest.DeriveJobID(*database, *table, *freezeName, sourceSchema, destinationSchema, insertSelect)
	}

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	copier := s3copy.Copier{Binary: *s5cmdBinary, Endpoint: *s3Endpoint}

	for _, sourcePart := range scannedParts {
		partID := manifest.DerivePartID(sourcePart.Disk, sourcePart.RelativePath, sourcePart.Name, sourceSchema, destinationSchema, insertSelect)
		sourceKey := manifest.SourcePartPrefix(*prefix, resolvedJobID, partID)
		finishedKey := manifest.FinishedPartPrefix(*prefix, resolvedJobID, partID)
		createdAt := time.Now().UTC()

		m := manifest.Manifest{
			Version:   manifest.Version,
			JobID:     resolvedJobID,
			PartID:    partID,
			Freeze:    *freezeName,
			Source:    manifest.TableRef{Database: *database, Table: *table},
			Dest:      manifest.TableRef{Database: *destinationDatabase, Table: *destinationTable},
			Part:      manifest.SourcePart{Disk: sourcePart.Disk, Name: sourcePart.Name, RelativePath: sourcePart.RelativePath},
			SQL:       manifest.SQLBundle{SourceSchema: sourceSchema, DestinationSchema: destinationSchema, InsertSelect: insertSelect},
			S3:        manifest.S3Refs{Bucket: *bucket, SourceKey: sourceKey, FinishedKey: finishedKey},
			CreatedAt: createdAt,
		}

		if err := artifact.WriteManifest(sourcePart.Path, m); err != nil {
			return fmt.Errorf("write source manifest for %s:%s: %w", sourcePart.Disk, sourcePart.RelativePath, err)
		}
		if err := copier.UploadDir(ctx, sourcePart.Path, *bucket, sourceKey); err != nil {
			return fmt.Errorf("upload source part %s:%s to s3://%s/%s: %w", sourcePart.Disk, sourcePart.RelativePath, *bucket, sourceKey, err)
		}

		partState := state.NewPart(resolvedJobID, partID, *bucket, sourceKey, finishedKey, createdAt)
		if err := stateStore.CreatePart(ctx, partState); err != nil {
			return fmt.Errorf("create state for %s: %w", sourceKey, err)
		}
		slog.Info("registered part", "disk", sourcePart.Disk, "part", sourcePart.RelativePath, "source_key", sourceKey, "finished_key", finishedKey)
	}

	slog.Info("upload complete", "job_id", resolvedJobID, "parts", len(scannedParts))
	return nil
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	var (
		configPath            = fs.String("config", defaultConfigPath, "JSON config file path")
		region                = fs.String("aws-region", "us-east-1", "AWS region")
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
	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	resolvedWorkerID, err := resolveWorkerID(*workerID)
	if err != nil {
		return err
	}

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
		server, err = chproc.Start(ctx, chproc.Config{
			Binary:     *clickHouseBinary,
			ConfigFile: *clickHouseConfigFile,
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
		part, err := stateStore.ClaimNextReady(ctx, resolvedWorkerID, time.Now().UTC())
		if err != nil {
			return err
		}
		if part == nil {
			if *once {
				slog.Info("no ready part available")
				return nil
			}
			if err := sleepOrDone(ctx, *pollInterval); err != nil {
				return err
			}
			continue
		}

		workItem := rewrite.WorkItem{
			Bucket:    part.Bucket,
			SourceKey: part.SourceKey,
			JobID:     part.JobID,
			PartID:    part.PartID,
			Attempt:   part.Attempts,
		}
		result, err := processor.ProcessPart(ctx, workItem)
		if err != nil {
			if markErr := stateStore.MarkFailed(ctx, *part, resolvedWorkerID, err, time.Now().UTC()); markErr != nil {
				return fmt.Errorf("process part %s/%s: %w; additionally failed to mark failed: %v", part.JobID, part.PartID, err, markErr)
			}
			return err
		}
		if err := stateStore.MarkFinished(ctx, *part, resolvedWorkerID, result.FinishedKey, time.Now().UTC()); err != nil {
			return err
		}
		if *once {
			return nil
		}
	}
}

func runImportFinished(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("import-finished", flag.ExitOnError)
	var (
		configPath         = fs.String("config", defaultConfigPath, "JSON config file path")
		database           = fs.String("database", "", "final destination database")
		table              = fs.String("table", "", "final destination table")
		jobID              = fs.String("job-id", "", "job id to import")
		stateTable         = fs.String("state-table", defaultStateTable, "DynamoDB state table")
		region             = fs.String("aws-region", "us-east-1", "AWS region")
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

	stateStore, err := state.New(ctx, state.Config{
		Region:   *region,
		Endpoint: *dynamoEndpoint,
		Table:    *stateTable,
	})
	if err != nil {
		return err
	}
	finishedParts, err := stateStore.ListFinishedParts(ctx, *jobID)
	if err != nil {
		return err
	}
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
		region         = fs.String("aws-region", "us-east-1", "AWS region")
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
		region         = fs.String("aws-region", "us-east-1", "AWS region")
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
		region         = fs.String("aws-region", "us-east-1", "AWS region")
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
