package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/partforge/partforge/internal/archive"
	"github.com/partforge/partforge/internal/awsio"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/chproc"
	"github.com/partforge/partforge/internal/ddl"
	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/manifest"
	"github.com/partforge/partforge/internal/metrics"
	"github.com/partforge/partforge/internal/parts"
	"github.com/partforge/partforge/internal/rewrite"
)

const defaultClickHouseURL = "http://127.0.0.1:8123"

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

Commands:
  upload-freeze     Package frozen source parts, upload them to S3, and enqueue SQS work.
  worker            Consume SQS work, rewrite source parts with local ClickHouse, and upload finished artifacts.
  import-finished   Attach finished artifacts into the final ClickHouse table with safe part renames.
  version           Print the build version.
`)
}

func runUploadFreeze(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("upload-freeze", flag.ExitOnError)
	var (
		database              = fs.String("database", "", "source database")
		table                 = fs.String("table", "", "source table")
		freezeName            = fs.String("freeze", "", "ALTER TABLE ... FREEZE WITH NAME value")
		shadowDir             = fs.String("shadow-dir", "/var/lib/clickhouse/shadow", "ClickHouse shadow directory")
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
		queueURL              = fs.String("queue-url", "", "SQS queue URL")
		region                = fs.String("aws-region", "us-east-1", "AWS region")
		s3Endpoint            = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		sqsEndpoint           = fs.String("sqs-endpoint", "", "optional SQS endpoint, e.g. LocalStack")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *database == "" || *table == "" || *freezeName == "" || *destinationDatabase == "" || *destinationTable == "" ||
		*destinationSchemaFile == "" || *insertSelectFile == "" || *bucket == "" || *queueURL == "" {
		return errors.New("database, table, freeze, destination-database, destination-table, destination-schema-file, insert-select-file, bucket, and queue-url are required")
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

	scannedParts, err := freeze.Scan(*shadowDir, *freezeName)
	if err != nil {
		return err
	}
	if len(scannedParts) == 0 {
		return fmt.Errorf("no ClickHouse parts found under %s", filepath.Join(*shadowDir, *freezeName))
	}

	resolvedJobID := *jobID
	if resolvedJobID == "" {
		resolvedJobID = manifest.DeriveJobID(*database, *table, *freezeName, sourceSchema, destinationSchema, insertSelect)
	}

	awsClients, err := awsio.New(ctx, awsio.Config{
		Region:      *region,
		S3Endpoint:  *s3Endpoint,
		SQSEndpoint: *sqsEndpoint,
	})
	if err != nil {
		return err
	}

	for _, sourcePart := range scannedParts {
		partID := manifest.DerivePartID(sourcePart.RelativePath, sourcePart.Name, sourceSchema, destinationSchema, insertSelect)
		sourceKey := manifest.SourceArchiveKey(*prefix, resolvedJobID, partID)
		finishedKey := manifest.FinishedArchiveKey(*prefix, resolvedJobID, partID)

		m := manifest.Manifest{
			Version:   manifest.Version,
			JobID:     resolvedJobID,
			PartID:    partID,
			Freeze:    *freezeName,
			Source:    manifest.TableRef{Database: *database, Table: *table},
			Dest:      manifest.TableRef{Database: *destinationDatabase, Table: *destinationTable},
			Part:      manifest.SourcePart{Name: sourcePart.Name, RelativePath: sourcePart.RelativePath},
			SQL:       manifest.SQLBundle{SourceSchema: sourceSchema, DestinationSchema: destinationSchema, InsertSelect: insertSelect},
			S3:        manifest.S3Refs{Bucket: *bucket, SourceKey: sourceKey, FinishedKey: finishedKey},
			CreatedAt: time.Now().UTC(),
		}

		tmp, err := os.CreateTemp("", "partforge-source-*.tar.gz")
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		if err := archive.WriteSource(tmp, m, sourcePart.Path); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return fmt.Errorf("create source archive for %s: %w", sourcePart.RelativePath, err)
		}
		if err := tmp.Close(); err != nil {
			_ = os.Remove(tmpName)
			return err
		}
		if err := awsClients.PutFile(ctx, *bucket, sourceKey, tmpName); err != nil {
			_ = os.Remove(tmpName)
			return fmt.Errorf("upload %s: %w", sourceKey, err)
		}
		_ = os.Remove(tmpName)

		msg := manifest.QueueMessage{Bucket: *bucket, Key: sourceKey, FinishedKey: finishedKey, JobID: resolvedJobID, PartID: partID}
		if err := awsClients.SendQueueMessage(ctx, *queueURL, msg); err != nil {
			return fmt.Errorf("send queue message for %s: %w", sourceKey, err)
		}
		slog.Info("queued part", "part", sourcePart.RelativePath, "source_key", sourceKey, "finished_key", finishedKey)
	}

	slog.Info("upload complete", "job_id", resolvedJobID, "parts", len(scannedParts))
	return nil
}

func runWorker(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	var (
		queueURL             = fs.String("queue-url", "", "SQS queue URL")
		region               = fs.String("aws-region", "us-east-1", "AWS region")
		s3Endpoint           = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		sqsEndpoint          = fs.String("sqs-endpoint", "", "optional SQS endpoint, e.g. LocalStack")
		clickHouseURL        = fs.String("clickhouse-url", defaultClickHouseURL, "local ClickHouse HTTP URL")
		clickHouseUser       = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword   = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		startClickHouse      = fs.Bool("start-clickhouse", true, "start clickhouse-server as a child process")
		clickHouseBinary     = fs.String("clickhouse-binary", "clickhouse", "clickhouse binary path")
		clickHouseConfigFile = fs.String("clickhouse-config-file", "/etc/clickhouse-server/config.xml", "clickhouse-server config file")
		once                 = fs.Bool("once", false, "process one message and exit")
		workDir              = fs.String("work-dir", "/tmp/partforge", "worker scratch directory")
		mergeTimeout         = fs.Duration("merge-timeout", 10*time.Minute, "maximum time to wait for destination merges")
		metricsAddr          = fs.String("metrics-addr", ":2112", "Prometheus metrics listen address; empty disables metrics")
		metricsPath          = fs.String("metrics-path", "/metrics", "Prometheus metrics HTTP path")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *queueURL == "" {
		return errors.New("queue-url is required")
	}

	awsClients, err := awsio.New(ctx, awsio.Config{
		Region:      *region,
		S3Endpoint:  *s3Endpoint,
		SQSEndpoint: *sqsEndpoint,
	})
	if err != nil {
		return err
	}

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
		AWS:          awsClients,
		ClickHouse:   ch,
		WorkDir:      *workDir,
		MergeTimeout: *mergeTimeout,
		Metrics:      recorder,
	}

	for {
		message, err := awsClients.ReceiveQueueMessage(ctx, *queueURL)
		if err != nil {
			return err
		}
		if message == nil {
			if *once {
				slog.Info("no message available")
				return nil
			}
			continue
		}

		if err := processor.ProcessQueueMessage(ctx, *queueURL, *message); err != nil {
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
		database           = fs.String("database", "", "final destination database")
		table              = fs.String("table", "", "final destination table")
		jobID              = fs.String("job-id", "", "job id to import")
		bucket             = fs.String("bucket", "", "S3 bucket")
		prefix             = fs.String("prefix", "partforge", "S3 key prefix")
		region             = fs.String("aws-region", "us-east-1", "AWS region")
		s3Endpoint         = fs.String("s3-endpoint", "", "optional S3 endpoint, e.g. LocalStack")
		clickHouseURL      = fs.String("clickhouse-url", defaultClickHouseURL, "destination ClickHouse HTTP URL")
		clickHouseUser     = fs.String("clickhouse-user", "", "ClickHouse HTTP user")
		clickHousePassword = fs.String("clickhouse-password", "", "ClickHouse HTTP password")
		workDir            = fs.String("work-dir", "/tmp/partforge-import", "import scratch directory")
		requireEmpty       = fs.Bool("require-empty", true, "fail if the destination table already has active parts")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *database == "" || *table == "" || *jobID == "" || *bucket == "" {
		return errors.New("database, table, job-id, and bucket are required")
	}

	awsClients, err := awsio.New(ctx, awsio.Config{
		Region:     *region,
		S3Endpoint: *s3Endpoint,
	})
	if err != nil {
		return err
	}

	importer := parts.Importer{
		AWS:        awsClients,
		ClickHouse: chhttp.Client{URL: *clickHouseURL, User: *clickHouseUser, Password: *clickHousePassword},
		WorkDir:    *workDir,
	}
	return importer.ImportJob(ctx, parts.ImportJob{
		Bucket:       *bucket,
		Prefix:       *prefix,
		JobID:        *jobID,
		Database:     *database,
		Table:        *table,
		RequireEmpty: *requireEmpty,
	})
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
