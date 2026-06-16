package rewrite

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/partforge/partforge/internal/archive"
	"github.com/partforge/partforge/internal/awsio"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/ddl"
	"github.com/partforge/partforge/internal/fileutil"
	"github.com/partforge/partforge/internal/manifest"
	"github.com/partforge/partforge/internal/metrics"
)

type Processor struct {
	AWS          *awsio.Clients
	ClickHouse   chhttp.Client
	WorkDir      string
	MergeTimeout time.Duration
	Metrics      metrics.Recorder
}

func (p Processor) ProcessQueueMessage(ctx context.Context, queueURL string, envelope awsio.QueueEnvelope) error {
	msg, err := manifest.UnmarshalQueueMessage(envelope.Body)
	if err != nil {
		return fmt.Errorf("parse queue message: %w", err)
	}
	if msg.Bucket == "" || msg.Key == "" || msg.FinishedKey == "" || msg.JobID == "" || msg.PartID == "" {
		return fmt.Errorf("queue message is missing bucket, key, finished_key, job_id, or part_id")
	}
	if err := validateSafeSegment(msg.JobID); err != nil {
		return err
	}
	if err := validateSafeSegment(msg.PartID); err != nil {
		return err
	}

	if exists, err := p.AWS.ObjectExists(ctx, msg.Bucket, msg.FinishedKey); err != nil {
		return fmt.Errorf("check finished artifact %s: %w", msg.FinishedKey, err)
	} else if exists {
		slog.Info("finished artifact already exists; deleting duplicate queue message", "key", msg.FinishedKey)
		return p.AWS.DeleteQueueMessage(ctx, queueURL, envelope.ReceiptHandle)
	}

	root := filepath.Join(defaultWorkDir(p.WorkDir), msg.JobID, msg.PartID)
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}

	sourceArchive := filepath.Join(root, "source.tar.gz")
	if err := p.AWS.DownloadToFile(ctx, msg.Bucket, msg.Key, sourceArchive); err != nil {
		return fmt.Errorf("download source artifact %s: %w", msg.Key, err)
	}

	extractRoot := filepath.Join(root, "source")
	f, err := os.Open(sourceArchive)
	if err != nil {
		return err
	}
	m, extractErr := archive.Extract(f, extractRoot)
	closeErr := f.Close()
	if extractErr != nil {
		return fmt.Errorf("extract source artifact: %w", extractErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if m.JobID != msg.JobID || m.PartID != msg.PartID {
		return fmt.Errorf("queue message references %s/%s but manifest contains %s/%s", msg.JobID, msg.PartID, m.JobID, m.PartID)
	}
	if m.S3.Bucket != msg.Bucket || m.S3.SourceKey != msg.Key || m.S3.FinishedKey != msg.FinishedKey {
		return fmt.Errorf("queue message S3 reference does not match manifest")
	}

	if exists, err := p.AWS.ObjectExists(ctx, m.S3.Bucket, m.S3.FinishedKey); err != nil {
		return fmt.Errorf("check finished artifact %s: %w", m.S3.FinishedKey, err)
	} else if exists {
		slog.Info("finished artifact already exists; deleting duplicate queue message", "key", m.S3.FinishedKey)
		return p.AWS.DeleteQueueMessage(ctx, queueURL, envelope.ReceiptHandle)
	}

	recorder := p.recorder()
	recorder.ForgeStarted(m)
	finishedArchive := filepath.Join(root, "finished.tar.gz")
	if err := p.rewriteArchive(ctx, m, extractRoot, finishedArchive); err != nil {
		recorder.ForgeFailed(m)
		return err
	}
	if err := p.AWS.PutFile(ctx, m.S3.Bucket, m.S3.FinishedKey, finishedArchive); err != nil {
		recorder.ForgeFailed(m)
		return fmt.Errorf("upload finished artifact %s: %w", m.S3.FinishedKey, err)
	}
	if err := p.AWS.DeleteQueueMessage(ctx, queueURL, envelope.ReceiptHandle); err != nil {
		recorder.ForgeFailed(m)
		return fmt.Errorf("delete processed queue message: %w", err)
	}
	recorder.ForgeCompleted(m)
	slog.Info("processed part", "job_id", m.JobID, "part_id", m.PartID, "finished_key", m.S3.FinishedKey)
	return nil
}

func (p Processor) rewriteArchive(ctx context.Context, m manifest.Manifest, extractRoot, finishedArchive string) error {
	if m.Source.Database == m.Dest.Database && m.Source.Table == m.Dest.Table {
		return fmt.Errorf("source and destination table names must differ inside the worker")
	}
	sourceDDL, err := ddl.ForTable(m.SQL.SourceSchema, m.Source.Database, m.Source.Table)
	if err != nil {
		return fmt.Errorf("normalize source DDL: %w", err)
	}
	destDDL, err := ddl.ForTable(m.SQL.DestinationSchema, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return fmt.Errorf("normalize destination DDL: %w", err)
	}

	databases := uniqueStrings(m.Source.Database, m.Dest.Database)
	for _, database := range databases {
		if err := p.ClickHouse.Exec(ctx, "DROP DATABASE IF EXISTS "+chhttp.Ident(database)+" SYNC"); err != nil {
			return fmt.Errorf("drop worker database %s: %w", database, err)
		}
	}
	defer func() {
		for _, database := range databases {
			if err := p.ClickHouse.Exec(context.Background(), "DROP DATABASE IF EXISTS "+chhttp.Ident(database)+" SYNC"); err != nil {
				slog.Warn("failed to drop worker database", "database", database, "error", err)
			}
		}
	}()

	for _, database := range databases {
		if err := p.ClickHouse.Exec(ctx, "CREATE DATABASE "+chhttp.Ident(database)); err != nil {
			return fmt.Errorf("create worker database %s: %w", database, err)
		}
	}
	if err := p.ClickHouse.Exec(ctx, sourceDDL); err != nil {
		return fmt.Errorf("create source table: %w", err)
	}
	if err := p.ClickHouse.Exec(ctx, destDDL); err != nil {
		return fmt.Errorf("create destination table: %w", err)
	}

	sourceDataPath, err := p.tableDataPath(ctx, m.Source.Database, m.Source.Table)
	if err != nil {
		return err
	}
	sourceDetached := filepath.Join(sourceDataPath, "detached")
	if err := os.MkdirAll(sourceDetached, 0o755); err != nil {
		return err
	}
	if err := fileutil.CopyDir(archive.SourcePartPath(extractRoot, m), filepath.Join(sourceDetached, m.Part.Name)); err != nil {
		return fmt.Errorf("copy source part to detached: %w", err)
	}
	if err := p.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(m.Source.Database, m.Source.Table)+" ATTACH PART "+chhttp.StringLiteral(m.Part.Name)); err != nil {
		return fmt.Errorf("attach source part %s: %w", m.Part.Name, err)
	}
	sourceStats, err := p.activePartStats(ctx, m.Source.Database, m.Source.Table)
	if err != nil {
		return fmt.Errorf("measure source active parts: %w", err)
	}
	p.recorder().SetActivePartStats("source", m, sourceStats)

	if err := p.runInsertSelect(ctx, m); err != nil {
		return fmt.Errorf("run insert-select: %w", err)
	}
	if err := p.waitForMerges(ctx, m.Dest.Database, m.Dest.Table); err != nil {
		return err
	}
	destStats, err := p.activePartStats(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return fmt.Errorf("measure destination active parts: %w", err)
	}
	p.recorder().SetActivePartStats("destination", m, destStats)

	destDataPath, err := p.tableDataPath(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return err
	}
	outputParts, err := p.activeParts(ctx, m.Dest.Database, m.Dest.Table)
	if err != nil {
		return err
	}
	for _, part := range outputParts {
		if err := p.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(m.Dest.Database, m.Dest.Table)+" DETACH PART "+chhttp.StringLiteral(part.Name)); err != nil {
			return fmt.Errorf("detach destination part %s: %w", part.Name, err)
		}
	}

	out, err := os.Create(finishedArchive)
	if err != nil {
		return err
	}
	writeErr := archive.WriteFinished(out, m, filepath.Join(destDataPath, "detached"), outputParts)
	closeErr := out.Close()
	if writeErr != nil {
		return fmt.Errorf("write finished archive: %w", writeErr)
	}
	return closeErr
}

func (p Processor) runInsertSelect(ctx context.Context, m manifest.Manifest) error {
	queryID := "partforge-" + m.JobID + "-" + m.PartID
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- p.ClickHouse.ExecWithOptions(queryCtx, m.SQL.InsertSelect, chhttp.QueryOptions{QueryID: queryID})
	}()

	recorder := p.recorder()
	progress := metrics.QueryProgress{}
	defer recorder.ClearCurrentProgress(m)

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
			}
			return nil
		case <-ticker.C:
			current, found, err := p.queryProgress(ctx, queryID)
			if err != nil {
				cancel()
				<-errCh
				return fmt.Errorf("read live query progress: %w", err)
			}
			if found {
				recorder.ObserveProgress(m, progress, current)
				progress = current
			}
		case <-ctx.Done():
			cancel()
			<-errCh
			return ctx.Err()
		}
	}
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

func (p Processor) activeParts(ctx context.Context, database, table string) ([]manifest.OutputPart, error) {
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
	parts := make([]manifest.OutputPart, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, manifest.OutputPart{Name: row[0], PartitionID: row[1]})
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
