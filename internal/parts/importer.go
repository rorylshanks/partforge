package parts

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	artifactpkg "github.com/partforge/partforge/internal/artifact"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/fileutil"
	"github.com/partforge/partforge/internal/s3copy"
)

type Importer struct {
	S3Copy     s3copy.Copier
	ClickHouse chhttp.Client
	WorkDir    string
}

type FinishedArtifact struct {
	Bucket string
	Key    string
	PartID string
}

type ImportJob struct {
	Artifacts        []FinishedArtifact
	JobID            string
	Database         string
	Table            string
	RequireEmpty     bool
	MarkImporting    func(context.Context, FinishedArtifact) error
	MarkImported     func(context.Context, FinishedArtifact) error
	MarkImportFailed func(context.Context, FinishedArtifact, error) error
}

func (i Importer) ImportJob(ctx context.Context, job ImportJob) error {
	if len(job.Artifacts) == 0 {
		return fmt.Errorf("no finished artifacts found for job %s", job.JobID)
	}
	startedAt := time.Now()
	artifacts := append([]FinishedArtifact(nil), job.Artifacts...)
	sort.Slice(artifacts, func(i, j int) bool {
		return artifacts[i].Key < artifacts[j].Key
	})
	for _, artifact := range artifacts {
		if artifact.Bucket == "" || artifact.Key == "" || artifact.PartID == "" {
			return fmt.Errorf("finished artifact is missing bucket, key, or part_id")
		}
	}

	slog.Info("import job started", "stage", "start_import", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table), "artifacts", len(artifacts), "require_empty", job.RequireEmpty)
	slog.Info("locating destination table data path", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
	dataPath, err := i.tableDataPath(ctx, job.Database, job.Table)
	if err != nil {
		return err
	}
	slog.Info("located destination table data path", "stage", "prepare_destination", "job_id", job.JobID, "data_path", dataPath)
	detachedPath := filepath.Join(dataPath, "detached")
	if err := os.MkdirAll(detachedPath, 0o755); err != nil {
		return err
	}
	if job.RequireEmpty {
		slog.Info("checking destination table is empty", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
		count, err := i.activePartCount(ctx, job.Database, job.Table)
		if err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("destination table %s already has %d active parts; rerunning import-finished could duplicate data", chhttp.TableSQL(job.Database, job.Table), count)
		}
		slog.Info("destination table is empty", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
	} else {
		slog.Info("skipping destination empty check", "stage", "prepare_destination", "job_id", job.JobID, "destination_table", chhttp.TableSQL(job.Database, job.Table))
	}

	root := filepath.Join(defaultImportWorkDir(i.WorkDir), job.JobID)
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}

	for idx, artifact := range artifacts {
		artifactStartedAt := time.Now()
		slog.Info(
			"importing artifact",
			"stage", "import_artifact",
			"job_id", job.JobID,
			"artifact_index", idx+1,
			"artifacts_total", len(artifacts),
			"part_id", artifact.PartID,
			"bucket", artifact.Bucket,
			"key", artifact.Key,
		)
		if job.MarkImporting != nil {
			if err := job.MarkImporting(ctx, artifact); err != nil {
				return err
			}
		}
		if err := i.importArtifact(ctx, job, artifact, detachedPath, filepath.Join(root, fmt.Sprintf("%06d", idx))); err != nil {
			if job.MarkImportFailed != nil {
				if markErr := job.MarkImportFailed(ctx, artifact, err); markErr != nil {
					return fmt.Errorf("import artifact s3://%s/%s: %w; additionally failed to mark import failed: %v", artifact.Bucket, artifact.Key, err, markErr)
				}
			}
			return err
		}
		if job.MarkImported != nil {
			if err := job.MarkImported(ctx, artifact); err != nil {
				return err
			}
		}
		slog.Info(
			"imported artifact",
			"stage", "import_artifact",
			"job_id", job.JobID,
			"artifact_index", idx+1,
			"artifacts_total", len(artifacts),
			"part_id", artifact.PartID,
			"key", artifact.Key,
			"elapsed", time.Since(artifactStartedAt),
		)
	}
	elapsed := time.Since(startedAt)
	slog.Info("import complete", "stage", "complete", "job_id", job.JobID, "artifacts", len(artifacts), "elapsed", elapsed, "artifacts_per_second", countRatePerSecond(len(artifacts), elapsed))
	return nil
}

func (i Importer) importArtifact(ctx context.Context, job ImportJob, artifact FinishedArtifact, detachedPath, workDir string) error {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}
	extractRoot := filepath.Join(workDir, "finished")
	slog.Info("downloading finished artifact", "stage", "download_finished", "job_id", job.JobID, "part_id", artifact.PartID, "bucket", artifact.Bucket, "key", artifact.Key)
	downloadStartedAt := time.Now()
	if err := i.S3Copy.DownloadPrefix(ctx, artifact.Bucket, artifact.Key, extractRoot); err != nil {
		return fmt.Errorf("download finished artifact s3://%s/%s: %w", artifact.Bucket, artifact.Key, err)
	}
	downloadStats, err := fileutil.StatDir(extractRoot)
	if err != nil {
		return fmt.Errorf("stat finished artifact s3://%s/%s: %w", artifact.Bucket, artifact.Key, err)
	}
	downloadElapsed := time.Since(downloadStartedAt)
	slog.Info("downloaded finished artifact", "stage", "download_finished", "job_id", job.JobID, "part_id", artifact.PartID, "files", downloadStats.Files, "bytes", downloadStats.Bytes, "elapsed", downloadElapsed, "bytes_per_second", ratePerSecond(downloadStats.Bytes, downloadElapsed))

	m, err := artifactpkg.ReadManifest(extractRoot)
	if err != nil {
		return fmt.Errorf("read finished manifest s3://%s/%s: %w", artifact.Bucket, artifact.Key, err)
	}
	if m.JobID != job.JobID {
		return fmt.Errorf("finished artifact s3://%s/%s belongs to job %s, expected %s", artifact.Bucket, artifact.Key, m.JobID, job.JobID)
	}
	if m.PartID != artifact.PartID {
		return fmt.Errorf("finished artifact s3://%s/%s belongs to part %s, expected %s", artifact.Bucket, artifact.Key, m.PartID, artifact.PartID)
	}

	for _, part := range m.Output.Parts {
		src := artifactpkg.FinishedPartPath(extractRoot, part.Name)
		dst := filepath.Join(detachedPath, part.Name)
		partStats, err := fileutil.StatDir(src)
		if err != nil {
			return fmt.Errorf("stat finished part %s: %w", part.Name, err)
		}
		slog.Info("attaching finished part", "stage", "attach_finished_part", "job_id", job.JobID, "part_id", artifact.PartID, "part", part.Name, "partition_id", part.PartitionID, "files", partStats.Files, "bytes", partStats.Bytes)
		if err := fileutil.MoveDir(src, dst); err != nil {
			return fmt.Errorf("move finished part %s to detached: %w", part.Name, err)
		}
		if err := i.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(job.Database, job.Table)+" ATTACH PART "+chhttp.StringLiteral(part.Name)); err != nil {
			return fmt.Errorf("attach finished part %s: %w", part.Name, err)
		}
	}
	slog.Info("attached finished artifact parts", "stage", "attach_finished_part", "job_id", job.JobID, "part_id", artifact.PartID, "key", artifact.Key, "parts", len(m.Output.Parts))
	return nil
}

func (i Importer) tableDataPath(ctx context.Context, database, table string) (string, error) {
	query := "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = " +
		chhttp.StringLiteral(database) + " AND name = " + chhttp.StringLiteral(table) + " FORMAT TSV"
	out, err := i.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return "", err
	}
	path := strings.TrimSpace(out)
	if path == "" {
		return "", fmt.Errorf("could not find data path for %s", chhttp.TableSQL(database, table))
	}
	return path, nil
}

func (i Importer) activePartCount(ctx context.Context, database, table string) (uint64, error) {
	query := "SELECT count() FROM system.parts WHERE database = " +
		chhttp.StringLiteral(database) + " AND table = " + chhttp.StringLiteral(table) +
		" AND active FORMAT TSV"
	out, err := i.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return 0, err
	}
	return chhttp.ParseUInt(out)
}

func defaultImportWorkDir(workDir string) string {
	if workDir == "" {
		return "/tmp/partforge-import"
	}
	return workDir
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
