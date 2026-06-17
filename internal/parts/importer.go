package parts

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
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

	workBase, err := i.importWorkDir(ctx, dataPath)
	if err != nil {
		return err
	}
	root := filepath.Join(workBase, job.JobID)
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	if err := ensureSameFilesystem(root, detachedPath); err != nil {
		return err
	}
	slog.Info("prepared import work directory", "stage", "prepare_destination", "job_id", job.JobID, "work_dir", root, "detached_path", detachedPath)

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
	downloadRoot := filepath.Join(workDir, "data")
	sourceKey := path.Join(artifact.Key, artifactpkg.FinishedDataName)
	slog.Info("downloading finished artifact data", "stage", "download_finished", "job_id", job.JobID, "part_id", artifact.PartID, "bucket", artifact.Bucket, "key", sourceKey)
	downloadStartedAt := time.Now()
	if err := i.S3Copy.DownloadPrefix(ctx, artifact.Bucket, sourceKey, downloadRoot); err != nil {
		return fmt.Errorf("download finished artifact data s3://%s/%s: %w", artifact.Bucket, sourceKey, err)
	}
	downloadStats, err := fileutil.StatDir(downloadRoot)
	if err != nil {
		return fmt.Errorf("stat finished artifact s3://%s/%s: %w", artifact.Bucket, artifact.Key, err)
	}
	downloadElapsed := time.Since(downloadStartedAt)
	slog.Info("downloaded finished artifact data", "stage", "download_finished", "job_id", job.JobID, "part_id", artifact.PartID, "files", downloadStats.Files, "bytes", downloadStats.Bytes, "elapsed", downloadElapsed, "bytes_per_second", ratePerSecond(downloadStats.Bytes, downloadElapsed))
	partNames, err := downloadedPartNames(downloadRoot)
	if err != nil {
		return fmt.Errorf("list downloaded finished parts s3://%s/%s: %w", artifact.Bucket, sourceKey, err)
	}
	if len(partNames) == 0 {
		return fmt.Errorf("finished artifact s3://%s/%s contains no part directories", artifact.Bucket, sourceKey)
	}

	for _, partName := range partNames {
		src := filepath.Join(downloadRoot, partName)
		dst := filepath.Join(detachedPath, partName)
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("detached part destination already exists: %s", dst)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := fileutil.MoveDir(src, dst); err != nil {
			return fmt.Errorf("move finished part %s into detached directory: %w", partName, err)
		}
		partStats, err := fileutil.StatDir(dst)
		if err != nil {
			return fmt.Errorf("stat finished part %s: %w", partName, err)
		}
		slog.Info("attaching finished part", "stage", "attach_finished_part", "job_id", job.JobID, "part_id", artifact.PartID, "part", partName, "files", partStats.Files, "bytes", partStats.Bytes)
		if err := i.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(job.Database, job.Table)+" ATTACH PART "+chhttp.StringLiteral(partName)); err != nil {
			return fmt.Errorf("attach finished part %s: %w", partName, err)
		}
	}
	slog.Info("attached finished artifact parts", "stage", "attach_finished_part", "job_id", job.JobID, "part_id", artifact.PartID, "key", artifact.Key, "parts", len(partNames))
	return nil
}

func downloadedPartNames(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			return nil, fmt.Errorf("unexpected file at finished artifact root: %s", filepath.Join(root, entry.Name()))
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
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

func (i Importer) importWorkDir(ctx context.Context, dataPath string) (string, error) {
	if strings.TrimSpace(i.WorkDir) != "" {
		return filepath.Abs(strings.TrimSpace(i.WorkDir))
	}
	diskPath, err := i.diskPathForDataPath(ctx, dataPath)
	if err != nil {
		return "", err
	}
	return defaultImportWorkDir(diskPath), nil
}

func (i Importer) diskPathForDataPath(ctx context.Context, dataPath string) (string, error) {
	query := "SELECT path, type FROM system.disks ORDER BY length(path) DESC FORMAT TSV"
	out, err := i.ClickHouse.QueryString(ctx, query)
	if err != nil {
		return "", fmt.Errorf("query ClickHouse disks: %w", err)
	}
	rows, err := chhttp.FormatTSVStrings(out, 2)
	if err != nil {
		return "", err
	}
	for _, row := range rows {
		diskPath := strings.TrimSpace(row[0])
		diskType := strings.ToLower(strings.TrimSpace(row[1]))
		if !pathContains(diskPath, dataPath) {
			continue
		}
		if diskType != "local" {
			return "", fmt.Errorf("destination table data path %s is on unsupported ClickHouse disk type %q", dataPath, row[1])
		}
		return diskPath, nil
	}
	return "", fmt.Errorf("could not match destination data path %s to a local ClickHouse disk", dataPath)
}

func defaultImportWorkDir(diskPath string) string {
	return filepath.Join(diskPath, "partforge-import-work")
}

func pathContains(root, child string) bool {
	root = filepath.Clean(root)
	child = filepath.Clean(child)
	rel, err := filepath.Rel(root, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func ensureSameFilesystem(a, b string) error {
	aDev, err := deviceID(a)
	if err != nil {
		return err
	}
	bDev, err := deviceID(b)
	if err != nil {
		return err
	}
	if aDev != bDev {
		return fmt.Errorf("import work directory %s and ClickHouse detached directory %s are on different filesystems; set -work-dir to a path on the ClickHouse data disk", a, b)
	}
	return nil
}

func deviceID(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("stat %s did not return syscall.Stat_t", path)
	}
	return uint64(stat.Dev), nil
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
