package parts

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/partforge/partforge/internal/archive"
	"github.com/partforge/partforge/internal/awsio"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/fileutil"
	"github.com/partforge/partforge/internal/manifest"
)

type Importer struct {
	AWS        *awsio.Clients
	ClickHouse chhttp.Client
	WorkDir    string
}

type ImportJob struct {
	Bucket       string
	Prefix       string
	JobID        string
	Database     string
	Table        string
	RequireEmpty bool
}

func (i Importer) ImportJob(ctx context.Context, job ImportJob) error {
	prefix := manifest.FinishedPrefix(job.Prefix, job.JobID)
	keys, err := i.AWS.ListKeys(ctx, job.Bucket, prefix)
	if err != nil {
		return fmt.Errorf("list finished artifacts under %s: %w", prefix, err)
	}
	keys = filterTarGz(keys)
	if len(keys) == 0 {
		return fmt.Errorf("no finished artifacts found under s3://%s/%s", job.Bucket, prefix)
	}
	sort.Strings(keys)

	dataPath, err := i.tableDataPath(ctx, job.Database, job.Table)
	if err != nil {
		return err
	}
	detachedPath := filepath.Join(dataPath, "detached")
	if err := os.MkdirAll(detachedPath, 0o755); err != nil {
		return err
	}
	if job.RequireEmpty {
		count, err := i.activePartCount(ctx, job.Database, job.Table)
		if err != nil {
			return err
		}
		if count != 0 {
			return fmt.Errorf("destination table %s already has %d active parts; rerunning import-finished could duplicate data", chhttp.TableSQL(job.Database, job.Table), count)
		}
	}

	root := filepath.Join(defaultImportWorkDir(i.WorkDir), job.JobID)
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	defer os.RemoveAll(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}

	for idx, key := range keys {
		if err := i.importArtifact(ctx, job, key, detachedPath, filepath.Join(root, fmt.Sprintf("%06d", idx))); err != nil {
			return err
		}
	}
	slog.Info("import complete", "job_id", job.JobID, "artifacts", len(keys))
	return nil
}

func (i Importer) importArtifact(ctx context.Context, job ImportJob, key, detachedPath, workDir string) error {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return err
	}
	archivePath := filepath.Join(workDir, "finished.tar.gz")
	if err := i.AWS.DownloadToFile(ctx, job.Bucket, key, archivePath); err != nil {
		return fmt.Errorf("download finished artifact %s: %w", key, err)
	}

	f, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	m, extractErr := archive.Extract(f, filepath.Join(workDir, "extract"))
	closeErr := f.Close()
	if extractErr != nil {
		return fmt.Errorf("extract finished artifact %s: %w", key, extractErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if m.JobID != job.JobID {
		return fmt.Errorf("finished artifact %s belongs to job %s, expected %s", key, m.JobID, job.JobID)
	}
	if m.Dest.Database != job.Database || m.Dest.Table != job.Table {
		return fmt.Errorf("finished artifact %s targets %s, expected %s", key, chhttp.TableSQL(m.Dest.Database, m.Dest.Table), chhttp.TableSQL(job.Database, job.Table))
	}

	for _, part := range m.Output.Parts {
		src := archive.FinishedPartPath(filepath.Join(workDir, "extract"), part.Name)
		dst := filepath.Join(detachedPath, part.Name)
		if err := fileutil.CopyDir(src, dst); err != nil {
			return fmt.Errorf("copy finished part %s to detached: %w", part.Name, err)
		}
		if err := i.ClickHouse.Exec(ctx, "ALTER TABLE "+chhttp.TableSQL(job.Database, job.Table)+" ATTACH PART "+chhttp.StringLiteral(part.Name)); err != nil {
			return fmt.Errorf("attach finished part %s: %w", part.Name, err)
		}
	}
	slog.Info("imported artifact", "key", key, "parts", len(m.Output.Parts))
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

func filterTarGz(keys []string) []string {
	var filtered []string
	for _, key := range keys {
		if strings.HasSuffix(key, ".tar.gz") {
			filtered = append(filtered, key)
		}
	}
	return filtered
}

func defaultImportWorkDir(workDir string) string {
	if workDir == "" {
		return "/tmp/partforge-import"
	}
	return workDir
}
