package archive

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/manifest"
)

func TestSourceArchiveRoundTrip(t *testing.T) {
	root := t.TempDir()
	part := filepath.Join(root, "all_1_1_0")
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(part, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	m := manifest.Manifest{
		Version:   manifest.Version,
		JobID:     "job-1",
		PartID:    "part-1",
		Freeze:    "freeze-1",
		Source:    manifest.TableRef{Database: "src", Table: "t"},
		Dest:      manifest.TableRef{Database: "dst", Table: "t2"},
		Part:      manifest.SourcePart{Name: "all_1_1_0", RelativePath: "store/x/all_1_1_0"},
		SQL:       manifest.SQLBundle{SourceSchema: "CREATE TABLE src.t (x UInt64) ENGINE = MergeTree ORDER BY x", DestinationSchema: "CREATE TABLE dst.t2 (x UInt64) ENGINE = MergeTree ORDER BY x", InsertSelect: "INSERT INTO dst.t2 SELECT * FROM src.t"},
		S3:        manifest.S3Refs{Bucket: "bucket", SourceKey: "source.tar.gz", FinishedKey: "finished.tar.gz"},
		CreatedAt: time.Now(),
	}

	archivePath := filepath.Join(root, "source.tar.gz")
	f, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteSource(f, m, part); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	f, err = os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Extract(f, filepath.Join(root, "extract"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got.JobID != m.JobID || got.Part.Name != m.Part.Name {
		t.Fatalf("unexpected manifest: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(root, "extract", "part", "all_1_1_0", "checksums.txt")); err != nil {
		t.Fatal(err)
	}
}
