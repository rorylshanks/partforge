package artifact

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/manifest"
)

func TestManifestRoundTrip(t *testing.T) {
	root := t.TempDir()
	m := manifest.Manifest{
		Version:   manifest.Version,
		JobID:     "job-1",
		PartID:    "part-1",
		Freeze:    "freeze-1",
		Source:    manifest.TableRef{Database: "src", Table: "t"},
		Dest:      manifest.TableRef{Database: "dst", Table: "t2"},
		Part:      manifest.SourcePart{Disk: "default", Name: "all_1_1_0", RelativePath: "store/x/all_1_1_0"},
		SQL:       manifest.SQLBundle{SourceSchema: "CREATE TABLE src.t (x UInt64) ENGINE = MergeTree ORDER BY x", DestinationSchema: "CREATE TABLE dst.t2 (x UInt64) ENGINE = MergeTree ORDER BY x", InsertSelect: "INSERT INTO dst.t2 SELECT * FROM src.t"},
		Options:   manifest.Options{OptimizeFinal: true},
		S3:        manifest.S3Refs{Bucket: "bucket", SourceKey: "source/part-1", FinishedKey: "finished/part-1"},
		CreatedAt: time.Now(),
	}

	if err := WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ManifestName)); err != nil {
		t.Fatal(err)
	}

	got, err := ReadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if got.JobID != m.JobID || got.Part.Name != m.Part.Name || got.Part.Disk != m.Part.Disk || !got.Options.OptimizeFinal {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}

func TestFinishedTarRoundTrip(t *testing.T) {
	root := t.TempDir()
	partA := filepath.Join(root, "parts-a", "all_1_1_0")
	partB := filepath.Join(root, "parts-b", "all_2_2_0")
	for _, part := range []string{partA, partB} {
		if err := os.MkdirAll(filepath.Join(part, "subdir"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(part, "checksums.txt"), []byte(filepath.Base(part)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(part, "subdir", "data.bin"), []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	tarPath := filepath.Join(root, "finished.tar")
	if err := WriteFinishedTar(tarPath, []string{partB, partA}); err != nil {
		t.Fatal(err)
	}
	extractRoot := filepath.Join(root, "extract")
	got, err := ExtractFinishedTar(tarPath, extractRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"all_1_1_0", "all_2_2_0"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parts = %#v, want %#v", got, want)
	}
	raw, err := os.ReadFile(filepath.Join(extractRoot, "all_2_2_0", "checksums.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "all_2_2_0" {
		t.Fatalf("extracted file = %q", raw)
	}
}

func TestWriteFinishedTarRejectsDuplicatePartNames(t *testing.T) {
	root := t.TempDir()
	left := filepath.Join(root, "left", "all_1_1_0")
	right := filepath.Join(root, "right", "all_1_1_0")
	for _, part := range []string{left, right} {
		if err := os.MkdirAll(part, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	err := WriteFinishedTar(filepath.Join(root, "finished.tar"), []string{left, right})
	if err == nil {
		t.Fatal("expected duplicate part name error")
	}
}
