package artifact

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	if got.JobID != m.JobID || got.Part.Name != m.Part.Name || got.Part.Disk != m.Part.Disk {
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

func TestExtractFinishedTarballsExtractsMultipleTarballs(t *testing.T) {
	root := t.TempDir()
	partA := createTestPart(t, filepath.Join(root, "parts-a", "all_1_1_0"), "a")
	partB := createTestPart(t, filepath.Join(root, "parts-b", "all_2_2_0"), "b")

	tarA := filepath.Join(root, "all_1_1_0.tar")
	if err := WriteFinishedTar(tarA, []string{partA}); err != nil {
		t.Fatal(err)
	}
	tarB := filepath.Join(root, "all_2_2_0.tar")
	if err := WriteFinishedTar(tarB, []string{partB}); err != nil {
		t.Fatal(err)
	}

	extractRoot := filepath.Join(root, "extract")
	got, err := ExtractFinishedTarballsContext(context.Background(), []string{tarB, tarA}, extractRoot)
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
	if string(raw) != "b" {
		t.Fatalf("extracted file = %q", raw)
	}
}

func TestExtractFinishedTarballsFromDirExtractsTarballs(t *testing.T) {
	root := t.TempDir()
	partA := createTestPart(t, filepath.Join(root, "parts-a", "all_1_1_0"), "a")
	partB := createTestPart(t, filepath.Join(root, "parts-b", "all_2_2_0"), "b")
	tarRoot := filepath.Join(root, "tarballs")
	if err := os.Mkdir(tarRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteFinishedTar(filepath.Join(tarRoot, "all_2_2_0.tar"), []string{partB}); err != nil {
		t.Fatal(err)
	}
	if err := WriteFinishedTar(filepath.Join(tarRoot, "all_1_1_0.tar"), []string{partA}); err != nil {
		t.Fatal(err)
	}

	extractRoot := filepath.Join(root, "extract")
	got, err := ExtractFinishedTarballsFromDirContext(context.Background(), tarRoot, extractRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"all_1_1_0", "all_2_2_0"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parts = %#v, want %#v", got, want)
	}
}

func TestFinishedTarballPaths(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"all_2_2_0.tar", "all_1_1_0.tar"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("tar"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	got, err := finishedTarballPaths(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(root, "all_1_1_0.tar"),
		filepath.Join(root, "all_2_2_0.tar"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tarballs = %#v, want %#v", got, want)
	}
}

func TestExtractFinishedTarballsFromDirRejectsRootDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "all_1_1_0"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := ExtractFinishedTarballsFromDirContext(context.Background(), root, filepath.Join(root, "extract"))
	if err == nil {
		t.Fatal("expected root directory error")
	}
	if !strings.Contains(err.Error(), "unexpected directory") {
		t.Fatalf("error = %v, want unexpected directory", err)
	}
}

func TestExtractFinishedTarballsFromDirRejectsNonTarFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := ExtractFinishedTarballsFromDirContext(context.Background(), root, filepath.Join(root, "extract"))
	if err == nil {
		t.Fatal("expected non-tar file error")
	}
	if !strings.Contains(err.Error(), "unexpected non-tar file") {
		t.Fatalf("error = %v, want unexpected non-tar file", err)
	}
}

func TestExtractFinishedTarballsRejectsDuplicatesBeforeExtract(t *testing.T) {
	root := t.TempDir()
	left := createTestPart(t, filepath.Join(root, "left", "all_1_1_0"), "left")
	right := createTestPart(t, filepath.Join(root, "right", "all_1_1_0"), "right")

	leftTar := filepath.Join(root, "left.tar")
	if err := WriteFinishedTar(leftTar, []string{left}); err != nil {
		t.Fatal(err)
	}
	rightTar := filepath.Join(root, "right.tar")
	if err := WriteFinishedTar(rightTar, []string{right}); err != nil {
		t.Fatal(err)
	}

	extractRoot := filepath.Join(root, "extract")
	_, err := ExtractFinishedTarballsContext(context.Background(), []string{leftTar, rightTar}, extractRoot)
	if err == nil {
		t.Fatal("expected duplicate finished part error")
	}
	if !strings.Contains(err.Error(), "duplicate finished part") {
		t.Fatalf("error = %v, want duplicate finished part", err)
	}
	entries, err := os.ReadDir(extractRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("extract root entries = %d, want 0", len(entries))
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

func TestExtractFinishedTarRejectsExistingPartFiles(t *testing.T) {
	root := t.TempDir()
	part := filepath.Join(root, "parts", "all_1_1_0")
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(part, "checksums.txt"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}

	tarPath := filepath.Join(root, "finished.tar")
	if err := WriteFinishedTar(tarPath, []string{part}); err != nil {
		t.Fatal(err)
	}
	extractRoot := filepath.Join(root, "extract")
	if _, err := ExtractFinishedTar(tarPath, extractRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := ExtractFinishedTar(tarPath, extractRoot); err == nil {
		t.Fatal("expected extracting over existing part files to fail")
	}
}

func createTestPart(t *testing.T, root, checksum string) string {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "checksums.txt"), []byte(checksum), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}
