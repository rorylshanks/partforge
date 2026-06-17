package parts

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDownloadedPartNames(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"all_2_2_0", "all_1_1_0"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	got, err := downloadedPartNames(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"all_1_1_0", "all_2_2_0"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parts = %#v, want %#v", got, want)
	}
}

func TestDownloadedPartNamesRejectsRootFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := downloadedPartNames(root); err == nil {
		t.Fatal("expected root file error")
	}
}

func TestDefaultImportWorkDirUsesClickHouseDisk(t *testing.T) {
	got := defaultImportWorkDir("/var/lib/clickhouse/")
	want := filepath.Join("/var/lib/clickhouse", "partforge-import-work")
	if got != want {
		t.Fatalf("work dir = %q, want %q", got, want)
	}
}

func TestPathContains(t *testing.T) {
	if !pathContains("/var/lib/clickhouse/", "/var/lib/clickhouse/store/abc/table") {
		t.Fatal("expected child path to be contained")
	}
	if pathContains("/var/lib/clickhouse/store", "/var/lib/clickhouse/store-other/table") {
		t.Fatal("expected sibling prefix path not to be contained")
	}
}

func TestEnsureSameFilesystem(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	if err := os.Mkdir(a, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(b, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureSameFilesystem(a, b); err != nil {
		t.Fatal(err)
	}
}
