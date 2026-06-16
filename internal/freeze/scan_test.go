package freeze

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanFindsParts(t *testing.T) {
	root := t.TempDir()
	part := filepath.Join(root, "shadow", "freeze-1", "store", "abc", "all_1_1_0")
	if err := os.MkdirAll(part, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(part, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	parts, err := Scan(filepath.Join(root, "shadow"), "freeze-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("got %d parts", len(parts))
	}
	if parts[0].RelativePath != "store/abc/all_1_1_0" {
		t.Fatalf("unexpected relative path %q", parts[0].RelativePath)
	}
}
