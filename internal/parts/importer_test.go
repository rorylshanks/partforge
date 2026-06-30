package parts

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PostHog/partforge/internal/artifact"
	"github.com/PostHog/partforge/internal/chhttp"
	"github.com/PostHog/partforge/internal/s3copy"
)

func TestImportArtifactDownloadsFinishedTarballs(t *testing.T) {
	root := t.TempDir()
	partRoot := filepath.Join(root, "source", "all_1_1_0")
	createPart(t, partRoot)
	tarPath := filepath.Join(root, "all_1_1_0.tar")
	if err := artifact.WriteFinishedTar(tarPath, []string{partRoot}); err != nil {
		t.Fatal(err)
	}
	binary, logFile := fakeS5cmdDownload(t, tarPath)
	var queries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		queries = append(queries, string(body))
	}))
	defer server.Close()

	detachedPath := filepath.Join(root, "detached")
	if err := os.Mkdir(detachedPath, 0o755); err != nil {
		t.Fatal(err)
	}
	owner, err := ownerFromPath(detachedPath)
	if err != nil {
		t.Fatal(err)
	}
	artifact := FinishedArtifact{
		Bucket: "bucket",
		Key:    "partforge/jobs/job-1/finished/part-1",
		PartID: "part-1",
	}

	err = (Importer{
		S3Copy:     s3copy.Copier{Binary: binary},
		ClickHouse: chhttp.Client{URL: server.URL},
	}).importArtifact(context.Background(), ImportJob{
		JobID:    "job-1",
		Database: "db",
		Table:    "dst",
	}, artifact, detachedPath, filepath.Join(root, "work"), owner)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}
	call := strings.TrimSpace(string(raw))
	wantSource := "cp s3://bucket/" + artifact.Key + "/* "
	if !strings.Contains(call, wantSource) {
		t.Fatalf("download call = %q, want finished artifact prefix source %q", call, wantSource)
	}
	if strings.Contains(call, "/data/*") || strings.Contains(call, "/attempt-") {
		t.Fatalf("download call uses old finished layout: %q", call)
	}
	if len(queries) != 1 || !strings.Contains(queries[0], "ATTACH PART 'all_1_1_0'") {
		t.Fatalf("attach queries = %#v", queries)
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

func fakeS5cmdDownload(t *testing.T, tarPath string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "s5cmd")
	logFile := filepath.Join(dir, "calls")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + shellQuote(logFile) + "\n" +
		"dest=\n" +
		"for arg do dest=$arg; done\n" +
		"dest=${dest%/}\n" +
		"mkdir -p \"$dest\"\n" +
		"cp " + shellQuote(tarPath) + " \"$dest/all_1_1_0.tar\"\n" +
		"exit 0\n"
	if err := os.WriteFile(binary, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binary, logFile
}

func createPart(t *testing.T, root string) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "data.bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
