package rewrite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/artifact"
	"github.com/partforge/partforge/internal/chhttp"
	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/manifest"
)

func TestReduceInsertSelectThreadSettings(t *testing.T) {
	next, reduced, err := reduceInsertSelectThreadSettings(chhttp.QuerySettings{
		"max_threads":        "8",
		"max_insert_threads": "6",
		"max_memory_usage":   "12345",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reduced {
		t.Fatal("expected settings to be reduced")
	}
	if next["max_threads"] != "4" {
		t.Fatalf("max_threads = %q", next["max_threads"])
	}
	if next["max_insert_threads"] != "3" {
		t.Fatalf("max_insert_threads = %q", next["max_insert_threads"])
	}
	if next["max_memory_usage"] != "12345" {
		t.Fatalf("max_memory_usage = %q", next["max_memory_usage"])
	}
}

func TestReduceInsertSelectThreadSettingsStopsAtOne(t *testing.T) {
	_, reduced, err := reduceInsertSelectThreadSettings(chhttp.QuerySettings{
		"max_threads":        "1",
		"max_insert_threads": "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if reduced {
		t.Fatal("expected no reduction once max_insert_threads is 1")
	}
}

func TestRetryableInsertSelectError(t *testing.T) {
	err := &chhttp.QueryError{StatusCode: 500, Body: "Code: 241. DB::Exception: MEMORY_LIMIT_EXCEEDED"}
	if !retryableInsertSelectError(err) {
		t.Fatal("expected memory limit error to be retryable")
	}

	if retryableInsertSelectError(&chhttp.QueryError{StatusCode: 500, Body: "Syntax error"}) {
		t.Fatal("expected syntax error to be non-retryable")
	}
	if retryableInsertSelectError(errors.New("network error")) {
		t.Fatal("expected unstructured error to be non-retryable")
	}
}

func TestInsertSelectRetryBackoff(t *testing.T) {
	if got := insertSelectRetryBackoff(1); got != time.Second {
		t.Fatalf("attempt 1 backoff = %s", got)
	}
	if got := insertSelectRetryBackoff(4); got != 8*time.Second {
		t.Fatalf("attempt 4 backoff = %s", got)
	}
	if got := insertSelectRetryBackoff(10); got != 10*time.Second {
		t.Fatalf("attempt 10 backoff = %s", got)
	}
}

func TestShouldReportProgress(t *testing.T) {
	now := time.Unix(100, 0)
	if shouldReportProgress(0, time.Time{}, now) {
		t.Fatal("expected disabled interval to skip progress report")
	}
	if !shouldReportProgress(15*time.Second, time.Time{}, now) {
		t.Fatal("expected first progress report")
	}
	if shouldReportProgress(15*time.Second, now.Add(-14*time.Second), now) {
		t.Fatal("expected interval gate to skip report")
	}
	if !shouldReportProgress(15*time.Second, now.Add(-15*time.Second), now) {
		t.Fatal("expected interval gate to allow report")
	}
}

func TestCopyFrozenOutputParts(t *testing.T) {
	root := t.TempDir()
	frozenRoot := filepath.Join(root, "shadow", "freeze-1", "store", "abc")
	createFrozenPart(t, filepath.Join(frozenRoot, "all_1_1_0"), "one")
	createFrozenPart(t, filepath.Join(frozenRoot, "all_2_2_0"), "two")
	finishedRoot := filepath.Join(root, "finished")

	output, err := copyFrozenOutputParts([]activePart{
		{Name: "all_1_1_0", PartitionID: "all"},
		{Name: "all_2_2_0", PartitionID: "all"},
	}, []freeze.Part{
		{Name: "all_2_2_0", Path: filepath.Join(frozenRoot, "all_2_2_0")},
		{Name: "all_1_1_0", Path: filepath.Join(frozenRoot, "all_1_1_0")},
	}, finishedRoot)
	if err != nil {
		t.Fatal(err)
	}
	want := []manifest.OutputPart{
		{Name: "all_1_1_0", PartitionID: "all"},
		{Name: "all_2_2_0", PartitionID: "all"},
	}
	if len(output.Parts) != len(want) {
		t.Fatalf("output parts = %#v, want %#v", output.Parts, want)
	}
	for i := range want {
		if output.Parts[i] != want[i] {
			t.Fatalf("output parts = %#v, want %#v", output.Parts, want)
		}
	}

	got, err := os.ReadFile(filepath.Join(artifact.FinishedPartPath(finishedRoot, "all_1_1_0"), "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "one" {
		t.Fatalf("copied data = %q, want one", got)
	}
	got, err = os.ReadFile(filepath.Join(artifact.FinishedPartPath(finishedRoot, "all_2_2_0"), "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "two" {
		t.Fatalf("copied data = %q, want two", got)
	}
}

func TestCopyFrozenOutputPartsRequiresExactSnapshot(t *testing.T) {
	root := t.TempDir()
	frozenPart := filepath.Join(root, "shadow", "freeze-1", "store", "abc", "all_1_1_0")
	createFrozenPart(t, frozenPart, "one")

	if _, err := copyFrozenOutputParts([]activePart{
		{Name: "all_1_1_0", PartitionID: "all"},
		{Name: "all_2_2_0", PartitionID: "all"},
	}, []freeze.Part{{Name: "all_1_1_0", Path: frozenPart}}, filepath.Join(root, "finished")); err == nil {
		t.Fatal("expected part count mismatch error")
	}
}

func createFrozenPart(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"checksums.txt", "columns.txt"} {
		if err := os.WriteFile(filepath.Join(path, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(path, "data.bin"), []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}
