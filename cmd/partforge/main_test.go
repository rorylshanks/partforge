package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/freeze"
	"github.com/partforge/partforge/internal/metrics"
	"github.com/partforge/partforge/internal/rewrite"
	"github.com/partforge/partforge/internal/state"
)

func TestDefaultCompactWindow(t *testing.T) {
	if defaultCompactWindow != 24*time.Hour {
		t.Fatalf("defaultCompactWindow = %s, want 24h", defaultCompactWindow)
	}
}

func TestSummarizeJob(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{PartID: "part-1", Status: state.StatusImported, ReadRows: 10, ReadBytes: 100, WrittenRows: 9, WrittenBytes: 90, DestinationFailedMerges: 1},
		{PartID: "part-2", Status: state.StatusFinished, ReadRows: 20, ReadBytes: 200, WrittenRows: 19, WrittenBytes: 190, DestinationFailedMerges: 2},
		{PartID: "part-3", Status: state.StatusFailed, Error: "boom"},
	})

	if summary.Status != "FAILED" {
		t.Fatalf("status = %s", summary.Status)
	}
	if summary.Total != 3 {
		t.Fatalf("total = %d", summary.Total)
	}
	if summary.RewriteCompleted != 2 {
		t.Fatalf("rewrite completed = %d", summary.RewriteCompleted)
	}
	if summary.ImportCompleted != 1 {
		t.Fatalf("import completed = %d", summary.ImportCompleted)
	}
	if summary.ReadRows != 30 || summary.ReadBytes != 300 || summary.WrittenRows != 28 || summary.WrittenBytes != 280 {
		t.Fatalf("summary progress = %+v", summary)
	}
	if summary.FailedMerges != 3 {
		t.Fatalf("failed merges = %d, want 3", summary.FailedMerges)
	}
	if len(summary.FailedParts) != 1 || summary.FailedParts[0].PartID != "part-3" {
		t.Fatalf("failed parts = %+v", summary.FailedParts)
	}
}

func TestSummarizeJobPartCounts(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{
			PartID:                     "part-1",
			Status:                     state.StatusSuperseded,
			DestinationActivePartCount: 4,
			SupersededBy:               "compact-1",
		},
		{
			PartID:                     "part-2",
			Status:                     state.StatusSuperseded,
			DestinationActivePartCount: 3,
			SupersededBy:               "compact-1",
		},
		{
			PartID:                     "compact-1",
			Status:                     state.StatusCompactReady,
			CompactGeneration:          1,
			CompactInputPartIDs:        []string{"part-1", "part-2"},
			DestinationActivePartCount: 2,
		},
		{
			PartID:                     "part-3",
			Status:                     state.StatusInProgress,
			RewriteStage:               "wait_merges",
			SourceActivePartCount:      1,
			DestinationActivePartCount: 5,
		},
	})

	if summary.InputClickHouseParts != 3 {
		t.Fatalf("input clickhouse parts = %d, want 3 original source parts", summary.InputClickHouseParts)
	}
	if summary.CurrentOutputClickHouseParts != 7 {
		t.Fatalf("current output clickhouse parts = %d, want compact output plus in-progress output", summary.CurrentOutputClickHouseParts)
	}
	compactReady := findStatusPartStats(summary.StatePartStats, state.StatusCompactReady)
	if compactReady.InputClickHouseParts != 7 || compactReady.OutputClickHouseParts != 2 {
		t.Fatalf("compact-ready part stats = %+v, want input=7 output=2", compactReady)
	}
	inProgress := findStatusPartStats(summary.StatePartStats, state.StatusInProgress)
	if inProgress.InputClickHouseParts != 1 || inProgress.OutputClickHouseParts != 5 {
		t.Fatalf("in-progress part stats = %+v, want input=1 output=5", inProgress)
	}
	superseded := findStatusPartStats(summary.StatePartStats, state.StatusSuperseded)
	if superseded.InputClickHouseParts != 0 || superseded.OutputClickHouseParts != 0 {
		t.Fatalf("superseded part stats = %+v, want input=0 output=0", superseded)
	}
}

func TestSummarizeJobCompactingProgressCountsBatchOnce(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{
			JobID:                  "job-1",
			PartID:                 "part-1",
			Status:                 state.StatusCompacting,
			WorkerID:               "worker-1",
			CompactOutputPartID:    "compact-1",
			CompactInputPartCount:  9,
			CompactOutputPartCount: 4,
		},
		{
			JobID:                  "job-1",
			PartID:                 "part-2",
			Status:                 state.StatusCompacting,
			WorkerID:               "worker-1",
			CompactOutputPartID:    "compact-1",
			CompactInputPartCount:  9,
			CompactOutputPartCount: 4,
		},
	})

	if summary.CurrentOutputClickHouseParts != 4 {
		t.Fatalf("current output clickhouse parts = %d, want compact batch output counted once", summary.CurrentOutputClickHouseParts)
	}
	compacting := findStatusPartStats(summary.StatePartStats, state.StatusCompacting)
	if compacting.Count != 2 || compacting.InputClickHouseParts != 9 || compacting.OutputClickHouseParts != 4 {
		t.Fatalf("compacting part stats = %+v, want count=2 input=9 output=4", compacting)
	}
}

func TestSummarizeJobCompactFinalizationETA(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	summary := summarizeJobWithOptions("job-1", []state.Part{
		{
			PartID:         "part-1",
			Status:         state.StatusCompactReady,
			CompactReadyAt: now.Add(-90 * time.Minute).Format(time.RFC3339Nano),
		},
		{
			PartID:         "part-2",
			Status:         state.StatusCompactReady,
			CompactReadyAt: now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
		},
	}, jobSummaryOptions{
		Now:           now,
		CompactWindow: 2 * time.Hour,
	})

	if summary.Compact == nil {
		t.Fatal("expected compact summary")
	}
	if summary.Compact.FinalizeStatus != "waiting" {
		t.Fatalf("finalize status = %q, want waiting", summary.Compact.FinalizeStatus)
	}
	if summary.Compact.FinalizeIn != "1h30m0s" {
		t.Fatalf("finalize in = %q, want 1h30m0s", summary.Compact.FinalizeIn)
	}
	if summary.Compact.FinalizeAfter != now.Add(90*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("finalize after = %q", summary.Compact.FinalizeAfter)
	}
}

func TestSummarizeJobCompactFinalizationReadyForSingleUnmergeablePart(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	summary := summarizeJobWithOptions("job-1", []state.Part{
		{
			PartID:                     "part-1",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
	}, jobSummaryOptions{
		Now:           now,
		CompactWindow: 2 * time.Hour,
	})

	if summary.Compact == nil {
		t.Fatal("expected compact summary")
	}
	if summary.Compact.FinalizeStatus != "ready" {
		t.Fatalf("finalize status = %q, want ready", summary.Compact.FinalizeStatus)
	}
	if summary.Compact.FinalizeIn != "0s" {
		t.Fatalf("finalize in = %q, want 0s", summary.Compact.FinalizeIn)
	}
}

func TestSummarizeJobCompactFinalizationReadyForIsolatedPartWithCompactingElsewhere(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	summary := summarizeJobWithOptions("job-1", []state.Part{
		{
			PartID:                     "part-1",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
		{
			PartID:                     "part-2",
			Status:                     state.StatusCompacting,
			UpdatedAt:                  now.Add(-time.Hour).Format(time.RFC3339Nano),
			DestinationActivePartCount: 4,
			DestinationActivePartitionCounts: map[string]uint64{
				"202607": 4,
			},
		},
	}, jobSummaryOptions{
		Now:           now,
		CompactWindow: 2 * time.Hour,
	})

	if summary.Compact == nil {
		t.Fatal("expected compact summary")
	}
	if summary.Compact.FinalizeStatus != "ready" {
		t.Fatalf("finalize status = %q, want ready", summary.Compact.FinalizeStatus)
	}
	if summary.Compact.FinalizeIn != "0s" {
		t.Fatalf("finalize in = %q, want 0s", summary.Compact.FinalizeIn)
	}
}

func TestSummarizeJobCompactFinalizationBlockers(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	summary := summarizeJobWithOptions("job-1", []state.Part{
		{
			PartID:         "part-1",
			Status:         state.StatusCompactReady,
			CompactReadyAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		},
		{
			PartID:    "part-2",
			Status:    state.StatusCompacting,
			UpdatedAt: now.Add(-time.Hour).Format(time.RFC3339Nano),
		},
		{
			PartID: "part-3",
			Status: state.StatusFailed,
		},
	}, jobSummaryOptions{
		Now:           now,
		CompactWindow: 2 * time.Hour,
	})

	if summary.Compact == nil {
		t.Fatal("expected compact summary")
	}
	if summary.Compact.FinalizeStatus != "blocked" {
		t.Fatalf("finalize status = %q, want blocked", summary.Compact.FinalizeStatus)
	}
	if summary.Compact.BlockedByMessage != "COMPACTING=1, FAILED=1" {
		t.Fatalf("blocked by = %q", summary.Compact.BlockedByMessage)
	}
}

func TestSummarizeJobCountsInProgressStages(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{PartID: "part-1", Status: state.StatusInProgress, RewriteStage: "wait_merges"},
		{PartID: "part-2", Status: state.StatusInProgress, RewriteStage: "insert_select"},
		{PartID: "part-3", Status: state.StatusInProgress, RewriteStage: "insert_select"},
		{PartID: "part-4", Status: state.StatusInProgress},
		{PartID: "part-5", Status: state.StatusFinished, RewriteStage: "insert_select"},
	})

	want := []inProgressStageCount{
		{Stage: "insert_select", Count: 2, InputClickHouseParts: 2},
		{Stage: "wait_merges", Count: 1, InputClickHouseParts: 1},
		{Stage: "unknown", Count: 1, InputClickHouseParts: 1},
	}
	if len(summary.InProgressStages) != len(want) {
		t.Fatalf("in-progress stages = %+v, want %+v", summary.InProgressStages, want)
	}
	for i := range want {
		if summary.InProgressStages[i] != want[i] {
			t.Fatalf("in-progress stage %d = %+v, want %+v", i, summary.InProgressStages[i], want[i])
		}
	}
}

func findStatusPartStats(stats []statusPartStats, status state.Status) statusPartStats {
	for _, stat := range stats {
		if stat.Status == status {
			return stat
		}
	}
	return statusPartStats{}
}

func TestSelectRetryParts(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusFailed},
		{PartID: "part-2", Status: state.StatusImported},
		{PartID: "part-3", Status: state.StatusFailed},
		{PartID: "part-4", Status: state.StatusInProgress},
		{PartID: "part-5", Status: state.StatusFinished},
	}

	all, err := selectRetryParts(parts, retryPartSelection{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all len = %d", len(all))
	}

	allWithInProgress, err := selectRetryParts(parts, retryPartSelection{All: true, IncludeInProgress: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(allWithInProgress) != 3 || allWithInProgress[2].PartID != "part-4" {
		t.Fatalf("all with in-progress = %+v", allWithInProgress)
	}

	forced, err := selectRetryParts(parts, retryPartSelection{All: true, Force: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(forced) != 5 {
		t.Fatalf("forced len = %d", len(forced))
	}

	forcedOne, err := selectRetryParts(parts, retryPartSelection{Force: true, PartID: "part-2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(forcedOne) != 1 || forcedOne[0].PartID != "part-2" {
		t.Fatalf("forced one = %+v", forcedOne)
	}

	one, err := selectRetryParts(parts, retryPartSelection{PartID: "part-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].PartID != "part-1" {
		t.Fatalf("one = %+v", one)
	}

	inProgress, err := selectRetryParts(parts, retryPartSelection{IncludeInProgress: true, PartID: "part-4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(inProgress) != 1 || inProgress[0].PartID != "part-4" {
		t.Fatalf("in-progress = %+v", inProgress)
	}

	if _, err := selectRetryParts(parts, retryPartSelection{PartID: "part-2"}); err == nil {
		t.Fatal("expected non-failed part error")
	}

	if _, err := selectRetryParts(parts, retryPartSelection{IncludeInProgress: true, PartID: "part-5"}); err == nil {
		t.Fatal("expected completed part error")
	}
}

func TestSelectDeletePartsByStatus(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusImported},
		{PartID: "part-2", Status: state.StatusFinished},
		{PartID: "part-3", Status: state.StatusImported},
	}

	selected, err := selectDeleteParts(parts, deletePartSelection{Status: state.StatusImported})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].PartID != "part-1" || selected[1].PartID != "part-3" {
		t.Fatalf("selected = %+v, want imported parts", selected)
	}
}

func TestSelectDeletePartsByRepeatedPartID(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusImported},
		{PartID: "part-2", Status: state.StatusFinished},
	}

	selected, err := selectDeleteParts(parts, deletePartSelection{PartIDs: []string{"part-2", "part-2", "part-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].PartID != "part-2" || selected[1].PartID != "part-1" {
		t.Fatalf("selected = %+v, want selected ids in request order without duplicates", selected)
	}
}

func TestSelectDeletePartsRejectsUnknownStatus(t *testing.T) {
	_, err := selectDeleteParts([]state.Part{
		{PartID: "part-1", Status: state.StatusImported},
	}, deletePartSelection{Status: state.Status("NOT_A_STATUS")})
	if err == nil {
		t.Fatal("expected unknown status error")
	}
}

func TestSelectDeletePartsRejectsMissingPartID(t *testing.T) {
	_, err := selectDeleteParts([]state.Part{
		{PartID: "part-1", Status: state.StatusImported},
	}, deletePartSelection{PartIDs: []string{"part-missing"}})
	if err == nil {
		t.Fatal("expected missing part error")
	}
}

func TestSelectSetPartStatePartsByStatus(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusCompacting},
		{PartID: "part-2", Status: state.StatusCompactReady},
		{PartID: "part-3", Status: state.StatusCompacting},
	}

	selected, err := selectSetPartStateParts(parts, setPartStateSelection{Status: state.StatusCompacting})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].PartID != "part-1" || selected[1].PartID != "part-3" {
		t.Fatalf("selected = %+v, want compacting parts", selected)
	}
}

func TestSelectSetPartStatePartsByRepeatedPartID(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusCompacting},
		{PartID: "part-2", Status: state.StatusCompactReady},
	}

	selected, err := selectSetPartStateParts(parts, setPartStateSelection{PartIDs: []string{"part-2", "part-2", "part-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].PartID != "part-2" || selected[1].PartID != "part-1" {
		t.Fatalf("selected = %+v, want selected ids in request order without duplicates", selected)
	}
}

func TestSelectSetPartStatePartsRejectsUnknownStatus(t *testing.T) {
	_, err := selectSetPartStateParts([]state.Part{
		{PartID: "part-1", Status: state.StatusCompacting},
	}, setPartStateSelection{Status: state.Status("NOT_A_STATUS")})
	if err == nil {
		t.Fatal("expected unknown status error")
	}
}

func TestSelectFinalizeCompactionPartsByOutputPartID(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusCompacting, CompactOutputPartID: "compact-out"},
		{PartID: "part-2", Status: state.StatusCompacting, CompactOutputPartID: "compact-out"},
		{PartID: "part-3", Status: state.StatusCompacting, CompactOutputPartID: "compact-other"},
		{PartID: "part-4", Status: state.StatusCompactReady, CompactOutputPartID: "compact-out"},
	}

	selected, err := selectFinalizeCompactionParts(parts, finalizeCompactionSelection{OutputPartID: "compact-out"})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].PartID != "part-1" || selected[1].PartID != "part-2" {
		t.Fatalf("selected = %+v, want compact-out compacting rows", selected)
	}
}

func TestSelectFinalizeCompactionPartsRejectsNonCompactingPartID(t *testing.T) {
	_, err := selectFinalizeCompactionParts([]state.Part{
		{PartID: "part-1", Status: state.StatusCompactReady},
	}, finalizeCompactionSelection{PartIDs: []string{"part-1"}})
	if err == nil {
		t.Fatal("expected non-compacting part error")
	}
}

func TestAdminSettableStatusRejectsWorkerOwnedStates(t *testing.T) {
	for _, status := range []state.Status{state.StatusReady, state.StatusCompactReady, state.StatusFinished} {
		if !adminSettableStatus(status) {
			t.Fatalf("expected %s to be admin settable", status)
		}
	}
	for _, status := range []state.Status{state.StatusInProgress, state.StatusCompacting, state.StatusImporting, state.StatusImported, state.StatusSuperseded, state.StatusFailed} {
		if adminSettableStatus(status) {
			t.Fatalf("expected %s to be rejected as admin settable", status)
		}
	}
}

func TestSelectImportFinishedParts(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusFinished},
		{PartID: "part-2", Status: state.StatusFinished},
	}

	all, err := selectImportFinishedParts(parts, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all parts len = %d, want 2", len(all))
	}

	one, err := selectImportFinishedParts(parts, "part-2")
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].PartID != "part-2" {
		t.Fatalf("selected part = %+v, want part-2", one)
	}

	if _, err := selectImportFinishedParts(parts, "part-missing"); err == nil {
		t.Fatal("expected missing part error")
	}
}

func TestBuildResetPlanUsesCompactLineage(t *testing.T) {
	parts := []state.Part{
		resetOriginalPart("part-1", state.StatusSuperseded, "compact-1"),
		resetOriginalPart("part-2", state.StatusSuperseded, "compact-1"),
		resetOriginalPart("part-3", state.StatusSuperseded, "compact-2"),
		resetCompactPart("compact-1", state.StatusSuperseded, []string{"part-1", "part-2"}, "compact-2", 1),
		resetCompactPart("compact-2", state.StatusCompactReady, []string{"compact-1", "part-3"}, "", 2),
	}

	plan, err := buildResetPlan("job-1", parts, resetModeJob)
	if err != nil {
		t.Fatal(err)
	}
	if plan.TargetStatus != state.StatusReady {
		t.Fatalf("target status = %s, want READY", plan.TargetStatus)
	}
	if got := partIDs(plan.OriginalParts); strings.Join(got, ",") != "part-1,part-2,part-3" {
		t.Fatalf("originals = %v", got)
	}
	if got := partIDs(plan.GeneratedParts); strings.Join(got, ",") != "compact-1,compact-2" {
		t.Fatalf("generated = %v", got)
	}

	prefixes := resetS3Prefixes(plan)
	if got := resetPrefixStrings(prefixes); strings.Join(got, ",") != "bucket:finished/compact-1,bucket:finished/compact-2,bucket:finished/part-1,bucket:finished/part-2,bucket:finished/part-3" {
		t.Fatalf("reset-job prefixes = %v", got)
	}
}

func TestBuildResetCompactionPlanPreservesRewrittenOriginals(t *testing.T) {
	parts := []state.Part{
		resetOriginalPart("part-1", state.StatusSuperseded, "compact-1"),
		resetOriginalPart("part-2", state.StatusSuperseded, "compact-1"),
		resetCompactPart("compact-1", state.StatusCompactReady, []string{"part-1", "part-2"}, "", 1),
	}

	plan, err := buildResetPlan("job-1", parts, resetModeCompaction)
	if err != nil {
		t.Fatal(err)
	}
	if plan.TargetStatus != state.StatusCompactReady {
		t.Fatalf("target status = %s, want COMPACT_READY", plan.TargetStatus)
	}
	if got := partIDs(plan.OriginalParts); strings.Join(got, ",") != "part-1,part-2" {
		t.Fatalf("originals = %v", got)
	}
	if got := resetPrefixStrings(resetS3Prefixes(plan)); strings.Join(got, ",") != "bucket:finished/compact-1" {
		t.Fatalf("reset-compaction prefixes = %v", got)
	}
}

func TestBuildResetPlanRejectsImportedParts(t *testing.T) {
	_, err := buildResetPlan("job-1", []state.Part{
		resetOriginalPart("part-1", state.StatusImported, ""),
	}, resetModeJob)
	if err == nil {
		t.Fatal("expected imported part error")
	}
}

func TestBuildResetPlanRejectsFailedImportParts(t *testing.T) {
	part := resetOriginalPart("part-1", state.StatusFailed, "")
	part.ImportingAt = "2026-06-24T00:00:00.000000000Z"
	_, err := buildResetPlan("job-1", []state.Part{part}, resetModeJob)
	if err == nil {
		t.Fatal("expected failed import part error")
	}
}

func TestBuildResetPlanRejectsMissingCompactInput(t *testing.T) {
	_, err := buildResetPlan("job-1", []state.Part{
		resetOriginalPart("part-1", state.StatusCompactReady, ""),
		resetCompactPart("compact-1", state.StatusCompactReady, []string{"part-missing"}, "", 1),
	}, resetModeJob)
	if err == nil {
		t.Fatal("expected missing input error")
	}
}

func TestBuildResetPlanRejectsSupersededMismatch(t *testing.T) {
	_, err := buildResetPlan("job-1", []state.Part{
		resetOriginalPart("part-1", state.StatusSuperseded, "compact-other"),
		resetCompactPart("compact-1", state.StatusCompactReady, []string{"part-1"}, "", 1),
		resetCompactPart("compact-other", state.StatusCompactReady, []string{"part-2"}, "", 1),
		resetOriginalPart("part-2", state.StatusSuperseded, "compact-other"),
	}, resetModeJob)
	if err == nil {
		t.Fatal("expected superseded mismatch error")
	}
}

func TestBuildResetCompactionRejectsOriginalWithoutRewriteMetadata(t *testing.T) {
	part := resetOriginalPart("part-1", state.StatusReady, "")
	part.DestinationDatabase = ""
	_, err := buildResetPlan("job-1", []state.Part{part}, resetModeCompaction)
	if err == nil {
		t.Fatal("expected missing rewrite metadata error")
	}
}

func TestBuildResetPlanAllowsMissingSupersededByOutput(t *testing.T) {
	plan, err := buildResetPlan("job-1", []state.Part{
		resetOriginalPart("part-1", state.StatusSuperseded, "compact-already-deleted"),
	}, resetModeJob)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.OriginalParts) != 1 || plan.OriginalParts[0].PartID != "part-1" {
		t.Fatalf("originals = %+v", plan.OriginalParts)
	}
}

func TestFinalizableCompactReadyPartsRequiresNoActiveWork(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	compactReady := state.Part{
		PartID:    "part-compact",
		Status:    state.StatusCompactReady,
		UpdatedAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
	}

	_, ok, err := finalizableCompactReadyParts([]state.Part{
		compactReady,
		{PartID: "part-ready", Status: state.StatusReady},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected active source work to block finalization")
	}

	selected, ok, err := finalizableCompactReadyParts([]state.Part{compactReady}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(selected) != 1 || selected[0].PartID != compactReady.PartID {
		t.Fatalf("selected = %+v, ok=%t; want compact-ready part", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsWaitsForThreshold(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	_, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:    "part-compact",
			Status:    state.StatusCompactReady,
			UpdatedAt: now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
		},
	}, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected recent compact-ready part to wait for threshold")
	}
}

func TestFinalizableCompactReadyPartsUsesStableCompactReadyTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:         "part-compact",
			Status:         state.StatusCompactReady,
			CompactReadyAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
			UpdatedAt:      now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(selected) != 1 {
		t.Fatalf("selected = %+v, ok=%t; want finalized compact-ready part", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsFinalizesSingleUnmergeablePart(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:                     "part-compact",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(selected) != 1 || selected[0].PartID != "part-compact" {
		t.Fatalf("selected = %+v, ok=%t; want single unmergeable part finalized", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsDoesNotShortcutMultiPartOutput(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:                     "part-compact",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok || len(selected) != 0 {
		t.Fatalf("selected = %+v, ok=%t; want compact window wait", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsFinalizesIsolatedPartitionWithOtherCurrentOutput(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:                     "part-compact",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
		{
			PartID:                     "part-finished",
			Status:                     state.StatusFinished,
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202607": 1,
			},
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(selected) != 1 || selected[0].PartID != "part-compact" {
		t.Fatalf("selected = %+v, ok=%t; want isolated compact-ready part finalized", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsFinalizesIsolatedPartitionWithOtherCompactingPartition(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:                     "part-compact",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
		{
			PartID:                     "part-compacting",
			Status:                     state.StatusCompacting,
			DestinationActivePartCount: 4,
			DestinationActivePartitionCounts: map[string]uint64{
				"202607": 4,
			},
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(selected) != 1 || selected[0].PartID != "part-compact" {
		t.Fatalf("selected = %+v, ok=%t; want isolated compact-ready part finalized", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsWaitsWhenCompactingPartitionOverlaps(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:                     "part-compact",
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		},
		{
			PartID:                     "part-compacting",
			Status:                     state.StatusCompacting,
			DestinationActivePartCount: 4,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 4,
			},
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok || len(selected) != 0 {
		t.Fatalf("selected = %+v, ok=%t; want overlapping compacting partition to block", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsWaitsWhenReadyPartitionOverlaps(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := func(partID string) state.Part {
		return state.Part{
			PartID:                     partID,
			Status:                     state.StatusCompactReady,
			CompactReadyAt:             now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			DestinationActivePartCount: 1,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 1,
			},
		}
	}
	selected, ok, err := finalizableCompactReadyParts([]state.Part{part("part-1"), part("part-2")}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok || len(selected) != 0 {
		t.Fatalf("selected = %+v, ok=%t; want overlapping compact-ready partition to wait", selected, ok)
	}
}

func TestFinalizableCompactReadyPartsUsesCurrentCompactReadyRowTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected, ok, err := finalizableCompactReadyParts([]state.Part{
		{
			PartID:         "part-original",
			Status:         state.StatusSuperseded,
			CompactReadyAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		},
		{
			PartID:         "part-fresh",
			Status:         state.StatusCompactReady,
			CompactReadyAt: now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if ok || len(selected) != 0 {
		t.Fatalf("selected = %+v, ok=%t; want fresh compact-ready row to keep compaction window open", selected, ok)
	}
}

func TestCompactWindowExpiredIgnoresJobsWithoutCompactPhase(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	expired, err := compactWindowExpired([]state.Part{
		{
			PartID:    "part-ready",
			Status:    state.StatusReady,
			UpdatedAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if expired {
		t.Fatal("expected job without compact phase to be treated as not expired")
	}
}

func TestCompactWindowExpiredUsesCurrentCompactReadyRowTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	expired, err := compactWindowExpired([]state.Part{
		{
			PartID:         "part-original",
			Status:         state.StatusSuperseded,
			CompactReadyAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		},
		{
			PartID:         "part-fresh",
			Status:         state.StatusCompactReady,
			CompactReadyAt: now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
		},
	}, 2*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if expired {
		t.Fatal("expected fresh compact-ready row to keep job compact window open")
	}
}

func TestCompactClaimSplayMaxUsesSmallFixedDelay(t *testing.T) {
	if got := compactClaimSplayMax(2 * time.Hour); got != 250*time.Millisecond {
		t.Fatalf("compactClaimSplayMax(2h) = %s, want 250ms", got)
	}
	if got := compactClaimSplayMax(0); got != 0 {
		t.Fatalf("compactClaimSplayMax(0) = %s, want 0", got)
	}
	if got := compactClaimSplayMax(time.Minute); got != 250*time.Millisecond {
		t.Fatalf("compactClaimSplayMax(1m) = %s, want 250ms", got)
	}
}

func TestCompactOutputReadyAtUsesLatestInputReadyTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	got, err := compactOutputReadyAt([]state.Part{
		{
			PartID:         "part-old",
			Status:         state.StatusCompacting,
			CompactReadyAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
		},
		{
			PartID:         "part-new",
			Status:         state.StatusCompacting,
			CompactReadyAt: now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(-2 * time.Hour)
	if !got.Equal(want) {
		t.Fatalf("compactOutputReadyAt = %s, want %s", got, want)
	}
}

func TestCompactBatchDeadlineUsesCompactWindow(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	deadline, err := compactBatchDeadline([]state.Part{
		{
			PartID:         "part-1",
			Status:         state.StatusCompacting,
			CompactReadyAt: now.Add(-3 * time.Hour).Format(time.RFC3339Nano),
			UpdatedAt:      now.Format(time.RFC3339Nano),
		},
		{
			PartID:         "part-2",
			Status:         state.StatusCompacting,
			CompactReadyAt: now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
			UpdatedAt:      now.Format(time.RFC3339Nano),
		},
	}, 24*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Add(23*time.Hour + 30*time.Minute); !deadline.Equal(want) {
		t.Fatalf("deadline = %s, want %s", deadline, want)
	}
}

func TestCompactBatchDeadlineDisabledForZeroWindow(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	deadline, err := compactBatchDeadline([]state.Part{
		{
			PartID:         "part-1",
			Status:         state.StatusCompacting,
			CompactReadyAt: now.Add(-time.Hour).Format(time.RFC3339Nano),
		},
	}, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if !deadline.IsZero() {
		t.Fatalf("deadline = %s, want zero", deadline)
	}
}

func TestCompactLeaseTimingDerivedFromCompactWindow(t *testing.T) {
	staleAfter := compactLeaseStaleAfter(2 * time.Hour)
	if staleAfter != 2*time.Hour {
		t.Fatalf("compactLeaseStaleAfter = %s, want 2h", staleAfter)
	}
	if got := compactLeaseHeartbeatInterval(staleAfter); got != 5*time.Minute {
		t.Fatalf("compactLeaseHeartbeatInterval = %s, want 5m cap", got)
	}
	if got := compactLeaseStaleAfter(time.Minute); got != 5*time.Minute {
		t.Fatalf("compactLeaseStaleAfter short window = %s, want 5m floor", got)
	}
	if got := compactLeaseStaleAfter(0); got != 5*time.Minute {
		t.Fatalf("compactLeaseStaleAfter zero window = %s, want 5m floor", got)
	}
}

func TestDerivedMergeSettleMinWait(t *testing.T) {
	if got := derivedMergeSettleMinWait(5*time.Minute, time.Minute); got != time.Minute {
		t.Fatalf("derivedMergeSettleMinWait(5m, 1m) = %s, want 1m", got)
	}
	if got := derivedMergeSettleMinWait(15*time.Second, 2*time.Minute); got != 3750*time.Millisecond {
		t.Fatalf("derivedMergeSettleMinWait(15s, 2m) = %s, want 3.75s", got)
	}
	if got := derivedMergeSettleMinWait(0, time.Minute); got != 0 {
		t.Fatalf("derivedMergeSettleMinWait(0, 1m) = %s, want 0", got)
	}
}

func TestSourceMergeWaitTimeoutsCapWhenCompactEnabled(t *testing.T) {
	idleTimeout, maxRuntime := sourceMergeWaitTimeouts(time.Hour, 2*time.Hour, true)
	if idleTimeout != compactSourceMergeWaitCap || maxRuntime != compactSourceMergeWaitCap {
		t.Fatalf("source merge timeouts = %s/%s, want %s/%s", idleTimeout, maxRuntime, compactSourceMergeWaitCap, compactSourceMergeWaitCap)
	}

	idleTimeout, maxRuntime = sourceMergeWaitTimeouts(time.Hour, 2*time.Hour, false)
	if idleTimeout != time.Hour || maxRuntime != 2*time.Hour {
		t.Fatalf("source merge timeouts without compact = %s/%s, want 1h/2h", idleTimeout, maxRuntime)
	}

	idleTimeout, maxRuntime = sourceMergeWaitTimeouts(time.Minute, 30*time.Second, true)
	if idleTimeout != time.Minute || maxRuntime != time.Minute {
		t.Fatalf("source merge timeouts with short max = %s/%s, want 1m/1m", idleTimeout, maxRuntime)
	}
}

func TestParseWorkerRole(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want workerRole
	}{
		{name: "all", in: "all", want: workerRoleAll},
		{name: "inserter", in: "inserter", want: workerRoleInserter},
		{name: "compactor", in: "compactor", want: workerRoleCompactor},
		{name: "trim case", in: " Compactor ", want: workerRoleCompactor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseWorkerRole(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("parseWorkerRole(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}

	if _, err := parseWorkerRole("writer"); err == nil {
		t.Fatal("expected invalid worker role to fail")
	}
}

func TestWorkerSettingsForRole(t *testing.T) {
	tests := []struct {
		name    string
		role    workerRole
		compact bool
		want    workerRoleSettings
	}{
		{
			name:    "all compacts opportunistically",
			role:    workerRoleAll,
			compact: true,
			want: workerRoleSettings{
				Insert:                true,
				Compact:               true,
				SourceMergeCompactCap: true,
			},
		},
		{
			name:    "all with compact disabled only inserts",
			role:    workerRoleAll,
			compact: false,
			want: workerRoleSettings{
				Insert:                true,
				Compact:               false,
				SourceMergeCompactCap: false,
			},
		},
		{
			name:    "inserter never claims compact work",
			role:    workerRoleInserter,
			compact: true,
			want: workerRoleSettings{
				Insert:                true,
				Compact:               false,
				SourceMergeCompactCap: true,
			},
		},
		{
			name:    "compactor never claims ready work",
			role:    workerRoleCompactor,
			compact: true,
			want: workerRoleSettings{
				Insert:                false,
				Compact:               true,
				SourceMergeCompactCap: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := workerSettingsForRole(tt.role, tt.compact)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("workerSettingsForRole(%q, %v) = %+v, want %+v", tt.role, tt.compact, got, tt.want)
			}
		})
	}

	if _, err := workerSettingsForRole(workerRoleCompactor, false); err == nil {
		t.Fatal("expected compactor role with compact disabled to fail")
	}
}

func TestParseFlagsIgnoresUnknownFlags(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	known := fs.String("known", "", "")
	enabled := fs.Bool("enabled", false, "")

	err := parseFlags(fs, []string{"-unknown", "discarded", "-known", "value", "--old-bool", "-enabled", "--inline=ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if *known != "value" {
		t.Fatalf("known = %q, want value", *known)
	}
	if !*enabled {
		t.Fatal("expected enabled flag to be parsed")
	}
}

func resetOriginalPart(partID string, status state.Status, supersededBy string) state.Part {
	return state.Part{
		JobID:                            "job-1",
		PartID:                           partID,
		Status:                           status,
		Bucket:                           "bucket",
		SourceKey:                        "source/" + partID,
		FinishedKey:                      "finished/" + partID,
		UpdatedAt:                        "2026-06-24T00:00:00.000000000Z",
		SupersededBy:                     supersededBy,
		DestinationDatabase:              "db",
		DestinationTable:                 "table",
		DestinationSchema:                "CREATE TABLE db.table",
		DestinationActivePartCount:       1,
		DestinationActivePartitionCounts: map[string]uint64{"p": 1},
	}
}

func resetCompactPart(partID string, status state.Status, inputs []string, supersededBy string, generation int) state.Part {
	return state.Part{
		JobID:                            "job-1",
		PartID:                           partID,
		Status:                           status,
		Bucket:                           "bucket",
		SourceKey:                        "finished/" + partID,
		FinishedKey:                      "finished/" + partID,
		UpdatedAt:                        "2026-06-24T00:00:00.000000000Z",
		CompactGeneration:                generation,
		CompactInputPartIDs:              inputs,
		SupersededBy:                     supersededBy,
		DestinationDatabase:              "db",
		DestinationTable:                 "table",
		DestinationSchema:                "CREATE TABLE db.table",
		DestinationActivePartCount:       1,
		DestinationActivePartitionCounts: map[string]uint64{"p": 1},
	}
}

func partIDs(parts []state.Part) []string {
	ids := make([]string, 0, len(parts))
	for _, part := range parts {
		ids = append(ids, part.PartID)
	}
	return ids
}

func TestJobStatusVisiblePartsHidesSupersededByDefault(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusSuperseded},
		{PartID: "part-2", Status: state.StatusCompactReady},
		{PartID: "part-3", Status: state.StatusFinished},
	}

	if got := strings.Join(partIDs(jobStatusVisibleParts(parts, false)), ","); got != "part-2,part-3" {
		t.Fatalf("visible parts = %s, want part-2,part-3", got)
	}
	if got := strings.Join(partIDs(jobStatusVisibleParts(parts, true)), ","); got != "part-1,part-2,part-3" {
		t.Fatalf("visible parts with all = %s, want every part", got)
	}
}

func resetPrefixStrings(prefixes []jobS3Prefix) []string {
	out := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		out = append(out, prefix.Bucket+":"+prefix.Prefix)
	}
	return out
}

func TestSelectRetryPartsStale(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	parts := []state.Part{
		{PartID: "part-failed", Status: state.StatusFailed},
		{PartID: "part-fresh", Status: state.StatusInProgress, ProgressUpdatedAt: now.Add(-2 * time.Minute).Format(time.RFC3339Nano)},
		{PartID: "part-stale", Status: state.StatusInProgress, ProgressUpdatedAt: now.Add(-6 * time.Minute).Format(time.RFC3339Nano)},
		{PartID: "part-empty", Status: state.StatusInProgress},
		{PartID: "part-exact", Status: state.StatusInProgress, ProgressUpdatedAt: now.Add(-5 * time.Minute).Format(time.RFC3339Nano)},
	}

	selected, err := selectRetryParts(parts, retryPartSelection{
		Stale:      true,
		StaleAfter: 5 * time.Minute,
		Now:        now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].PartID != "part-stale" {
		t.Fatalf("stale selected = %+v", selected)
	}
}

func TestSelectRetryPartsStaleRejectsMalformedProgressTime(t *testing.T) {
	_, err := selectRetryParts([]state.Part{
		{PartID: "part-bad", Status: state.StatusInProgress, ProgressUpdatedAt: "not-time"},
	}, retryPartSelection{
		Stale:      true,
		StaleAfter: 5 * time.Minute,
		Now:        time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("expected malformed progress_updated_at error")
	}
}

func TestJobS3Prefixes(t *testing.T) {
	prefixes, err := jobS3Prefixes("job-1", []state.Part{
		{
			JobID:       "job-1",
			PartID:      "part-1",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-1/source/part-1",
			FinishedKey: "partforge/jobs/job-1/finished/part-1",
		},
		{
			JobID:       "job-1",
			PartID:      "part-2",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-1/source/part-2",
			FinishedKey: "partforge/jobs/job-1/finished/part-2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixes) != 1 {
		t.Fatalf("prefixes = %+v", prefixes)
	}
	if prefixes[0].Bucket != "bucket" || prefixes[0].Prefix != "partforge/jobs/job-1" {
		t.Fatalf("prefix = %+v", prefixes[0])
	}
}

func TestJobS3PrefixesRejectsWrongJobKey(t *testing.T) {
	_, err := jobS3Prefixes("job-1", []state.Part{
		{
			JobID:       "job-1",
			PartID:      "part-1",
			Bucket:      "bucket",
			SourceKey:   "partforge/jobs/job-10/source/part-1",
			FinishedKey: "partforge/jobs/job-10/finished/part-1",
		},
	})
	if err == nil {
		t.Fatal("expected wrong job key error")
	}
}

func TestStateProgress(t *testing.T) {
	query := metrics.QueryProgress{ReadRows: 1, ReadBytes: 2, WrittenRows: 3, WrittenBytes: 4}
	source := metrics.PartStats{Count: 5, Rows: 6, Bytes: 7}
	dest := metrics.PartStats{Count: 8, Rows: 9, Bytes: 10}
	failedMerges := uint64(11)
	stageStartedAt := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)

	progress := stateProgress(rewrite.ProgressSnapshot{
		QueryProgress:              &query,
		SourceActivePartStats:      &source,
		DestinationActivePartStats: &dest,
		DestinationFailedMerges:    &failedMerges,
		StageProgress: &rewrite.StageProgress{
			Stage:          "download_source",
			StageStartedAt: stageStartedAt,
			StageElapsed:   1500 * time.Millisecond,
			TotalElapsed:   2500 * time.Millisecond,
			CompletedStageDurations: map[string]time.Duration{
				"process_part": time.Second,
			},
		},
	})

	if progress.QueryProgress == nil || progress.QueryProgress.WrittenBytes != 4 {
		t.Fatalf("query progress = %+v", progress.QueryProgress)
	}
	if progress.SourceActivePartStats == nil || progress.SourceActivePartStats.Rows != 6 {
		t.Fatalf("source stats = %+v", progress.SourceActivePartStats)
	}
	if progress.DestinationActivePartStats == nil || progress.DestinationActivePartStats.Bytes != 10 {
		t.Fatalf("destination stats = %+v", progress.DestinationActivePartStats)
	}
	if progress.DestinationFailedMerges == nil || *progress.DestinationFailedMerges != failedMerges {
		t.Fatalf("destination failed merges = %+v", progress.DestinationFailedMerges)
	}
	if progress.StageProgress == nil || progress.StageProgress.Stage != "download_source" {
		t.Fatalf("stage progress = %+v", progress.StageProgress)
	}
	if progress.StageProgress.StageStartedAt != stageStartedAt {
		t.Fatalf("stage started at = %s, want %s", progress.StageProgress.StageStartedAt, stageStartedAt)
	}
	if progress.StageProgress.StageElapsedMs != 1500 || progress.StageProgress.TotalElapsedMs != 2500 {
		t.Fatalf("stage durations = %+v", progress.StageProgress)
	}
	if progress.StageProgress.CompletedStageDurationsMs["process_part"] != 1000 {
		t.Fatalf("completed stage durations = %+v", progress.StageProgress.CompletedStageDurationsMs)
	}
}

func TestFormatStageDurations(t *testing.T) {
	got := formatStageDurations(map[string]int64{
		"b": 2000,
		"a": 1500,
	})
	if got != "a=1.5s,b=2s" {
		t.Fatalf("formatStageDurations = %q", got)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{bytes: 0, want: "0 B"},
		{bytes: 300, want: "300 B"},
		{bytes: 1536, want: "1.5 KB"},
		{bytes: 10 * 1024 * 1024, want: "10 MB"},
		{bytes: 5*1024*1024*1024 + 512*1024*1024, want: "5.5 GB"},
	}

	for _, tt := range tests {
		if got := formatBytes(tt.bytes); got != tt.want {
			t.Fatalf("formatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestFormatByteRate(t *testing.T) {
	tests := []struct {
		bytesPerSecond float64
		want           string
	}{
		{bytesPerSecond: 0, want: "0 B/s"},
		{bytesPerSecond: 1536, want: "1.5 KB/s"},
		{bytesPerSecond: 10 * 1024 * 1024, want: "10 MB/s"},
		{bytesPerSecond: 1.5 * 1024 * 1024 * 1024, want: "1.5 GB/s"},
	}

	for _, tt := range tests {
		if got := formatByteRate(tt.bytesPerSecond); got != tt.want {
			t.Fatalf("formatByteRate(%f) = %q, want %q", tt.bytesPerSecond, got, tt.want)
		}
	}
}

func TestHumanizeLogAttrHumanizesByteFields(t *testing.T) {
	tests := []struct {
		attr slog.Attr
		want string
	}{
		{attr: slog.Uint64("uploaded_bytes", 10*1024*1024), want: "10 MB"},
		{attr: slog.Float64("overall_bytes_per_second", 1.5*1024*1024), want: "1.5 MB/s"},
		{attr: slog.String("max_memory_usage", "1073741824"), want: "1 GB"},
		{attr: slog.Int("bytes", 1536), want: "1.5 KB"},
	}

	for _, tt := range tests {
		got := humanizeLogAttr(nil, tt.attr)
		if got.Value.String() != tt.want {
			t.Fatalf("humanizeLogAttr(%s) = %q, want %q", tt.attr.Key, got.Value.String(), tt.want)
		}
	}
}

func TestHumanizeLogAttrLeavesOtherNumbersRaw(t *testing.T) {
	got := humanizeLogAttr(nil, slog.Int("attempt", 7))
	if got.Value.Kind() != slog.KindInt64 || got.Value.Int64() != 7 {
		t.Fatalf("humanizeLogAttr changed non-byte number: %+v", got)
	}
}

func TestPrintJobSummaryHumanizesBytes(t *testing.T) {
	summary := jobSummary{
		JobID:        "job-1",
		Status:       "READY",
		Total:        1,
		Counts:       map[state.Status]int{state.StatusReady: 1},
		ReadRows:     1000,
		ReadBytes:    10 * 1024 * 1024,
		WrittenRows:  900,
		WrittenBytes: 3 * 1024 * 1024 * 1024,
		FailedMerges: 4,
	}

	got := captureFileOutput(t, func(out *os.File) {
		printJobSummary(out, summary)
	})

	for _, want := range []string{
		"read: 1000 rows 10 MB",
		"written: 900 rows 3 GB",
		"failed_merges: 4",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printJobSummary output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "10485760 bytes") || strings.Contains(got, "3221225472 bytes") {
		t.Fatalf("printJobSummary output contains raw byte values:\n%s", got)
	}
}

func TestPrintJobSummaryIncludesInProgressStages(t *testing.T) {
	summary := jobSummary{
		JobID:  "job-1",
		Status: "REWRITING",
		Total:  3,
		Counts: map[state.Status]int{
			state.StatusInProgress: 3,
		},
		InProgressStages: []inProgressStageCount{
			{Stage: "insert_select", Count: 2},
			{Stage: "wait_merges", Count: 1},
		},
	}

	got := captureFileOutput(t, func(out *os.File) {
		printJobSummary(out, summary)
	})

	for _, want := range []string{
		"IN_PROGRESS STAGES",
		"insert_select",
		"wait_merges",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printJobSummary output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "INPUT_CH_PARTS") || strings.Contains(got, "OUTPUT_CH_PARTS") {
		t.Fatalf("printJobSummary output should not include detailed part counters:\n%s", got)
	}
}

func TestPrintJobSummaryIncludesCompactETA(t *testing.T) {
	summary := jobSummary{
		JobID:  "job-1",
		Status: "COMPACTING",
		Total:  2,
		Counts: map[state.Status]int{
			state.StatusCompactReady: 1,
			state.StatusCompacting:   1,
		},
		Compact: &compactJobSummary{
			ReadyParts:       1,
			CompactingParts:  1,
			Window:           "2h0m0s",
			FinalizeStatus:   "blocked",
			FinalizeAfter:    "2026-06-24T13:30:00Z",
			FinalizeIn:       "30m0s",
			BlockedByMessage: "COMPACTING=1",
		},
	}

	got := captureFileOutput(t, func(out *os.File) {
		printJobSummary(out, summary)
	})

	for _, want := range []string{
		"compact: ready=1 compacting=1 window=2h0m0s",
		"compact_finalize: blocked by COMPACTING=1; eligible after 2026-06-24T13:30:00Z (in 30m0s)",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printJobSummary output missing %q:\n%s", want, got)
		}
	}
}

func TestPrintPartRowsHumanizesBytes(t *testing.T) {
	got := captureFileOutput(t, func(out *os.File) {
		printPartRows(out, []state.Part{
			{
				PartID:                     "part-1",
				Status:                     state.StatusFinished,
				ReadRows:                   100,
				ReadBytes:                  1536,
				WrittenRows:                90,
				WrittenBytes:               2 * 1024 * 1024,
				DestinationActivePartCount: 3,
				DestinationFailedMerges:    4,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 3,
				},
				RewriteStageDurationsMs: map[string]int64{
					"wait_merges": 65_000,
				},
			},
		})
	})

	for _, want := range []string{
		"READ_SIZE",
		"WRITTEN_SIZE",
		"OUTPUT_CH_PARTS",
		"PARTITIONS",
		"FAILED_MERGES",
		"SETTLE_WAIT",
		"202606",
		"1.5 KB",
		"2 MB",
		"4",
		"3",
		"1m5s",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printPartRows output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "READ_BYTES") || strings.Contains(got, "WRITTEN_BYTES") {
		t.Fatalf("printPartRows output still uses raw byte headers:\n%s", got)
	}
}

func TestPrintPartRowsUsesHiddenSupersededInputsForCounts(t *testing.T) {
	displayParts := []state.Part{
		{
			PartID:                     "compact-1",
			Status:                     state.StatusCompactReady,
			WorkerID:                   "worker",
			ReadRows:                   11,
			ReadBytes:                  12 * 1024,
			WrittenRows:                13,
			WrittenBytes:               14 * 1024,
			SourceActivePartRows:       15,
			DestinationActivePartRows:  16,
			DestinationActivePartCount: 2,
			CompactInputPartIDs:        []string{"part-1", "part-2"},
		},
	}
	allParts := []state.Part{
		{PartID: "part-1", Status: state.StatusSuperseded, DestinationActivePartCount: 4},
		{PartID: "part-2", Status: state.StatusSuperseded, DestinationActivePartCount: 3},
		displayParts[0],
	}

	got := captureFileOutput(t, func(out *os.File) {
		printPartRowsWithLookup(out, displayParts, allParts)
	})

	want := regexp.MustCompile(`compact-1\s+COMPACT_READY\s+0\s+worker\s+11\s+12\s+KB\s+13\s+14\s+KB\s+15\s+16\s+7\s+2`)
	if !want.MatchString(got) {
		t.Fatalf("printPartRows output missing hidden input counts:\n%s", got)
	}
}

func TestPrintPartRowsShowsMultiplePartitions(t *testing.T) {
	got := captureFileOutput(t, func(out *os.File) {
		printPartRows(out, []state.Part{
			{
				PartID:                     "part-1",
				Status:                     state.StatusCompacting,
				DestinationActivePartCount: 3,
				DestinationActivePartitionCounts: map[string]uint64{
					"202607": 1,
					"202606": 2,
				},
			},
		})
	})

	for _, want := range []string{
		"PARTITIONS",
		"202606:2,202607:1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("printPartRows output missing %q:\n%s", want, got)
		}
	}
}

func TestPrintPartRowsUsesCurrentWaitMergesElapsed(t *testing.T) {
	got := captureFileOutput(t, func(out *os.File) {
		printPartRows(out, []state.Part{
			{
				PartID:                "part-1",
				Status:                state.StatusInProgress,
				RewriteStage:          "wait_merges",
				RewriteStageElapsedMs: 125_000,
			},
		})
	})

	if !strings.Contains(got, "2m5s") {
		t.Fatalf("printPartRows output missing live settle wait:\n%s", got)
	}
}

func TestUploadPartsInParallelProcessesEveryTask(t *testing.T) {
	tasks := []uploadPartTask{
		{Index: 1, SourcePart: freeze.Part{Name: "part-1"}},
		{Index: 2, SourcePart: freeze.Part{Name: "part-2"}},
		{Index: 3, SourcePart: freeze.Part{Name: "part-3"}},
		{Index: 4, SourcePart: freeze.Part{Name: "part-4"}},
		{Index: 5, SourcePart: freeze.Part{Name: "part-5"}},
	}

	var active int64
	var maxActive int64
	seen := map[int]bool{}
	var seenMu sync.Mutex

	err := uploadPartsInParallel(context.Background(), tasks, 2, func(ctx context.Context, workerID int, task uploadPartTask) (uploadPartResult, error) {
		current := atomic.AddInt64(&active, 1)
		for {
			max := atomic.LoadInt64(&maxActive)
			if current <= max || atomic.CompareAndSwapInt64(&maxActive, max, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&active, -1)
		return uploadPartResult{Index: task.Index, SourcePart: task.SourcePart}, nil
	}, func(result uploadPartResult) {
		seenMu.Lock()
		defer seenMu.Unlock()
		seen[result.Index] = true
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(tasks) {
		t.Fatalf("processed %d tasks, want %d", len(seen), len(tasks))
	}
	if max := atomic.LoadInt64(&maxActive); max > 2 {
		t.Fatalf("max active uploads = %d, want <= 2", max)
	}
}

func TestUploadPartsInParallelCancelsOnError(t *testing.T) {
	boom := errors.New("boom")
	tasks := []uploadPartTask{
		{Index: 1},
		{Index: 2},
		{Index: 3},
	}

	err := uploadPartsInParallel(context.Background(), tasks, 2, func(ctx context.Context, workerID int, task uploadPartTask) (uploadPartResult, error) {
		if task.Index == 1 {
			return uploadPartResult{}, boom
		}
		<-ctx.Done()
		return uploadPartResult{}, ctx.Err()
	}, nil)
	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want %v", err, boom)
	}
}

func TestResolveS5cmdNumWorkers(t *testing.T) {
	tests := []struct {
		name              string
		configured        int
		uploadConcurrency int
		want              int
	}{
		{name: "explicit", configured: 17, uploadConcurrency: 4, want: 17},
		{name: "auto divides default workers", configured: 0, uploadConcurrency: 4, want: 64},
		{name: "auto clamps concurrency", configured: 0, uploadConcurrency: 0, want: 256},
		{name: "auto clamps workers", configured: 0, uploadConcurrency: 512, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveS5cmdNumWorkers(tt.configured, tt.uploadConcurrency)
			if got != tt.want {
				t.Fatalf("workers = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCreateWorkerRunDirs(t *testing.T) {
	workDir := t.TempDir()
	dirs, err := createWorkerRunDirs(workDir)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(dirs.Root) != workDir {
		t.Fatalf("run dir parent = %q, want %q", filepath.Dir(dirs.Root), workDir)
	}
	if filepath.Base(dirs.ClickHouse) != "clickhouse" || filepath.Dir(dirs.ClickHouse) != dirs.Root {
		t.Fatalf("clickhouse dir = %q, want child of %q", dirs.ClickHouse, dirs.Root)
	}
	if filepath.Base(dirs.Scratch) != "scratch" || filepath.Dir(dirs.Scratch) != dirs.Root {
		t.Fatalf("scratch dir = %q, want child of %q", dirs.Scratch, dirs.Root)
	}
	for _, dir := range []string{dirs.Root, dirs.ClickHouse, dirs.Scratch} {
		if info, err := os.Stat(dir); err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		} else if !info.IsDir() {
			t.Fatalf("%s is not a directory", dir)
		}
	}
}

func TestWorkerProcessContextCancelsImmediatelyOnShutdown(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	processCtx, shutdown := workerProcessContext(shutdownCtx, "job-1", "part-1")
	defer shutdown.Stop()

	cancelShutdown()
	waitForShutdownRequested(t, shutdown)

	select {
	case <-processCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("process context was not canceled")
	}
	if !shutdown.Forced() {
		t.Fatal("shutdown was not marked forced")
	}
}

func TestWorkerProcessContextStopBeforeShutdown(t *testing.T) {
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	processCtx, shutdown := workerProcessContext(shutdownCtx, "job-1", "part-1")
	shutdown.Stop()

	cancelShutdown()
	select {
	case <-processCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("process context was not canceled after worker stopped")
	}
	if shutdown.Forced() {
		t.Fatal("shutdown marked forced after worker stopped")
	}
}

func waitForShutdownRequested(t *testing.T, shutdown workerPartShutdown) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if shutdown.Requested() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("shutdown was not requested")
}

func TestUploadConcurrencyFromCPUs(t *testing.T) {
	got, err := uploadConcurrencyFromCPUs(12)
	if err != nil {
		t.Fatal(err)
	}
	if got != 12 {
		t.Fatalf("concurrency = %d, want 12", got)
	}

	if _, err := uploadConcurrencyFromCPUs(0); err == nil {
		t.Fatal("expected invalid CPU count error")
	}
}

func TestApplyConfigDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeTestFile(t, path, `{
  "bucket": "global-bucket",
  "aws_region": "us-west-2",
  "commands": {
    "upload-freeze": {
      "bucket": "upload-bucket",
      "prefix": "uploads"
    }
  }
}`)

	fs := flag.NewFlagSet("upload-freeze", flag.ContinueOnError)
	bucket := fs.String("bucket", "", "")
	prefix := fs.String("prefix", "partforge", "")
	region := fs.String("aws-region", "", "")
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigDefaults(fs, path, "upload-freeze"); err != nil {
		t.Fatal(err)
	}

	if *bucket != "upload-bucket" {
		t.Fatalf("bucket = %q", *bucket)
	}
	if *prefix != "uploads" {
		t.Fatalf("prefix = %q", *prefix)
	}
	if *region != "us-west-2" {
		t.Fatalf("region = %q", *region)
	}
}

func TestApplyConfigDefaultsDoesNotOverrideCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeTestFile(t, path, `{"bucket": "config-bucket"}`)

	fs := flag.NewFlagSet("upload-freeze", flag.ContinueOnError)
	bucket := fs.String("bucket", "", "")
	if err := fs.Parse([]string{"-bucket=cli-bucket"}); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigDefaults(fs, path, "upload-freeze"); err != nil {
		t.Fatal(err)
	}

	if *bucket != "cli-bucket" {
		t.Fatalf("bucket = %q", *bucket)
	}
}

func TestDestinationTableRefFromSchema(t *testing.T) {
	got, err := destinationTableRefFromSchema(
		"CREATE TABLE posthog.query_log_archive_temp (x UInt64) ENGINE = MergeTree ORDER BY x",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.Database != "posthog" || got.Table != "query_log_archive_temp" {
		t.Fatalf("destination = %+v", got)
	}
}

func TestDestinationTableRefFromSchemaRequiresDatabase(t *testing.T) {
	_, err := destinationTableRefFromSchema(
		"CREATE TABLE query_log_archive_temp (x UInt64) ENGINE = MergeTree ORDER BY x",
	)
	if err == nil {
		t.Fatal("expected unqualified table error")
	}
}

func TestApplyClickHouseClientConfigDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config>
  <user>alice</user>
  <password>secret</password>
</config>`)

	user := ""
	password := ""
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "alice" || password != "secret" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestApplyClickHouseClientConfigDefaultsFillsOnlyMissingCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config>
  <user>alice</user>
  <password>secret</password>
</config>`)

	user := "bob"
	password := ""
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "bob" || password != "secret" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestApplyClickHouseClientConfigDefaultsDoesNotOverrideConfiguredCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config>
  <user>alice</user>
  <password>secret</password>
</config>`)

	user := "bob"
	password := "configured"
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "bob" || password != "configured" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestApplyClickHouseClientConfigDefaultsUsesDefaultUserForPasswordOnlyConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.xml")
	writeTestFile(t, path, `<config><password>secret</password></config>`)

	user := ""
	password := ""
	if err := applyClickHouseClientConfigDefaultsFrom(path, &user, &password); err != nil {
		t.Fatal(err)
	}

	if user != "default" || password != "secret" {
		t.Fatalf("credentials = %q/%q", user, password)
	}
}

func TestBuildClickHousePrometheusTarget(t *testing.T) {
	tests := []struct {
		name  string
		inURL string
		port  int
		path  string
		want  string
	}{
		{
			name:  "localhost",
			inURL: "http://127.0.0.1:8123/?database=default",
			port:  9363,
			path:  "/metrics",
			want:  "http://127.0.0.1:9363/metrics",
		},
		{
			name:  "ipv6",
			inURL: "http://[::1]:8123",
			port:  9363,
			path:  "/metrics",
			want:  "http://[::1]:9363/metrics",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildClickHousePrometheusTarget(tt.inURL, tt.port, tt.path)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("target = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildClickHousePrometheusTargetRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		inURL string
		port  int
		path  string
	}{
		{name: "missing scheme", inURL: "127.0.0.1:8123", port: 9363, path: "/metrics"},
		{name: "missing host", inURL: "http:///query", port: 9363, path: "/metrics"},
		{name: "invalid port", inURL: "http://127.0.0.1:8123", port: 0, path: "/metrics"},
		{name: "invalid path", inURL: "http://127.0.0.1:8123", port: 9363, path: "metrics"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildClickHousePrometheusTarget(tt.inURL, tt.port, tt.path)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func captureFileOutput(t *testing.T, write func(*os.File)) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "output.txt")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	write(out)
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
