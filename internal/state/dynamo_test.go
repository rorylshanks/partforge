package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestResolveDynamoRegionKeepsResolvedRegion(t *testing.T) {
	got := resolveDynamoRegion(
		context.Background(),
		aws.Config{Region: "eu-central-1"},
		defaultRegion,
		func(context.Context, aws.Config) (string, error) {
			t.Fatal("should not call IMDS when a region is already resolved")
			return "", nil
		},
	)

	if got != "eu-central-1" {
		t.Fatalf("region = %q", got)
	}
}

func TestResolveDynamoRegionUsesIMDS(t *testing.T) {
	got := resolveDynamoRegion(
		context.Background(),
		aws.Config{},
		defaultRegion,
		func(context.Context, aws.Config) (string, error) {
			return "eu-central-1", nil
		},
	)

	if got != "eu-central-1" {
		t.Fatalf("region = %q", got)
	}
}

func TestResolveDynamoRegionFallsBackWhenIMDSUnavailable(t *testing.T) {
	got := resolveDynamoRegion(
		context.Background(),
		aws.Config{},
		defaultRegion,
		func(context.Context, aws.Config) (string, error) {
			return "", errors.New("imds unavailable")
		},
	)

	if got != defaultRegion {
		t.Fatalf("region = %q", got)
	}
}

func TestProgressRemoveExpressionCoversRewriteMetadata(t *testing.T) {
	expr := progressRemoveExpression()
	for _, attr := range []string{
		"progress_updated_at",
		"read_rows",
		"read_bytes",
		"written_rows",
		"written_bytes",
		"source_active_part_count",
		"source_active_part_rows",
		"source_active_part_bytes",
		"destination_active_part_count",
		"destination_active_part_rows",
		"destination_active_part_bytes",
		"destination_active_partition_counts",
		"destination_failed_merges",
		"rewrite_stage",
		"rewrite_stage_started_at",
		"rewrite_stage_elapsed_ms",
		"rewrite_total_elapsed_ms",
		"rewrite_stage_durations_ms",
	} {
		if !strings.Contains(expr, attr) {
			t.Fatalf("progress remove expression %q missing %s", expr, attr)
		}
	}
}

func TestSelectCompactBatchPartsAllowsSingleMultiPartArtifact(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-1",
			DestinationActivePartCount: 4,
			DestinationActivePartBytes: 1024,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 4,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxBytes: 2048})

	if len(selected) != 1 || selected[0].PartID != "part-1" {
		t.Fatalf("selected = %+v, want part-1", selected)
	}
}

func TestSelectCompactBatchPartsAllowsOversizedSingleMultiPartArtifact(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-1",
			DestinationActivePartCount: 4,
			DestinationActivePartBytes: 4096,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 4,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxBytes: 2048})

	if len(selected) != 1 || selected[0].PartID != "part-1" {
		t.Fatalf("selected = %+v, want oversized part-1", selected)
	}
}

func TestSelectCompactBatchPartsIgnoresCooldownField(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected := selectCompactBatchParts(compactGroup{
		parts: []Part{
			{
				PartID:                     "part-cooldown",
				DestinationActivePartCount: 2,
				DestinationActivePartBytes: 1024,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 2,
				},
				CompactCooldownUntil: formatTime(now.Add(time.Hour)),
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})
	if len(selected) != 1 || selected[0].PartID != "part-cooldown" {
		t.Fatalf("selected = %+v, want cooldown field ignored", selected)
	}
}

func TestSelectCompactBatchPartsIncludesRowsWithCooldownField(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	selected := selectCompactBatchParts(compactGroup{
		parts: []Part{
			{
				PartID:                     "part-fresh",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 1,
				},
			},
			{
				PartID:                     "part-cooldown",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"202606": 1,
				},
				CompactCooldownUntil: formatTime(now.Add(time.Hour)),
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-fresh" || selected[1].PartID != "part-cooldown" {
		t.Fatalf("selected = %+v, want fresh part with cooldown companion", selected)
	}
}

func TestCompactCandidateGroupsIncludesRowsWithCooldownField(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	groups := compactCandidateGroups([]Part{
		{
			JobID:                      "job-1",
			PartID:                     "part-cooldown",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
			CompactCooldownUntil: formatTime(now.Add(time.Hour)),
		},
		{
			JobID:                      "job-1",
			PartID:                     "part-ready",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
	}, nil, CompactClaimOptions{})
	if len(groups) != 1 || len(groups[0].parts) != 2 || groups[0].parts[0].PartID != "part-cooldown" || groups[0].parts[1].PartID != "part-ready" {
		t.Fatalf("groups = %+v, want cooldown and ready parts", groups)
	}
}

func TestCompactCandidateGroupsSkipsExcludedJobs(t *testing.T) {
	groups := compactCandidateGroups([]Part{
		{
			JobID:                      "job-1",
			PartID:                     "part-1",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
		{
			JobID:                      "job-2",
			PartID:                     "part-2",
			Bucket:                     "bucket",
			DestinationDatabase:        "db",
			DestinationTable:           "table",
			DestinationSchema:          "schema",
			DestinationActivePartCount: 2,
			DestinationActivePartitionCounts: map[string]uint64{
				"202606": 2,
			},
		},
	}, nil, CompactClaimOptions{ExcludedJobIDs: map[string]struct{}{"job-1": {}}})
	if len(groups) != 1 || len(groups[0].parts) != 1 || groups[0].parts[0].JobID != "job-2" {
		t.Fatalf("groups = %+v, want only non-excluded job-2", groups)
	}
}

func TestNewCompactPartSetsCompactReadyAt(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	readyAt := now.Add(-2 * time.Hour)
	part := NewCompactPart("job-1", "compact-1", "bucket", "finished/key", "db", "table", "schema", []string{"part-1"}, 1, PartStats{Count: 1}, map[string]uint64{"p": 1}, readyAt, now)
	if part.CreatedAt != formatTime(now) {
		t.Fatalf("created_at = %q, want %q", part.CreatedAt, formatTime(now))
	}
	if part.CompactReadyAt != formatTime(readyAt) {
		t.Fatalf("compact_ready_at = %q, want %q", part.CompactReadyAt, formatTime(readyAt))
	}
}

func TestCompactReadyAtForReleasePreservesStableTime(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		CompactReadyAt:    formatTime(now.Add(-3 * time.Hour)),
		ProgressUpdatedAt: formatTime(now.Add(-2 * time.Hour)),
		UpdatedAt:         formatTime(now),
	}
	if got := compactReadyAtForRelease(part, now); got != part.CompactReadyAt {
		t.Fatalf("compactReadyAtForRelease = %q, want compact_ready_at %q", got, part.CompactReadyAt)
	}
}

func TestCompactReadyAtForReleaseBackfillsExistingRowsFromProgress(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		ProgressUpdatedAt: formatTime(now.Add(-2 * time.Hour)),
		UpdatedAt:         formatTime(now),
	}
	if got := compactReadyAtForRelease(part, now); got != part.ProgressUpdatedAt {
		t.Fatalf("compactReadyAtForRelease = %q, want progress_updated_at %q", got, part.ProgressUpdatedAt)
	}
}

func TestCompactHeartbeatTimeUsesUpdatedAt(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		JobID:        "job-1",
		PartID:       "part-1",
		UpdatedAt:    formatTime(now),
		CompactingAt: formatTime(now.Add(-time.Hour)),
	}
	got, err := compactHeartbeatTime(part)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Fatalf("compactHeartbeatTime = %s, want %s", got, now)
	}
}

func TestCompactStaleTimeUsesOldestLeaseTimestamp(t *testing.T) {
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	part := Part{
		JobID:        "job-1",
		PartID:       "part-1",
		UpdatedAt:    formatTime(now),
		CompactingAt: formatTime(now.Add(-2 * time.Hour)),
	}
	got, err := compactStaleTime(part)
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(-2 * time.Hour)
	if !got.Equal(want) {
		t.Fatalf("compactStaleTime = %s, want %s", got, want)
	}
}

func TestSelectCompactBatchPartsDoesNotCombineInsufficientPartitions(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 0 {
		t.Fatalf("selected = %+v, want no cross-partition batch", selected)
	}
}

func TestSelectCompactBatchPartsFillsMultipleIdlePartitions(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-a2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
		{
			PartID:                     "part-b2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 4 ||
		selected[0].PartID != "part-a1" ||
		selected[1].PartID != "part-a2" ||
		selected[2].PartID != "part-b1" ||
		selected[3].PartID != "part-b2" {
		t.Fatalf("selected = %+v, want both eligible partitions", selected)
	}
}

func TestSelectCompactBatchPartsDoesNotPartiallyFillSecondPartitionAtMaxArtifacts(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-a2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
		{
			PartID:                     "part-b2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 3})

	if len(selected) != 2 || selected[0].PartID != "part-a1" || selected[1].PartID != "part-a2" {
		t.Fatalf("selected = %+v, want only complete partition-a batch", selected)
	}
}

func TestSelectCompactBatchPartsDoesNotPartiallyFillSecondPartitionAtMaxBytes(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-a2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b1",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
		{
			PartID:                     "part-b2",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxBytes: 350})

	if len(selected) != 2 || selected[0].PartID != "part-a1" || selected[1].PartID != "part-a2" {
		t.Fatalf("selected = %+v, want only complete partition-a batch", selected)
	}
}

func TestSelectCompactBatchPartsUsesSharedPartition(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-a" || selected[1].PartID != "part-b" {
		t.Fatalf("selected = %+v, want part-a and part-b", selected)
	}
}

func TestSelectCompactBatchPartsFillsSharedPartitionBatch(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-c",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 8})

	if len(selected) != 3 || selected[0].PartID != "part-a" || selected[1].PartID != "part-b" || selected[2].PartID != "part-c" {
		t.Fatalf("selected = %+v, want all compatible partition-a parts", selected)
	}
}

func TestSelectCompactBatchPartsStopsAtMaxArtifactsWhileFilling(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-c",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 2, MaxArtifacts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-a" || selected[1].PartID != "part-b" {
		t.Fatalf("selected = %+v, want max-artifacts-limited part-a and part-b", selected)
	}
}

func TestSelectCompactBatchPartsHonorsRequiredPartitions(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{parts: []Part{
		{
			PartID:                     "part-a",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-a": 1,
			},
		},
		{
			PartID:                     "part-b",
			DestinationActivePartCount: 1,
			DestinationActivePartBytes: 100,
			DestinationActivePartitionCounts: map[string]uint64{
				"partition-b": 1,
			},
		},
	}}, CompactClaimOptions{MinInputParts: 1, RequiredPartitionIDs: []string{"partition-b"}})

	if len(selected) != 1 || selected[0].PartID != "part-b" {
		t.Fatalf("selected = %+v, want only part-b", selected)
	}
}

func TestSelectCompactBatchPartsPrefersPartitionNotAlreadyCompacting(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{
		compactingPartitionIDs: []string{"partition-a"},
		parts: []Part{
			{
				PartID:                     "part-a1",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
			{
				PartID:                     "part-a2",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
			{
				PartID:                     "part-b1",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-b": 1,
				},
			},
			{
				PartID:                     "part-b2",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-b": 1,
				},
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-b1" || selected[1].PartID != "part-b2" {
		t.Fatalf("selected = %+v, want idle partition-b parts", selected)
	}
}

func TestSelectCompactBatchPartsFallsBackToAlreadyCompactingPartition(t *testing.T) {
	selected := selectCompactBatchParts(compactGroup{
		compactingPartitionIDs: []string{"partition-a"},
		parts: []Part{
			{
				PartID:                     "part-a1",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
			{
				PartID:                     "part-a2",
				DestinationActivePartCount: 1,
				DestinationActivePartBytes: 100,
				DestinationActivePartitionCounts: map[string]uint64{
					"partition-a": 1,
				},
			},
		},
	}, CompactClaimOptions{MinInputParts: 2})

	if len(selected) != 2 || selected[0].PartID != "part-a1" || selected[1].PartID != "part-a2" {
		t.Fatalf("selected = %+v, want busy partition-a fallback", selected)
	}
}
