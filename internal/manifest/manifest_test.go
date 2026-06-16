package manifest

import (
	"testing"
	"time"
)

func TestFinishedPartAttemptPrefix(t *testing.T) {
	finishedAt := time.Date(2026, 6, 16, 17, 30, 45, 123456789, time.UTC)

	got := FinishedPartAttemptPrefix("prefix/jobs/job-1/finished/part-1", 3, finishedAt)
	want := "prefix/jobs/job-1/finished/part-1/attempt-000003-20260616T173045.123456789Z"
	if got != want {
		t.Fatalf("attempt prefix = %q, want %q", got, want)
	}
}
