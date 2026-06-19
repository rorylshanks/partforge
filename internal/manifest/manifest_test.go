package manifest

import "testing"

func TestDeriveJobIDWithDefaultOptionsMatchesLegacyDerivation(t *testing.T) {
	got := DeriveJobIDWithOptions("db", "table", "freeze", "source", "dest", "insert", Options{})
	want := DeriveJobID("db", "table", "freeze", "source", "dest", "insert")
	if got != want {
		t.Fatalf("job id with default options = %q, want %q", got, want)
	}
}

func TestDeriveJobIDIncludesOptimizeFinalOption(t *testing.T) {
	defaultID := DeriveJobID("db", "table", "freeze", "source", "dest", "insert")
	optimizeID := DeriveJobIDWithOptions("db", "table", "freeze", "source", "dest", "insert", Options{OptimizeFinal: true})
	if optimizeID == defaultID {
		t.Fatal("expected optimize_final to affect derived job id")
	}
}

func TestDerivePartIDIncludesOptimizeFinalOption(t *testing.T) {
	defaultID := DerivePartID("disk", "relative", "part", "source", "dest", "insert")
	optimizeID := DerivePartIDWithOptions("disk", "relative", "part", "source", "dest", "insert", Options{OptimizeFinal: true})
	if optimizeID == defaultID {
		t.Fatal("expected optimize_final to affect derived part id")
	}
}
