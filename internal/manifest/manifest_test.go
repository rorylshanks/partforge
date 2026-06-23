package manifest

import "testing"

func TestDeriveJobIDStable(t *testing.T) {
	got := DeriveJobID("db", "table", "freeze", "source", "dest", "insert")
	again := DeriveJobID("db", "table", "freeze", "source", "dest", "insert")
	if got != again {
		t.Fatalf("job id is not stable: %q != %q", got, again)
	}
}

func TestDerivePartIDIncludesPartIdentity(t *testing.T) {
	left := DerivePartID("disk", "relative", "part", "source", "dest", "insert")
	right := DerivePartID("disk", "relative", "other-part", "source", "dest", "insert")
	if left == right {
		t.Fatal("expected part name to affect derived part id")
	}
}
