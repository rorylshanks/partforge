package resources

import "testing"

func TestInsertSelectSettings(t *testing.T) {
	settings, err := InsertSelectSettings(Limits{CPUs: 6, MemoryBytes: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if settings["max_threads"] != "6" {
		t.Fatalf("max_threads = %q", settings["max_threads"])
	}
	if settings["max_insert_threads"] != "6" {
		t.Fatalf("max_insert_threads = %q", settings["max_insert_threads"])
	}
	if settings["max_memory_usage"] != "8000" {
		t.Fatalf("max_memory_usage = %q", settings["max_memory_usage"])
	}
}

func TestInsertThreadCount(t *testing.T) {
	tests := []struct {
		cpus int
		want int
	}{
		{cpus: 1, want: 1},
		{cpus: 2, want: 2},
		{cpus: 3, want: 3},
		{cpus: 8, want: 8},
		{cpus: 16, want: 16},
	}

	for _, tt := range tests {
		if got := insertThreadCount(tt.cpus); got != tt.want {
			t.Fatalf("insertThreadCount(%d) = %d, want %d", tt.cpus, got, tt.want)
		}
	}
}

func TestMergeTreeSettingsForLimits(t *testing.T) {
	tests := []struct {
		name      string
		limits    Limits
		wantRows  uint64
		wantBytes uint64
	}{
		{
			name:      "low memory clamps to safe minimum",
			limits:    Limits{CPUs: 8, MemoryBytes: 1 * 1024 * 1024 * 1024},
			wantRows:  8192,
			wantBytes: 9 * 1024 * 1024,
		},
		{
			name:      "scales with memory per background worker",
			limits:    Limits{CPUs: 16, MemoryBytes: 32 * 1024 * 1024 * 1024},
			wantRows:  155648,
			wantBytes: 153 * 1024 * 1024,
		},
		{
			name:      "high memory clamps to upper bound",
			limits:    Limits{CPUs: 16, MemoryBytes: 1024 * 1024 * 1024 * 1024},
			wantRows:  262144,
			wantBytes: 256 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, err := MergeTreeSettingsForLimits(tt.limits)
			if err != nil {
				t.Fatal(err)
			}
			if settings.MergeMaxBlockSize != tt.wantRows {
				t.Fatalf("merge_max_block_size = %d, want %d", settings.MergeMaxBlockSize, tt.wantRows)
			}
			if settings.MergeMaxBlockSizeBytes != tt.wantBytes {
				t.Fatalf("merge_max_block_size_bytes = %d, want %d", settings.MergeMaxBlockSizeBytes, tt.wantBytes)
			}
		})
	}
}

func TestParseCgroupV2CPUQuota(t *testing.T) {
	cpus, limited, err := parseCgroupV2CPUQuota("250000 100000")
	if err != nil {
		t.Fatal(err)
	}
	if !limited || cpus != 3 {
		t.Fatalf("limited = %v, cpus = %d", limited, cpus)
	}

	_, limited, err = parseCgroupV2CPUQuota("max 100000")
	if err != nil {
		t.Fatal(err)
	}
	if limited {
		t.Fatal("expected unlimited cpu.max")
	}
}

func TestParseCPUSet(t *testing.T) {
	count, err := parseCPUSet("0-3,6,8-9")
	if err != nil {
		t.Fatal(err)
	}
	if count != 7 {
		t.Fatalf("count = %d", count)
	}
}

func TestParseCgroupMemoryLimit(t *testing.T) {
	memory, limited, err := parseCgroupMemoryLimit("12345")
	if err != nil {
		t.Fatal(err)
	}
	if !limited || memory != 12345 {
		t.Fatalf("limited = %v, memory = %d", limited, memory)
	}

	_, limited, err = parseCgroupMemoryLimit("max")
	if err != nil {
		t.Fatal(err)
	}
	if limited {
		t.Fatal("expected unlimited memory.max")
	}
}
