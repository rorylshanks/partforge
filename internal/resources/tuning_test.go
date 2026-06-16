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
