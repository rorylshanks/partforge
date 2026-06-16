package resources

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/partforge/partforge/internal/chhttp"
)

const insertMemoryUsagePercent uint64 = 80

type Limits struct {
	CPUs        int
	MemoryBytes uint64
}

func DetectLimits() (Limits, error) {
	cpus, err := detectCPUs()
	if err != nil {
		return Limits{}, err
	}
	memoryBytes, err := detectMemoryBytes()
	if err != nil {
		return Limits{}, err
	}
	return Limits{CPUs: cpus, MemoryBytes: memoryBytes}, nil
}

func InsertSelectSettings(limits Limits) (chhttp.QuerySettings, error) {
	if limits.CPUs < 1 {
		return nil, fmt.Errorf("cpu limit must be at least 1, got %d", limits.CPUs)
	}
	if limits.MemoryBytes == 0 {
		return nil, fmt.Errorf("memory limit must be greater than zero")
	}
	maxMemoryUsage := limits.MemoryBytes * insertMemoryUsagePercent / 100
	if maxMemoryUsage == 0 {
		return nil, fmt.Errorf("derived max_memory_usage is zero from memory limit %d", limits.MemoryBytes)
	}
	threads := strconv.Itoa(limits.CPUs)
	return chhttp.QuerySettings{
		"max_threads":        threads,
		"max_insert_threads": threads,
		"max_memory_usage":   strconv.FormatUint(maxMemoryUsage, 10),
	}, nil
}

func detectCPUs() (int, error) {
	candidates := []int{runtime.NumCPU()}

	quotaCPUs, found, err := detectCgroupQuotaCPUs()
	if err != nil {
		return 0, err
	}
	if found {
		candidates = append(candidates, quotaCPUs)
	}

	cpusetCPUs, found, err := detectCgroupCPUSet()
	if err != nil {
		return 0, err
	}
	if found {
		candidates = append(candidates, cpusetCPUs)
	}

	cpus := 0
	for _, candidate := range candidates {
		if candidate < 1 {
			continue
		}
		if cpus == 0 || candidate < cpus {
			cpus = candidate
		}
	}
	if cpus < 1 {
		return 0, fmt.Errorf("could not determine usable CPU count")
	}
	return cpus, nil
}

func detectCgroupQuotaCPUs() (int, bool, error) {
	if raw, found, err := readOptionalFile("/sys/fs/cgroup/cpu.max"); err != nil {
		return 0, false, err
	} else if found {
		cpus, limited, err := parseCgroupV2CPUQuota(raw)
		return cpus, limited, err
	}

	quotaRaw, quotaFound, err := readOptionalFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	if err != nil {
		return 0, false, err
	}
	periodRaw, periodFound, err := readOptionalFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if err != nil {
		return 0, false, err
	}
	if !quotaFound && !periodFound {
		return 0, false, nil
	}
	if !quotaFound || !periodFound {
		return 0, false, fmt.Errorf("incomplete cgroup v1 CPU quota files")
	}
	cpus, limited, err := parseCgroupV1CPUQuota(quotaRaw, periodRaw)
	return cpus, limited, err
}

func detectCgroupCPUSet() (int, bool, error) {
	for _, path := range []string{
		"/sys/fs/cgroup/cpuset.cpus.effective",
		"/sys/fs/cgroup/cpuset.cpus",
		"/sys/fs/cgroup/cpuset/cpuset.cpus",
	} {
		raw, found, err := readOptionalFile(path)
		if err != nil {
			return 0, false, err
		}
		if !found || strings.TrimSpace(raw) == "" {
			continue
		}
		count, err := parseCPUSet(raw)
		return count, true, err
	}
	return 0, false, nil
}

func detectMemoryBytes() (uint64, error) {
	hostMemory, err := hostMemoryBytes()
	if err != nil {
		return 0, err
	}
	memory := hostMemory

	if raw, found, err := readOptionalFile("/sys/fs/cgroup/memory.max"); err != nil {
		return 0, err
	} else if found {
		cgroupMemory, limited, err := parseCgroupMemoryLimit(raw)
		if err != nil {
			return 0, err
		}
		if limited && cgroupMemory < memory {
			memory = cgroupMemory
		}
	}

	if raw, found, err := readOptionalFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err != nil {
		return 0, err
	} else if found {
		cgroupMemory, limited, err := parseCgroupMemoryLimit(raw)
		if err != nil {
			return 0, err
		}
		if limited && cgroupMemory < memory {
			memory = cgroupMemory
		}
	}

	if memory == 0 {
		return 0, fmt.Errorf("could not determine memory limit")
	}
	return memory, nil
}

func hostMemoryBytes() (uint64, error) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("read /proc/meminfo: %w", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "MemTotal:" {
			continue
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse MemTotal %q: %w", fields[1], err)
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("MemTotal not found in /proc/meminfo")
}

func parseCgroupV2CPUQuota(raw string) (int, bool, error) {
	fields := strings.Fields(raw)
	if len(fields) != 2 {
		return 0, false, fmt.Errorf("expected two fields in cpu.max, got %q", strings.TrimSpace(raw))
	}
	if fields[0] == "max" {
		return 0, false, nil
	}
	quota, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse cpu.max quota %q: %w", fields[0], err)
	}
	period, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse cpu.max period %q: %w", fields[1], err)
	}
	cpus, err := quotaToCPUs(quota, period)
	return cpus, true, err
}

func parseCgroupV1CPUQuota(quotaRaw, periodRaw string) (int, bool, error) {
	quota, err := strconv.ParseInt(strings.TrimSpace(quotaRaw), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse cgroup cpu quota %q: %w", strings.TrimSpace(quotaRaw), err)
	}
	if quota < 0 {
		return 0, false, nil
	}
	period, err := strconv.ParseInt(strings.TrimSpace(periodRaw), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse cgroup cpu period %q: %w", strings.TrimSpace(periodRaw), err)
	}
	cpus, err := quotaToCPUs(quota, period)
	return cpus, true, err
}

func quotaToCPUs(quota, period int64) (int, error) {
	if quota <= 0 {
		return 0, fmt.Errorf("cpu quota must be positive, got %d", quota)
	}
	if period <= 0 {
		return 0, fmt.Errorf("cpu period must be positive, got %d", period)
	}
	return int(math.Ceil(float64(quota) / float64(period))), nil
}

func parseCPUSet(raw string) (int, error) {
	total := 0
	for _, segment := range strings.Split(strings.TrimSpace(raw), ",") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		bounds := strings.Split(segment, "-")
		switch len(bounds) {
		case 1:
			if _, err := strconv.Atoi(bounds[0]); err != nil {
				return 0, fmt.Errorf("parse cpuset CPU %q: %w", segment, err)
			}
			total++
		case 2:
			start, err := strconv.Atoi(bounds[0])
			if err != nil {
				return 0, fmt.Errorf("parse cpuset start %q: %w", segment, err)
			}
			end, err := strconv.Atoi(bounds[1])
			if err != nil {
				return 0, fmt.Errorf("parse cpuset end %q: %w", segment, err)
			}
			if end < start {
				return 0, fmt.Errorf("invalid cpuset range %q", segment)
			}
			total += end - start + 1
		default:
			return 0, fmt.Errorf("invalid cpuset segment %q", segment)
		}
	}
	if total == 0 {
		return 0, fmt.Errorf("cpuset is empty")
	}
	return total, nil
}

func parseCgroupMemoryLimit(raw string) (uint64, bool, error) {
	value := strings.TrimSpace(raw)
	if value == "max" || value == "" {
		return 0, false, nil
	}
	memory, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse cgroup memory limit %q: %w", value, err)
	}
	if memory == 0 {
		return 0, false, fmt.Errorf("cgroup memory limit is zero")
	}
	return memory, true, nil
}

func readOptionalFile(path string) (string, bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("read %s: %w", path, err)
	}
	return string(raw), true, nil
}
