package chproc

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestArgsIncludeGeneratedStorageConfig(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "clickhouse")
	cfg := Config{
		Binary:     "clickhouse",
		ConfigFile: "/etc/clickhouse-server/config.xml",
		DataDir:    dataDir,
		Tuning:     Tuning{BackgroundPoolSize: 12},
	}

	got, err := cfg.args()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"server",
		"--config-file=/etc/clickhouse-server/config.xml",
		"--log-file=" + filepath.Join(root, "logs", "clickhouse-server.log"),
		"--errorlog-file=" + filepath.Join(root, "logs", "clickhouse-server.err.log"),
		"--pid-file=" + filepath.Join(root, "clickhouse-server.pid"),
		"--",
		"--path=" + withTrailingSeparator(filepath.Join(root, "data")),
		"--tmp_path=" + withTrailingSeparator(filepath.Join(root, "tmp")),
		"--user_files_path=" + withTrailingSeparator(filepath.Join(root, "user_files")),
		"--format_schema_path=" + withTrailingSeparator(filepath.Join(root, "format_schemas")),
		"--custom_cached_disks_base_directory=" + withTrailingSeparator(filepath.Join(root, "caches")),
		"--filesystem_caches_path=" + withTrailingSeparator(filepath.Join(root, "filesystem_caches")),
		"--custom_local_disks_base_directory=" + withTrailingSeparator(filepath.Join(root, "disks")),
		"--user_directories.local_directory.path=" + withTrailingSeparator(filepath.Join(root, "access")),
		"--background_pool_size=12",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	for _, path := range []string{"data", "tmp", "user_files", "format_schemas", "caches", "filesystem_caches", "disks", "access", "logs"} {
		if info, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("stat generated path %s: %v", path, err)
		} else if !info.IsDir() {
			t.Fatalf("generated path %s is not a directory", path)
		}
	}
}

func TestArgsIncludeBackgroundPoolSize(t *testing.T) {
	cfg := Config{Binary: "clickhouse", Tuning: Tuning{BackgroundPoolSize: 12}}
	got, err := cfg.args()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"server",
		"--",
		"--background_pool_size=12",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestArgsForClickHouseServerBinaryOmitServerSubcommand(t *testing.T) {
	cfg := Config{Binary: "clickhouse-server", ConfigFile: "/etc/clickhouse-server/config.xml"}
	got, err := cfg.args()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"--config-file=/etc/clickhouse-server/config.xml"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestArgsRejectInvalidBackgroundPoolSize(t *testing.T) {
	cfg := Config{Binary: "clickhouse", Tuning: Tuning{BackgroundPoolSize: -1}}
	_, err := cfg.args()
	if err == nil {
		t.Fatal("expected invalid background pool size error")
	}
}

func TestStartFailsWhenProcessExitsBeforeReady(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "fake-clickhouse")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 42\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "clickhouse")
	errorLogDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(errorLogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(errorLogDir, "clickhouse-server.err.log"), []byte("Code: 76. Cannot open file /tmp/clickhouse/status\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	server, err := Start(context.Background(), Config{
		DataDir: dataDir,
		Binary:  binary,
		URL:     "http://127.0.0.1:1",
		Timeout: time.Minute,
	})
	if server != nil {
		t.Fatalf("server = %+v, want nil", server)
	}
	if err == nil {
		t.Fatal("expected start error")
	}
	if !strings.Contains(err.Error(), "clickhouse server exited before becoming ready") {
		t.Fatalf("start error = %v", err)
	}
	if !strings.Contains(err.Error(), "Cannot open file /tmp/clickhouse/status") {
		t.Fatalf("start error missing ClickHouse log detail: %v", err)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatalf("Start did not fail fast: %s", time.Since(started))
	}
}

func TestClickHouseErrorLogLinePrefersErrorLine(t *testing.T) {
	got := clickHouseErrorLogLine("trace\n2026 <Error> Application: Code: 76. Cannot open file /tmp/status\n0. stack\n")
	want := "2026 <Error> Application: Code: 76. Cannot open file /tmp/status"
	if got != want {
		t.Fatalf("clickHouseErrorLogLine = %q, want %q", got, want)
	}
}

func TestPIDFileUnlockedReportsFcntlLockedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clickhouse-server.pid")
	if err := os.WriteFile(path, []byte("123\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestPIDFileLockHelper", "--", path)
	cmd.Env = append(os.Environ(), "PARTFORGE_PID_LOCK_HELPER=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	})

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(line) != "locked" {
		t.Fatalf("helper output = %q, want locked", line)
	}

	unlocked, err := pidFileUnlocked(path)
	if err != nil {
		t.Fatal(err)
	}
	if unlocked {
		t.Fatal("pidFileUnlocked = true, want false while another process holds the lock")
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	unlocked, err = pidFileUnlocked(path)
	if err != nil {
		t.Fatal(err)
	}
	if !unlocked {
		t.Fatal("pidFileUnlocked = false, want true after helper exits")
	}
}

func TestPIDFileLockHelper(t *testing.T) {
	if os.Getenv("PARTFORGE_PID_LOCK_HELPER") != "1" {
		return
	}
	path := os.Args[len(os.Args)-1]
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer file.Close()
	lock := syscall.Flock_t{Type: syscall.F_WRLCK}
	if err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Println("locked")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	lock.Type = syscall.F_UNLCK
	if err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}
