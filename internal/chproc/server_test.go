package chproc

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
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
