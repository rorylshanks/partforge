package chproc

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/partforge/partforge/internal/chhttp"
)

type Config struct {
	Binary     string
	ConfigFile string
	DataDir    string
	URL        string
	User       string
	Password   string
	Timeout    time.Duration
	Tuning     Tuning
}

type Tuning struct {
	BackgroundPoolSize int
}

type Server struct {
	cmd *exec.Cmd
}

func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Binary == "" {
		return nil, fmt.Errorf("clickhouse binary is empty")
	}
	args, err := cfg.args()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(cfg.Binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start clickhouse server: %w", err)
	}
	server := &Server{cmd: cmd}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 90 * time.Second
	}
	deadline := time.Now().Add(timeout)
	client := chhttp.Client{URL: cfg.URL, User: cfg.User, Password: cfg.Password}
	for {
		if err := client.Ping(ctx); err == nil {
			slog.Info("clickhouse server is ready")
			return server, nil
		}
		if time.Now().After(deadline) {
			server.Stop()
			return nil, fmt.Errorf("clickhouse did not become ready within %s", timeout)
		}
		select {
		case <-ctx.Done():
			server.Stop()
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (cfg Config) args() ([]string, error) {
	args := []string{}
	if !strings.Contains(filepath.Base(cfg.Binary), "clickhouse-server") {
		args = append(args, "server")
	}
	if cfg.ConfigFile != "" {
		args = append(args, "--config-file="+cfg.ConfigFile)
	}
	var configOverrides []string
	if strings.TrimSpace(cfg.DataDir) != "" {
		storageArgs, storageOverrides, err := storageConfigArgs(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		args = append(args, storageArgs...)
		configOverrides = append(configOverrides, storageOverrides...)
	}
	if cfg.Tuning.BackgroundPoolSize < 0 {
		return nil, fmt.Errorf("background pool size must be non-negative, got %d", cfg.Tuning.BackgroundPoolSize)
	}
	if cfg.Tuning.BackgroundPoolSize > 0 {
		configOverrides = append(configOverrides, fmt.Sprintf("--background_pool_size=%d", cfg.Tuning.BackgroundPoolSize))
	}
	if len(configOverrides) > 0 {
		args = append(args, "--")
		args = append(args, configOverrides...)
	}
	return args, nil
}

func storageConfigArgs(dataDir string) ([]string, []string, error) {
	root, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve clickhouse data dir %s: %w", dataDir, err)
	}
	root = filepath.Clean(root)
	logDir := filepath.Join(root, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create clickhouse path %s: %w", logDir, err)
	}
	paths := []struct {
		arg  string
		path string
	}{
		{"--path=", filepath.Join(root, "data")},
		{"--tmp_path=", filepath.Join(root, "tmp")},
		{"--user_files_path=", filepath.Join(root, "user_files")},
		{"--format_schema_path=", filepath.Join(root, "format_schemas")},
		{"--custom_cached_disks_base_directory=", filepath.Join(root, "caches")},
		{"--filesystem_caches_path=", filepath.Join(root, "filesystem_caches")},
		{"--custom_local_disks_base_directory=", filepath.Join(root, "disks")},
		{"--user_directories.local_directory.path=", filepath.Join(root, "access")},
	}
	for _, p := range paths {
		if err := os.MkdirAll(p.path, 0o755); err != nil {
			return nil, nil, fmt.Errorf("create clickhouse path %s: %w", p.path, err)
		}
	}

	serverArgs := []string{
		"--log-file=" + filepath.Join(logDir, "clickhouse-server.log"),
		"--errorlog-file=" + filepath.Join(logDir, "clickhouse-server.err.log"),
		"--pid-file=" + filepath.Join(root, "clickhouse-server.pid"),
	}
	configOverrides := make([]string, 0, len(paths))
	for _, p := range paths {
		configOverrides = append(configOverrides, p.arg+withTrailingSeparator(p.path))
	}
	return serverArgs, configOverrides, nil
}

func withTrailingSeparator(path string) string {
	clean := filepath.Clean(path)
	if strings.HasSuffix(clean, string(filepath.Separator)) {
		return clean
	}
	return clean + string(filepath.Separator)
}

func (s *Server) Stop() {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return
	}
	_ = s.cmd.Process.Kill()
	done := make(chan struct{})
	go func() {
		_ = s.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		slog.Warn("timed out waiting for killed clickhouse process")
	}
}
