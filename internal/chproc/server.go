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

	cmd := exec.CommandContext(ctx, cfg.Binary, args...)
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
	if strings.TrimSpace(cfg.DataDir) != "" {
		storageArgs, err := storageConfigArgs(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		args = append(args, "--")
		args = append(args, storageArgs...)
	}
	return args, nil
}

func storageConfigArgs(dataDir string) ([]string, error) {
	root, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve clickhouse data dir %s: %w", dataDir, err)
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create clickhouse data dir %s: %w", root, err)
	}

	return []string{
		"--path=" + withTrailingSeparator(filepath.Join(root, "data")),
		"--tmp_path=" + withTrailingSeparator(filepath.Join(root, "tmp")),
		"--user_files_path=" + withTrailingSeparator(filepath.Join(root, "user_files")),
		"--format_schema_path=" + withTrailingSeparator(filepath.Join(root, "format_schemas")),
		"--custom_cached_disks_base_directory=" + withTrailingSeparator(filepath.Join(root, "caches")),
		"--filesystem_caches_path=" + withTrailingSeparator(filepath.Join(root, "filesystem_caches")),
		"--custom_local_disks_base_directory=" + withTrailingSeparator(filepath.Join(root, "disks")),
		"--user_directories.local_directory.path=" + withTrailingSeparator(filepath.Join(root, "access")),
	}, nil
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
	if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
		_ = s.cmd.Process.Kill()
	}
	done := make(chan struct{})
	go func() {
		_ = s.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = s.cmd.Process.Kill()
		<-done
	}
}
