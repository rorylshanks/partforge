package chproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
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
	cmd         *exec.Cmd
	done        chan error
	pidFilePath string
	stopErr     error
	stopOnce    sync.Once
}

const clickHouseStopTimeout = 30 * time.Second

func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Binary == "" {
		return nil, fmt.Errorf("clickhouse binary is empty")
	}
	errorLogPath := cfg.errorLogPath()
	pidFilePath := cfg.pidFilePath()
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
	server := &Server{cmd: cmd, done: make(chan error, 1), pidFilePath: pidFilePath}
	go func() {
		server.done <- cmd.Wait()
	}()

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
			_ = server.Stop()
			return nil, clickHouseStartError(fmt.Sprintf("clickhouse did not become ready within %s", timeout), nil, errorLogPath)
		}
		select {
		case err := <-server.done:
			if err == nil {
				return nil, clickHouseStartError("clickhouse server exited before becoming ready", nil, errorLogPath)
			}
			return nil, clickHouseStartError("clickhouse server exited before becoming ready", err, errorLogPath)
		case <-ctx.Done():
			_ = server.Stop()
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

func (cfg Config) errorLogPath() string {
	if strings.TrimSpace(cfg.DataDir) == "" {
		return ""
	}
	root, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return ""
	}
	return filepath.Join(filepath.Clean(root), "logs", "clickhouse-server.err.log")
}

func (cfg Config) pidFilePath() string {
	if strings.TrimSpace(cfg.DataDir) == "" {
		return ""
	}
	root, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return ""
	}
	return filepath.Join(filepath.Clean(root), "clickhouse-server.pid")
}

func clickHouseStartError(message string, cause error, errorLogPath string) error {
	detail := clickHouseErrorLogTail(errorLogPath)
	if cause != nil {
		if detail != "" {
			return fmt.Errorf("%s: %w; clickhouse error log: %s", message, cause, detail)
		}
		return fmt.Errorf("%s: %w", message, cause)
	}
	if detail != "" {
		return fmt.Errorf("%s; clickhouse error log: %s", message, detail)
	}
	return errors.New(message)
}

func clickHouseErrorLogTail(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()

	const maxBytes = 16 * 1024
	info, err := file.Stat()
	if err != nil {
		return ""
	}
	offset := int64(0)
	if info.Size() > maxBytes {
		offset = info.Size() - maxBytes
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return ""
	}
	raw, err := io.ReadAll(file)
	if err != nil {
		return ""
	}
	return clickHouseErrorLogLine(string(raw))
}

func clickHouseErrorLogLine(logTail string) string {
	lines := strings.Split(logTail, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.Contains(line, "<Error>") || strings.Contains(line, "Code:") {
			return strings.Join(strings.Fields(line), " ")
		}
	}
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return strings.Join(strings.Fields(line), " ")
		}
	}
	return ""
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

func (s *Server) Stop() error {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	s.stopOnce.Do(func() {
		s.stopErr = s.stop()
	})
	return s.stopErr
}

func (s *Server) stop() error {
	signalErr := s.cmd.Process.Signal(syscall.SIGTERM)
	if signalErr != nil && !errors.Is(signalErr, os.ErrProcessDone) {
		return fmt.Errorf("send SIGTERM to clickhouse process: %w", signalErr)
	}

	select {
	case err := <-s.done:
		if err != nil && !processTerminatedBy(err, syscall.SIGTERM) {
			return fmt.Errorf("wait for clickhouse process after SIGTERM: %w", err)
		}
	case <-time.After(clickHouseStopTimeout):
		return fmt.Errorf("clickhouse process did not exit within %s after SIGTERM", clickHouseStopTimeout)
	}

	if err := waitForPIDFileUnlock(s.pidFilePath, clickHouseStopTimeout); err != nil {
		return err
	}
	return nil
}

func processTerminatedBy(err error, signal syscall.Signal) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == signal
}

func waitForPIDFileUnlock(path string, timeout time.Duration) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		unlocked, err := pidFileUnlocked(path)
		if err != nil {
			return err
		}
		if unlocked {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("clickhouse pid file %s remained locked after %s", path, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func pidFileUnlocked(path string) (bool, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("open clickhouse pid file %s: %w", path, err)
	}
	defer file.Close()

	lock := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: int16(io.SeekStart)}
	if err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock); err != nil {
		if err == syscall.EACCES || err == syscall.EAGAIN {
			return false, nil
		}
		return false, fmt.Errorf("lock clickhouse pid file %s: %w", path, err)
	}
	lock.Type = syscall.F_UNLCK
	if err := syscall.FcntlFlock(file.Fd(), syscall.F_SETLK, &lock); err != nil {
		return false, fmt.Errorf("unlock clickhouse pid file %s: %w", path, err)
	}
	return true, nil
}
