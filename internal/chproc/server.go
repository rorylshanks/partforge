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
	args := []string{}
	if !strings.Contains(filepath.Base(cfg.Binary), "clickhouse-server") {
		args = append(args, "server")
	}
	if cfg.ConfigFile != "" {
		args = append(args, "--config-file="+cfg.ConfigFile)
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
