package s3copy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const directoryCopyRetries = 3

type Copier struct {
	Binary     string
	Endpoint   string
	NumWorkers int
}

type CommandError struct {
	Binary string
	Args   []string
	Err    error
	Output string
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("%s %s failed: %v: %s", e.Binary, strings.Join(e.Args, " "), e.Err, strings.TrimSpace(e.Output))
}

func (e *CommandError) Unwrap() error {
	return e.Err
}

func (c Copier) UploadDir(ctx context.Context, localDir, bucket, prefix string) error {
	if err := requireDir(localDir); err != nil {
		return err
	}
	return c.runCopy(ctx, withTrailingSeparator(localDir), s3URI(bucket, prefix)+"/")
}

func (c Copier) UploadGlob(ctx context.Context, localGlob, bucket, prefix string) error {
	return c.runCopy(ctx, localGlob, s3URI(bucket, prefix)+"/")
}

func (c Copier) DownloadPrefix(ctx context.Context, bucket, prefix, localDir string) error {
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return err
	}
	return c.runCopy(ctx, s3URI(bucket, prefix)+"/*", withTrailingSeparator(localDir))
}

func (c Copier) DeletePrefix(ctx context.Context, bucket, prefix string) error {
	target, err := deletePrefixTarget(bucket, prefix)
	if err != nil {
		return err
	}
	return c.run(ctx, "rm", target)
}

func (c Copier) DeletePrefixIfExists(ctx context.Context, bucket, prefix string) error {
	err := c.DeletePrefix(ctx, bucket, prefix)
	if err == nil || isNoObjectFound(err) {
		return nil
	}
	return err
}

func (c Copier) runCopy(ctx context.Context, args ...string) error {
	fullArgs := c.copyArgs(args...)
	maxAttempts := directoryCopyRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.runArgs(ctx, fullArgs)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		lastErr = err
		if attempt < maxAttempts {
			slog.Warn(
				"s5cmd directory copy failed; retrying",
				"attempt", attempt,
				"next_attempt", attempt+1,
				"max_attempts", maxAttempts,
				"error", err,
			)
		}
	}
	return fmt.Errorf("s5cmd directory copy failed after %d attempts: %w", maxAttempts, lastErr)
}

func (c Copier) run(ctx context.Context, command string, args ...string) error {
	return c.runArgs(ctx, c.args(command, args...))
}

func (c Copier) runArgs(ctx context.Context, fullArgs []string) error {
	binary := c.Binary
	if strings.TrimSpace(binary) == "" {
		binary = "s5cmd"
	}
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &CommandError{Binary: binary, Args: fullArgs, Err: err, Output: string(out)}
	}
	return nil
}

func isNoObjectFound(err error) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && strings.Contains(commandErr.Output, "no object found")
}

func (c Copier) args(command string, args ...string) []string {
	return c.argsWithRetryCount(command, "0", args...)
}

func (c Copier) copyArgs(args ...string) []string {
	return c.argsWithRetryCount("cp", "", args...)
}

func (c Copier) argsWithRetryCount(command, retryCount string, args ...string) []string {
	fullArgs := []string{"--log=error"}
	if retryCount != "" {
		fullArgs = append(fullArgs, "--retry-count", retryCount)
	}
	if c.NumWorkers > 0 {
		fullArgs = append(fullArgs, "--numworkers", fmt.Sprintf("%d", c.NumWorkers))
	}
	if c.Endpoint != "" {
		fullArgs = append(fullArgs, "--endpoint-url", c.Endpoint)
	}
	fullArgs = append(fullArgs, command)
	fullArgs = append(fullArgs, args...)
	return fullArgs
}

func requireDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}

func s3URI(bucket, prefix string) string {
	return "s3://" + strings.Trim(bucket, "/") + "/" + strings.Trim(prefix, "/")
}

func deletePrefixTarget(bucket, prefix string) (string, error) {
	bucket = strings.Trim(bucket, "/")
	prefix = strings.Trim(prefix, "/")
	if bucket == "" {
		return "", fmt.Errorf("s3 bucket is required")
	}
	if prefix == "" {
		return "", fmt.Errorf("s3 prefix is required")
	}
	if containsS5cmdGlobMeta(bucket) {
		return "", fmt.Errorf("s3 bucket %q contains s5cmd glob metacharacters", bucket)
	}
	if containsS5cmdGlobMeta(prefix) {
		return "", fmt.Errorf("s3 prefix %q contains s5cmd glob metacharacters", prefix)
	}
	return s3URI(bucket, prefix) + "/*", nil
}

func containsS5cmdGlobMeta(value string) bool {
	return strings.ContainsAny(value, "*?[]{}")
}

func withTrailingSeparator(path string) string {
	clean := filepath.Clean(path)
	if strings.HasSuffix(clean, string(filepath.Separator)) {
		return clean
	}
	return clean + string(filepath.Separator)
}
