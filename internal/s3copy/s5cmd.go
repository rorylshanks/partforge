package s3copy

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Copier struct {
	Binary     string
	Endpoint   string
	NumWorkers int
}

func (c Copier) UploadDir(ctx context.Context, localDir, bucket, prefix string) error {
	if err := requireDir(localDir); err != nil {
		return err
	}
	return c.run(ctx, "cp", withTrailingSeparator(localDir), s3URI(bucket, prefix)+"/")
}

func (c Copier) DownloadPrefix(ctx context.Context, bucket, prefix, localDir string) error {
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return err
	}
	return c.run(ctx, "cp", s3URI(bucket, prefix)+"/*", withTrailingSeparator(localDir))
}

func (c Copier) DeletePrefix(ctx context.Context, bucket, prefix string) error {
	target, err := deletePrefixTarget(bucket, prefix)
	if err != nil {
		return err
	}
	return c.run(ctx, "rm", target)
}

func (c Copier) run(ctx context.Context, command string, args ...string) error {
	binary := c.Binary
	if strings.TrimSpace(binary) == "" {
		binary = "s5cmd"
	}
	fullArgs := c.args(command, args...)
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s failed: %w: %s", binary, strings.Join(fullArgs, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (c Copier) args(command string, args ...string) []string {
	fullArgs := []string{"--retry-count", "0"}
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
