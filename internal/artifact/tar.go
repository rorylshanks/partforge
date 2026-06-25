package artifact

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const finishedTarCopyBufferSize = 4 * 1024 * 1024

func WriteFinishedTar(tarPath string, partDirs []string) error {
	if strings.TrimSpace(tarPath) == "" {
		return fmt.Errorf("finished tar path is required")
	}
	if len(partDirs) == 0 {
		return fmt.Errorf("no finished part directories to archive")
	}
	if err := os.MkdirAll(filepath.Dir(tarPath), 0o755); err != nil {
		return err
	}

	sortedDirs := append([]string(nil), partDirs...)
	sort.Slice(sortedDirs, func(i, j int) bool {
		left, right := filepath.Base(filepath.Clean(sortedDirs[i])), filepath.Base(filepath.Clean(sortedDirs[j]))
		if left == right {
			return sortedDirs[i] < sortedDirs[j]
		}
		return left < right
	})

	tmp, err := os.CreateTemp(filepath.Dir(tarPath), filepath.Base(tarPath)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	tw := tar.NewWriter(tmp)
	seen := map[string]struct{}{}
	copyBuffer := make([]byte, finishedTarCopyBufferSize)
	for _, partDir := range sortedDirs {
		partName := filepath.Base(filepath.Clean(partDir))
		if err := validateTarPartName(partName); err != nil {
			_ = tw.Close()
			_ = tmp.Close()
			return err
		}
		if _, ok := seen[partName]; ok {
			_ = tw.Close()
			_ = tmp.Close()
			return fmt.Errorf("duplicate finished part directory %q", partName)
		}
		seen[partName] = struct{}{}
		if err := writePartDirToTar(tw, partDir, partName, copyBuffer); err != nil {
			_ = tw.Close()
			_ = tmp.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, tarPath); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

func ExtractFinishedTar(tarPath, destRoot string) ([]string, error) {
	return ExtractFinishedTarContext(context.Background(), tarPath, destRoot)
}

func ExtractFinishedTarContext(ctx context.Context, tarPath, destRoot string) ([]string, error) {
	if strings.TrimSpace(destRoot) == "" {
		return nil, fmt.Errorf("finished tar extract destination is required")
	}
	if err := os.MkdirAll(destRoot, 0o755); err != nil {
		return nil, err
	}
	partNames, err := validateFinishedTar(tarPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "tar", "--extract", "--file", tarPath, "--directory", destRoot, "--keep-old-files", "--no-same-owner")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("tar extract %s: %w: %s", tarPath, err, strings.TrimSpace(string(out)))
	}
	return partNames, nil
}

func validateFinishedTar(tarPath string) ([]string, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	partNames := map[string]struct{}{}
	tr := tar.NewReader(f)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		_, partName, err := cleanFinishedTarEntryName(header.Name)
		if err != nil {
			return nil, err
		}
		partNames[partName] = struct{}{}
		switch header.Typeflag {
		case tar.TypeDir, tar.TypeReg, tar.TypeRegA:
		default:
			return nil, fmt.Errorf("unsupported finished tar entry type %c for %s", header.Typeflag, header.Name)
		}
	}

	names := make([]string, 0, len(partNames))
	for name := range partNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func writePartDirToTar(tw *tar.Writer, partDir, partName string, copyBuffer []byte) error {
	info, err := os.Stat(partDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("finished part path %s is not a directory", partDir)
	}
	return filepath.WalkDir(partDir, func(entryPath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported finished part file type: %s", entryPath)
		}
		rel, err := filepath.Rel(partDir, entryPath)
		if err != nil {
			return err
		}
		tarName := partName
		if rel != "." {
			tarName = path.Join(partName, filepath.ToSlash(rel))
		}
		if info.IsDir() && !strings.HasSuffix(tarName, "/") {
			tarName += "/"
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = tarName
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(entryPath)
		if err != nil {
			return err
		}
		if _, err := io.CopyBuffer(tw, f, copyBuffer); err != nil {
			_ = f.Close()
			return err
		}
		return f.Close()
	})
}

func cleanFinishedTarEntryName(name string) (string, string, error) {
	if strings.TrimSpace(name) == "" || path.IsAbs(name) || strings.Contains(name, "\\") {
		return "", "", fmt.Errorf("unsafe finished tar entry name %q", name)
	}
	trimmed := strings.TrimSuffix(name, "/")
	for _, segment := range strings.Split(trimmed, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", "", fmt.Errorf("unsafe finished tar entry name %q", name)
		}
	}
	clean := path.Clean(name)
	if clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", "", fmt.Errorf("unsafe finished tar entry name %q", name)
	}
	partName := strings.Split(clean, "/")[0]
	if err := validateTarPartName(partName); err != nil {
		return "", "", err
	}
	return clean, partName, nil
}

func validateTarPartName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("unsafe finished part name %q", name)
	}
	return nil
}
