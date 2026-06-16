package freeze

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type Part struct {
	Name         string
	Path         string
	RelativePath string
}

func Scan(shadowDir, freezeName string) ([]Part, error) {
	root := filepath.Join(shadowDir, freezeName)
	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat freeze directory %s: %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("freeze path %s is not a directory", root)
	}

	var parts []Part
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		if path == root {
			return nil
		}
		if !looksLikePart(path) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		parts = append(parts, Part{
			Name:         filepath.Base(path),
			Path:         path,
			RelativePath: filepath.ToSlash(rel),
		})
		return filepath.SkipDir
	})
	if err != nil {
		return nil, fmt.Errorf("scan freeze directory %s: %w", root, err)
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].RelativePath < parts[j].RelativePath
	})
	return parts, nil
}

func looksLikePart(path string) bool {
	required := []string{"checksums.txt", "columns.txt"}
	for _, name := range required {
		info, err := os.Stat(filepath.Join(path, name))
		if err != nil || info.IsDir() {
			return false
		}
	}
	return true
}
