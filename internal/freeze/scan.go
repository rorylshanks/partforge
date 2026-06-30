package freeze

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/PostHog/partforge/internal/chhttp"
)

type Part struct {
	Disk         string
	Name         string
	Path         string
	RelativePath string
}

type Disk struct {
	Name string
	Path string
	Type string
}

func LocalDisks(ctx context.Context, ch chhttp.Client) ([]Disk, error) {
	out, err := ch.QueryString(ctx, "SELECT name, path, type FROM system.disks ORDER BY name FORMAT TSV")
	if err != nil {
		return nil, fmt.Errorf("query ClickHouse disks: %w", err)
	}
	rows, err := chhttp.FormatTSVStrings(out, 3)
	if err != nil {
		return nil, err
	}
	var disks []Disk
	for _, row := range rows {
		disk := Disk{Name: row[0], Path: row[1], Type: row[2]}
		if err := validateLocalDisk(disk); err != nil {
			return nil, err
		}
		disks = append(disks, disk)
	}
	if len(disks) == 0 {
		return nil, fmt.Errorf("ClickHouse reported no local disks")
	}
	return disks, nil
}

func validateLocalDisk(disk Disk) error {
	diskType := strings.ToLower(strings.TrimSpace(disk.Type))
	if strings.Contains(diskType, "s3") {
		return fmt.Errorf("ClickHouse disk %q uses unsupported S3 storage", disk.Name)
	}
	if diskType != "local" {
		return fmt.Errorf("ClickHouse disk %q has unsupported type %q", disk.Name, disk.Type)
	}
	if strings.TrimSpace(disk.Path) == "" {
		return fmt.Errorf("ClickHouse disk %q has empty path", disk.Name)
	}
	return nil
}

func ScanDisks(disks []Disk, freezeName string) ([]Part, error) {
	var parts []Part
	var roots []string
	for _, disk := range disks {
		root := filepath.Join(disk.Path, "shadow", freezeName)
		roots = append(roots, root)
		slog.Info("scanning freeze root", "stage", "scan_freeze", "disk", disk.Name, "root", root)
		diskParts, err := Scan(disk.Name, root)
		if errors.Is(err, os.ErrNotExist) {
			slog.Info("freeze root missing; skipping disk", "stage", "scan_freeze", "disk", disk.Name, "root", root)
			continue
		}
		if err != nil {
			return nil, err
		}
		slog.Info("scanned freeze root", "stage", "scan_freeze", "disk", disk.Name, "root", root, "parts", len(diskParts))
		parts = append(parts, diskParts...)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("no ClickHouse parts found under local disk freeze roots: %s", strings.Join(roots, ", "))
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].Disk != parts[j].Disk {
			return parts[i].Disk < parts[j].Disk
		}
		return parts[i].RelativePath < parts[j].RelativePath
	})
	return parts, nil
}

func Scan(diskName, root string) ([]Part, error) {
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
			Disk:         diskName,
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
