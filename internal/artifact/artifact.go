package artifact

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/PostHog/partforge/internal/manifest"
)

const ManifestName = "manifest.json"

func WriteManifest(dir string, m manifest.Manifest) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, ManifestName), b, 0o644)
}

func ReadManifest(dir string) (manifest.Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("read %s: %w", filepath.Join(dir, ManifestName), err)
	}
	var m manifest.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return manifest.Manifest{}, err
	}
	if err := m.Validate(); err != nil {
		return manifest.Manifest{}, err
	}
	return m, nil
}

func RemoveManifest(dir string) error {
	if err := os.Remove(filepath.Join(dir, ManifestName)); err != nil {
		return fmt.Errorf("remove %s: %w", filepath.Join(dir, ManifestName), err)
	}
	return nil
}
