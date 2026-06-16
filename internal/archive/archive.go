package archive

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/partforge/partforge/internal/manifest"
)

const ManifestName = "manifest.json"

func WriteSource(w io.Writer, m manifest.Manifest, partPath string) error {
	if err := m.Validate(); err != nil {
		return err
	}
	gw := gzip.NewWriter(w)
	tw := tar.NewWriter(gw)

	if err := writeManifest(tw, m); err != nil {
		_ = tw.Close()
		_ = gw.Close()
		return err
	}
	if err := addDir(tw, partPath, filepath.ToSlash(filepath.Join("part", m.Part.Name))); err != nil {
		_ = tw.Close()
		_ = gw.Close()
		return err
	}
	if err := tw.Close(); err != nil {
		_ = gw.Close()
		return err
	}
	return gw.Close()
}

func WriteFinished(w io.Writer, m manifest.Manifest, partsRoot string, outputParts []manifest.OutputPart) error {
	if err := m.Validate(); err != nil {
		return err
	}
	m.Output.Parts = outputParts

	gw := gzip.NewWriter(w)
	tw := tar.NewWriter(gw)

	if err := writeManifest(tw, m); err != nil {
		_ = tw.Close()
		_ = gw.Close()
		return err
	}
	for _, part := range outputParts {
		if err := addDir(tw, filepath.Join(partsRoot, part.Name), filepath.ToSlash(filepath.Join("parts", part.Name))); err != nil {
			_ = tw.Close()
			_ = gw.Close()
			return err
		}
	}
	if err := tw.Close(); err != nil {
		_ = gw.Close()
		return err
	}
	return gw.Close()
}

func Extract(r io.Reader, dest string) (manifest.Manifest, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return manifest.Manifest{}, err
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	var foundManifest bool
	var m manifest.Manifest

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return manifest.Manifest{}, err
		}
		target, err := safeJoin(dest, header.Name)
		if err != nil {
			return manifest.Manifest{}, err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
				return manifest.Manifest{}, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return manifest.Manifest{}, err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return manifest.Manifest{}, err
			}
			_, copyErr := io.Copy(f, tr)
			closeErr := f.Close()
			if copyErr != nil {
				return manifest.Manifest{}, copyErr
			}
			if closeErr != nil {
				return manifest.Manifest{}, closeErr
			}
			if header.Name == ManifestName {
				b, err := os.ReadFile(target)
				if err != nil {
					return manifest.Manifest{}, err
				}
				if err := json.Unmarshal(b, &m); err != nil {
					return manifest.Manifest{}, err
				}
				foundManifest = true
			}
		default:
			return manifest.Manifest{}, fmt.Errorf("unsupported tar entry %s type %d", header.Name, header.Typeflag)
		}
	}

	if !foundManifest {
		return manifest.Manifest{}, fmt.Errorf("archive does not contain %s", ManifestName)
	}
	if err := m.Validate(); err != nil {
		return manifest.Manifest{}, err
	}
	return m, nil
}

func SourcePartPath(root string, m manifest.Manifest) string {
	return filepath.Join(root, "part", m.Part.Name)
}

func FinishedPartPath(root, name string) string {
	return filepath.Join(root, "parts", name)
}

func writeManifest(tw *tar.Writer, m manifest.Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	header := &tar.Header{
		Name: ManifestName,
		Mode: 0o644,
		Size: int64(len(b)),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err = tw.Write(b)
	return err
}

func addDir(tw *tar.Writer, root, archiveRoot string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := archiveRoot
		if rel != "." {
			name = filepath.ToSlash(filepath.Join(archiveRoot, rel))
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = name
		if entry.IsDir() {
			header.Name += "/"
			return tw.WriteHeader(header)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported non-regular file in part: %s", path)
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func safeJoin(root, name string) (string, error) {
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("archive path %q is absolute", name)
	}
	clean := filepath.Clean(name)
	if clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
		return "", fmt.Errorf("archive path %q escapes destination", name)
	}
	target := filepath.Join(root, clean)
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return "", err
	}
	if targetAbs != rootAbs && !strings.HasPrefix(targetAbs, rootAbs+string(filepath.Separator)) {
		return "", fmt.Errorf("archive path %q escapes destination", name)
	}
	return target, nil
}
