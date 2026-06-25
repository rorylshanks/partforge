package artifact

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/partforge/partforge/internal/manifest"
)

const finishedTarCopyBufferSize = 4 * 1024 * 1024
const finishedTarExtractBufferSize = 1024 * 1024
const defaultFinishedTarExtractWorkers = 256
const tarBlockSize int64 = 512
const maxInt64 = int64(1<<63 - 1)

type finishedTarPlan struct {
	ArchivePath string
	PartNames   []string
	Dirs        []finishedTarDir
	Files       []finishedTarFile
}

type finishedTarDir struct {
	Name      string
	LocalPath string
	Mode      os.FileMode
}

type finishedTarFile struct {
	ArchivePath string
	Archive     *os.File
	Name        string
	LocalPath   string
	Offset      int64
	Size        int64
	Mode        os.FileMode
}

type rawTarHeader struct {
	Name     string
	Typeflag byte
	Size     int64
	Mode     os.FileMode
}

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
	return extractFinishedTarArchivesContext(ctx, []string{tarPath}, destRoot)
}

func ExtractFinishedTarballsContext(ctx context.Context, tarPaths []string, destRoot string) ([]string, error) {
	return extractFinishedTarArchivesContext(ctx, tarPaths, destRoot)
}

func ExtractFinishedTarballsFromDirContext(ctx context.Context, root, destRoot string) ([]string, error) {
	tarPaths, err := finishedTarballPaths(root)
	if err != nil {
		return nil, err
	}
	return ExtractFinishedTarballsContext(ctx, tarPaths, destRoot)
}

func finishedTarballPaths(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		entryPath := filepath.Join(root, entry.Name())
		if entry.IsDir() {
			return nil, fmt.Errorf("unexpected directory at finished artifact root: %s", entryPath)
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("unexpected non-regular file at finished artifact root: %s", entryPath)
		}
		if !strings.HasSuffix(entry.Name(), manifest.FinishedTarSuffix) {
			return nil, fmt.Errorf("unexpected non-tar file at finished artifact root: %s", entryPath)
		}
		paths = append(paths, entryPath)
	}
	sort.Strings(paths)
	return paths, nil
}

func extractFinishedTarArchivesContext(ctx context.Context, tarPaths []string, destRoot string) ([]string, error) {
	if strings.TrimSpace(destRoot) == "" {
		return nil, fmt.Errorf("finished tar extract destination is required")
	}
	if err := os.MkdirAll(destRoot, 0o755); err != nil {
		return nil, err
	}

	partSeen := map[string]string{}
	entryKinds := map[string]string{}
	var partNames []string
	var dirs []finishedTarDir
	var files []finishedTarFile
	for _, tarPath := range tarPaths {
		plan, err := planFinishedTar(tarPath, destRoot)
		if err != nil {
			return nil, fmt.Errorf("plan %s: %w", tarPath, err)
		}
		for _, partName := range plan.PartNames {
			if existingArchive, ok := partSeen[partName]; ok && existingArchive != plan.ArchivePath {
				return nil, fmt.Errorf("duplicate finished part %q in downloaded tarballs", partName)
			}
			if _, ok := partSeen[partName]; !ok {
				partSeen[partName] = plan.ArchivePath
				partNames = append(partNames, partName)
			}
		}
		for _, dir := range plan.Dirs {
			if kind, ok := entryKinds[dir.Name]; ok {
				if kind != "dir" {
					return nil, fmt.Errorf("finished tar entry %q is both %s and directory", dir.Name, kind)
				}
				continue
			}
			entryKinds[dir.Name] = "dir"
			dirs = append(dirs, dir)
		}
		for _, file := range plan.Files {
			if kind, ok := entryKinds[file.Name]; ok {
				return nil, fmt.Errorf("duplicate finished tar entry %q conflicts with %s", file.Name, kind)
			}
			entryKinds[file.Name] = "file"
			files = append(files, file)
		}
	}

	sort.Strings(partNames)
	if err := createFinishedTarDirs(dirs); err != nil {
		return nil, err
	}
	if err := extractFinishedTarFiles(ctx, files); err != nil {
		return nil, err
	}
	if err := chmodFinishedTarDirs(dirs); err != nil {
		return nil, err
	}
	return partNames, nil
}

func createFinishedTarDirs(dirs []finishedTarDir) error {
	sort.Slice(dirs, func(i, j int) bool {
		leftDepth, rightDepth := strings.Count(dirs[i].Name, "/"), strings.Count(dirs[j].Name, "/")
		if leftDepth == rightDepth {
			return dirs[i].Name < dirs[j].Name
		}
		return leftDepth < rightDepth
	})
	for _, dir := range dirs {
		if err := os.MkdirAll(dir.LocalPath, dir.Mode|0o700); err != nil {
			return fmt.Errorf("create finished tar directory %s: %w", dir.LocalPath, err)
		}
	}
	return nil
}

func chmodFinishedTarDirs(dirs []finishedTarDir) error {
	sort.Slice(dirs, func(i, j int) bool {
		leftDepth, rightDepth := strings.Count(dirs[i].Name, "/"), strings.Count(dirs[j].Name, "/")
		if leftDepth == rightDepth {
			return dirs[i].Name > dirs[j].Name
		}
		return leftDepth > rightDepth
	})
	for _, dir := range dirs {
		if err := os.Chmod(dir.LocalPath, dir.Mode); err != nil {
			return fmt.Errorf("chmod finished tar directory %s: %w", dir.LocalPath, err)
		}
	}
	return nil
}

func extractFinishedTarFiles(ctx context.Context, files []finishedTarFile) error {
	if len(files) == 0 {
		return nil
	}
	archives := map[string]*os.File{}
	defer func() {
		for _, archive := range archives {
			_ = archive.Close()
		}
	}()
	for i := range files {
		archive := archives[files[i].ArchivePath]
		if archive == nil {
			f, err := os.Open(files[i].ArchivePath)
			if err != nil {
				return fmt.Errorf("open finished tar %s: %w", files[i].ArchivePath, err)
			}
			archives[files[i].ArchivePath] = f
			archive = f
		}
		files[i].Archive = archive
	}

	workers := defaultFinishedTarExtractWorkers
	if workers > len(files) {
		workers = len(files)
	}
	extractCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan finishedTarFile)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, finishedTarExtractBufferSize)
			for file := range jobs {
				if extractCtx.Err() != nil {
					return
				}
				if err := extractFinishedTarFile(extractCtx, file, buffer); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, file := range files {
			select {
			case jobs <- file:
			case <-extractCtx.Done():
				return
			}
		}
	}()

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		select {
		case err := <-errCh:
			return err
		default:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		cancel()
		<-doneCh
		return err
	case <-ctx.Done():
		cancel()
		<-doneCh
		select {
		case err := <-errCh:
			return err
		default:
			return ctx.Err()
		}
	}
}

func extractFinishedTarFile(ctx context.Context, file finishedTarFile, buffer []byte) error {
	if err := os.MkdirAll(filepath.Dir(file.LocalPath), 0o755); err != nil {
		return fmt.Errorf("create parent directory for %s: %w", file.LocalPath, err)
	}
	dst, err := os.OpenFile(file.LocalPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, file.Mode)
	if err != nil {
		return fmt.Errorf("create finished tar file %s: %w", file.LocalPath, err)
	}
	removeDst := true
	defer func() {
		if removeDst {
			_ = os.Remove(file.LocalPath)
		}
	}()
	if file.Size > 0 {
		if err := dst.Truncate(file.Size); err != nil {
			_ = dst.Close()
			return fmt.Errorf("size finished tar file %s: %w", file.LocalPath, err)
		}
	}
	written, err := copyFinishedTarPayload(ctx, dst, file.Archive, file.Offset, file.Size, buffer)
	if err != nil {
		_ = dst.Close()
		return fmt.Errorf("extract finished tar file %s: %w", file.LocalPath, err)
	}
	if written != file.Size {
		_ = dst.Close()
		return fmt.Errorf("extract finished tar file %s: wrote %d bytes, want %d", file.LocalPath, written, file.Size)
	}
	if err := dst.Chmod(file.Mode); err != nil {
		_ = dst.Close()
		return fmt.Errorf("chmod finished tar file %s: %w", file.LocalPath, err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("close finished tar file %s: %w", file.LocalPath, err)
	}
	removeDst = false
	return nil
}

func copyFinishedTarPayload(ctx context.Context, dst, src *os.File, offset, size int64, buffer []byte) (int64, error) {
	var written int64
	for written < size {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		chunk := int64(len(buffer))
		if remaining := size - written; remaining < chunk {
			chunk = remaining
		}
		nr, readErr := src.ReadAt(buffer[:chunk], offset+written)
		if nr > 0 {
			nw, writeErr := dst.Write(buffer[:nr])
			written += int64(nw)
			if writeErr != nil {
				return written, writeErr
			}
			if nw != nr {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			return written, readErr
		}
		if nr == 0 {
			return written, io.ErrUnexpectedEOF
		}
	}
	return written, nil
}

func planFinishedTar(tarPath, destRoot string) (finishedTarPlan, error) {
	f, err := os.Open(tarPath)
	if err != nil {
		return finishedTarPlan{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return finishedTarPlan{}, err
	}
	size := info.Size()

	plan := finishedTarPlan{ArchivePath: tarPath}
	partSeen := map[string]struct{}{}
	var offset int64
	var pendingPAX map[string]string
	var globalPAX map[string]string
	var pendingLongName string
	for {
		if offset+tarBlockSize > size {
			if offset == size {
				break
			}
			return finishedTarPlan{}, fmt.Errorf("truncated tar header at offset %d", offset)
		}
		var block [tarBlockSize]byte
		if _, err := f.ReadAt(block[:], offset); err != nil {
			return finishedTarPlan{}, err
		}
		if isZeroTarBlock(block[:]) {
			break
		}
		headerOffset := offset
		offset += tarBlockSize
		header, err := parseRawTarHeader(block[:])
		if err != nil {
			return finishedTarPlan{}, fmt.Errorf("parse tar header at offset %d: %w", headerOffset, err)
		}
		paddedSize, err := paddedTarSize(header.Size)
		if err != nil {
			return finishedTarPlan{}, fmt.Errorf("tar entry %q size %d: %w", header.Name, header.Size, err)
		}
		dataOffset := offset
		nextOffset := dataOffset + paddedSize
		if nextOffset < dataOffset || nextOffset > size {
			return finishedTarPlan{}, fmt.Errorf("tar entry %q extends past archive end", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeXHeader:
			records, err := readPAXRecords(f, dataOffset, header.Size)
			if err != nil {
				return finishedTarPlan{}, fmt.Errorf("read pax header %q: %w", header.Name, err)
			}
			pendingPAX = records
			offset = nextOffset
			continue
		case tar.TypeXGlobalHeader:
			records, err := readPAXRecords(f, dataOffset, header.Size)
			if err != nil {
				return finishedTarPlan{}, fmt.Errorf("read global pax header %q: %w", header.Name, err)
			}
			if globalPAX == nil {
				globalPAX = map[string]string{}
			}
			for key, value := range records {
				globalPAX[key] = value
			}
			offset = nextOffset
			continue
		case tar.TypeGNULongName:
			name, err := readTarStringPayload(f, dataOffset, header.Size)
			if err != nil {
				return finishedTarPlan{}, fmt.Errorf("read gnu long name: %w", err)
			}
			pendingLongName = name
			offset = nextOffset
			continue
		case tar.TypeGNULongLink:
			offset = nextOffset
			continue
		}

		entryName := header.Name
		if pendingLongName != "" {
			entryName = pendingLongName
			pendingLongName = ""
		}
		records := mergedPAXRecords(globalPAX, pendingPAX)
		pendingPAX = nil
		if paxPath := records["path"]; paxPath != "" {
			entryName = paxPath
		}
		entrySize := header.Size
		if paxSize := records["size"]; paxSize != "" {
			parsedSize, err := strconv.ParseInt(paxSize, 10, 64)
			if err != nil || parsedSize < 0 {
				return finishedTarPlan{}, fmt.Errorf("invalid pax size %q for %q", paxSize, entryName)
			}
			entrySize = parsedSize
			paddedSize, err = paddedTarSize(entrySize)
			if err != nil {
				return finishedTarPlan{}, fmt.Errorf("tar entry %q size %d: %w", entryName, entrySize, err)
			}
			nextOffset = dataOffset + paddedSize
			if nextOffset < dataOffset || nextOffset > size {
				return finishedTarPlan{}, fmt.Errorf("tar entry %q extends past archive end", entryName)
			}
		}

		cleanName, partName, err := cleanFinishedTarEntryName(entryName)
		if err != nil {
			return finishedTarPlan{}, err
		}
		if _, ok := partSeen[partName]; !ok {
			partSeen[partName] = struct{}{}
			plan.PartNames = append(plan.PartNames, partName)
		}
		localPath := filepath.Join(destRoot, filepath.FromSlash(cleanName))
		switch header.Typeflag {
		case tar.TypeDir:
			plan.Dirs = append(plan.Dirs, finishedTarDir{Name: cleanName, LocalPath: localPath, Mode: header.Mode})
		case tar.TypeReg, tar.TypeRegA:
			plan.Files = append(plan.Files, finishedTarFile{
				ArchivePath: tarPath,
				Name:        cleanName,
				LocalPath:   localPath,
				Offset:      dataOffset,
				Size:        entrySize,
				Mode:        header.Mode,
			})
		default:
			return finishedTarPlan{}, fmt.Errorf("unsupported finished tar entry type %c for %s", header.Typeflag, entryName)
		}
		offset = nextOffset
	}

	sort.Strings(plan.PartNames)
	return plan, nil
}

func parseRawTarHeader(block []byte) (rawTarHeader, error) {
	if err := verifyTarHeaderChecksum(block); err != nil {
		return rawTarHeader{}, err
	}
	name := tarString(block[0:100])
	prefix := tarString(block[345:500])
	if prefix != "" {
		name = prefix + "/" + name
	}
	modeValue, err := parseTarNumeric(block[100:108])
	if err != nil {
		return rawTarHeader{}, fmt.Errorf("parse mode: %w", err)
	}
	sizeValue, err := parseTarNumeric(block[124:136])
	if err != nil {
		return rawTarHeader{}, fmt.Errorf("parse size: %w", err)
	}
	if sizeValue < 0 {
		return rawTarHeader{}, fmt.Errorf("negative size %d", sizeValue)
	}
	typeflag := block[156]
	if typeflag == tar.TypeRegA && strings.HasSuffix(name, "/") {
		typeflag = tar.TypeDir
	}
	return rawTarHeader{
		Name:     name,
		Typeflag: typeflag,
		Size:     sizeValue,
		Mode:     os.FileMode(modeValue & 0o7777),
	}, nil
}

func verifyTarHeaderChecksum(block []byte) error {
	expected, err := parseTarOctal(block[148:156])
	if err != nil {
		return fmt.Errorf("parse checksum: %w", err)
	}
	var actual int64
	for i, b := range block {
		if i >= 148 && i < 156 {
			actual += int64(' ')
			continue
		}
		actual += int64(b)
	}
	if expected != actual {
		return fmt.Errorf("checksum mismatch: got %d, want %d", actual, expected)
	}
	return nil
}

func parseTarNumeric(field []byte) (int64, error) {
	if len(field) > 0 && field[0]&0x80 != 0 {
		return parseTarBase256(field)
	}
	return parseTarOctal(field)
}

func parseTarOctal(field []byte) (int64, error) {
	field = bytes.Trim(field, "\x00 ")
	if len(field) == 0 {
		return 0, nil
	}
	var value int64
	for _, b := range field {
		if b < '0' || b > '7' {
			return 0, fmt.Errorf("invalid octal byte %q", b)
		}
		if value > (maxInt64-int64(b-'0'))/8 {
			return 0, fmt.Errorf("octal value overflows int64")
		}
		value = value*8 + int64(b-'0')
	}
	return value, nil
}

func parseTarBase256(field []byte) (int64, error) {
	if len(field) == 0 {
		return 0, nil
	}
	if field[0]&0x40 != 0 {
		return 0, fmt.Errorf("negative base-256 value is unsupported")
	}
	value := int64(field[0] & 0x7f)
	for _, b := range field[1:] {
		if value > (maxInt64-int64(b))/256 {
			return 0, fmt.Errorf("base-256 value overflows int64")
		}
		value = value*256 + int64(b)
	}
	return value, nil
}

func isZeroTarBlock(block []byte) bool {
	for _, b := range block {
		if b != 0 {
			return false
		}
	}
	return true
}

func paddedTarSize(size int64) (int64, error) {
	if size < 0 {
		return 0, fmt.Errorf("negative size")
	}
	if size > maxInt64-(tarBlockSize-1) {
		return 0, fmt.Errorf("size overflows tar padding")
	}
	return (size + tarBlockSize - 1) / tarBlockSize * tarBlockSize, nil
}

func tarString(field []byte) string {
	if i := bytes.IndexByte(field, 0); i >= 0 {
		field = field[:i]
	}
	return string(field)
}

func readPAXRecords(f *os.File, offset, size int64) (map[string]string, error) {
	payload, err := readTarPayload(f, offset, size)
	if err != nil {
		return nil, err
	}
	records := map[string]string{}
	for len(payload) > 0 {
		space := bytes.IndexByte(payload, ' ')
		if space <= 0 {
			return nil, fmt.Errorf("malformed pax record length")
		}
		recordLen, err := strconv.Atoi(string(payload[:space]))
		if err != nil {
			return nil, fmt.Errorf("parse pax record length: %w", err)
		}
		if recordLen <= space+1 || recordLen > len(payload) {
			return nil, fmt.Errorf("invalid pax record length %d", recordLen)
		}
		record := payload[space+1 : recordLen]
		if len(record) == 0 || record[len(record)-1] != '\n' {
			return nil, fmt.Errorf("pax record missing newline")
		}
		record = record[:len(record)-1]
		equals := bytes.IndexByte(record, '=')
		if equals <= 0 {
			return nil, fmt.Errorf("pax record missing key")
		}
		records[string(record[:equals])] = string(record[equals+1:])
		payload = payload[recordLen:]
	}
	return records, nil
}

func readTarStringPayload(f *os.File, offset, size int64) (string, error) {
	payload, err := readTarPayload(f, offset, size)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimRight(payload, "\x00")), nil
}

func readTarPayload(f *os.File, offset, size int64) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative payload size %d", size)
	}
	if size > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("payload size %d exceeds addressable memory", size)
	}
	payload := make([]byte, int(size))
	if len(payload) == 0 {
		return payload, nil
	}
	if _, err := f.ReadAt(payload, offset); err != nil {
		return nil, err
	}
	return payload, nil
}

func mergedPAXRecords(globalPAX, pendingPAX map[string]string) map[string]string {
	if len(globalPAX) == 0 && len(pendingPAX) == 0 {
		return nil
	}
	records := make(map[string]string, len(globalPAX)+len(pendingPAX))
	for key, value := range globalPAX {
		records[key] = value
	}
	for key, value := range pendingPAX {
		records[key] = value
	}
	return records
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
