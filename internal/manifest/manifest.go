package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"
)

const Version = 1

type Manifest struct {
	Version   int        `json:"version"`
	JobID     string     `json:"job_id"`
	PartID    string     `json:"part_id"`
	Freeze    string     `json:"freeze"`
	Source    TableRef   `json:"source"`
	Dest      TableRef   `json:"dest"`
	Part      SourcePart `json:"part"`
	SQL       SQLBundle  `json:"sql"`
	S3        S3Refs     `json:"s3"`
	Output    Output     `json:"output,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

type TableRef struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type SourcePart struct {
	Disk         string `json:"disk"`
	Name         string `json:"name"`
	RelativePath string `json:"relative_path"`
}

type SQLBundle struct {
	SourceSchema      string `json:"source_schema"`
	DestinationSchema string `json:"destination_schema"`
	InsertSelect      string `json:"insert_select"`
}

type S3Refs struct {
	Bucket      string `json:"bucket"`
	SourceKey   string `json:"source_key"`
	FinishedKey string `json:"finished_key"`
}

type Output struct {
	Parts []OutputPart `json:"parts,omitempty"`
}

type OutputPart struct {
	Name        string `json:"name"`
	PartitionID string `json:"partition_id"`
}

func (m Manifest) Validate() error {
	if m.Version != Version {
		return Error("unsupported manifest version")
	}
	if m.JobID == "" || m.PartID == "" || m.Part.Disk == "" || m.Part.Name == "" {
		return Error("manifest is missing job_id, part_id, part.disk, or part.name")
	}
	if m.Source.Database == "" || m.Source.Table == "" || m.Dest.Database == "" || m.Dest.Table == "" {
		return Error("manifest is missing source or destination table")
	}
	if strings.TrimSpace(m.SQL.SourceSchema) == "" || strings.TrimSpace(m.SQL.DestinationSchema) == "" || strings.TrimSpace(m.SQL.InsertSelect) == "" {
		return Error("manifest is missing source schema, destination schema, or insert-select")
	}
	if m.S3.Bucket == "" || m.S3.SourceKey == "" || m.S3.FinishedKey == "" {
		return Error("manifest is missing S3 references")
	}
	return nil
}

type Error string

func (e Error) Error() string { return string(e) }

func DeriveJobID(database, table, freeze, sourceSchema, destinationSchema, insertSelect string) string {
	return "job-" + shortHash(database, table, freeze, sourceSchema, destinationSchema, insertSelect)
}

func DerivePartID(disk, relativePath, name, sourceSchema, destinationSchema, insertSelect string) string {
	return "part-" + shortHash(disk, relativePath, name, sourceSchema, destinationSchema, insertSelect)
}

func SourcePartPrefix(prefix, jobID, partID string) string {
	return cleanKey(prefix, "jobs", jobID, "source", partID)
}

func FinishedPartPrefix(prefix, jobID, partID string) string {
	return cleanKey(prefix, "jobs", jobID, "finished", partID)
}

func FinishedPartAttemptPrefix(finishedPartPrefix string, attempt int, finishedAt time.Time) string {
	return cleanKey(
		finishedPartPrefix,
		fmt.Sprintf("attempt-%06d-%s", attempt, finishedAt.UTC().Format("20060102T150405.000000000Z")),
	)
}

func FinishedPrefix(prefix, jobID string) string {
	return cleanKey(prefix, "jobs", jobID, "finished") + "/"
}

func ImportedMarkerKey(prefix, jobID, partID string) string {
	return cleanKey(prefix, "jobs", jobID, "imported", partID+".json")
}

func shortHash(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func cleanKey(elem ...string) string {
	cleaned := make([]string, 0, len(elem))
	for _, e := range elem {
		e = strings.Trim(e, "/")
		if e != "" {
			cleaned = append(cleaned, e)
		}
	}
	return path.Join(cleaned...)
}
