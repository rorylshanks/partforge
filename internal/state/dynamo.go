package state

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	StatusReady        Status = "READY"
	StatusInProgress   Status = "IN_PROGRESS"
	StatusCompactReady Status = "COMPACT_READY"
	StatusCompacting   Status = "COMPACTING"
	StatusSuperseded   Status = "SUPERSEDED"
	StatusFinished     Status = "FINISHED"
	StatusImporting    Status = "IMPORTING"
	StatusImported     Status = "IMPORTED"
	StatusFailed       Status = "FAILED"

	readyIndexName = "gsi1"
	timeFormat     = "2006-01-02T15:04:05.000000000Z"
	defaultRegion  = "us-east-1"
)

type Status string

type Config struct {
	Region   string
	Endpoint string
	Table    string
}

type Store struct {
	client *dynamodb.Client
	table  string
}

type Part struct {
	PK             string `dynamodbav:"pk"`
	SK             string `dynamodbav:"sk"`
	GSI1PK         string `dynamodbav:"gsi1pk"`
	GSI1SK         string `dynamodbav:"gsi1sk"`
	JobID          string `dynamodbav:"job_id"`
	PartID         string `dynamodbav:"part_id"`
	Status         Status `dynamodbav:"status"`
	Bucket         string `dynamodbav:"bucket"`
	SourceKey      string `dynamodbav:"source_key"`
	FinishedKey    string `dynamodbav:"finished_key"`
	CreatedAt      string `dynamodbav:"created_at"`
	UpdatedAt      string `dynamodbav:"updated_at"`
	StartedAt      string `dynamodbav:"started_at,omitempty"`
	FinishedAt     string `dynamodbav:"finished_at,omitempty"`
	CompactReadyAt string `dynamodbav:"compact_ready_at,omitempty"`
	CompactingAt   string `dynamodbav:"compacting_at,omitempty"`
	SupersededAt   string `dynamodbav:"superseded_at,omitempty"`
	ImportingAt    string `dynamodbav:"importing_at,omitempty"`
	ImportedAt     string `dynamodbav:"imported_at,omitempty"`
	FailedAt       string `dynamodbav:"failed_at,omitempty"`
	WorkerID       string `dynamodbav:"worker_id,omitempty"`
	Attempts       int    `dynamodbav:"attempts"`
	Error          string `dynamodbav:"error,omitempty"`

	DestinationDatabase  string   `dynamodbav:"destination_database,omitempty"`
	DestinationTable     string   `dynamodbav:"destination_table,omitempty"`
	DestinationSchema    string   `dynamodbav:"destination_schema,omitempty"`
	CompactGeneration    int      `dynamodbav:"compact_generation,omitempty"`
	CompactInputPartIDs  []string `dynamodbav:"compact_input_part_ids,omitempty"`
	CompactCooldownUntil string   `dynamodbav:"compact_cooldown_until,omitempty"`
	SupersededBy         string   `dynamodbav:"superseded_by,omitempty"`

	CompactOutputPartID    string `dynamodbav:"compact_output_part_id,omitempty"`
	CompactProgressAt      string `dynamodbav:"compact_progress_at,omitempty"`
	CompactInputPartCount  uint64 `dynamodbav:"compact_input_part_count,omitempty"`
	CompactInputRows       uint64 `dynamodbav:"compact_input_rows,omitempty"`
	CompactInputBytes      uint64 `dynamodbav:"compact_input_bytes,omitempty"`
	CompactOutputPartCount uint64 `dynamodbav:"compact_output_part_count,omitempty"`
	CompactOutputRows      uint64 `dynamodbav:"compact_output_rows,omitempty"`
	CompactOutputBytes     uint64 `dynamodbav:"compact_output_bytes,omitempty"`

	ProgressUpdatedAt                string            `dynamodbav:"progress_updated_at,omitempty"`
	ReadRows                         uint64            `dynamodbav:"read_rows,omitempty"`
	ReadBytes                        uint64            `dynamodbav:"read_bytes,omitempty"`
	WrittenRows                      uint64            `dynamodbav:"written_rows,omitempty"`
	WrittenBytes                     uint64            `dynamodbav:"written_bytes,omitempty"`
	SourceActivePartCount            uint64            `dynamodbav:"source_active_part_count,omitempty"`
	SourceActivePartRows             uint64            `dynamodbav:"source_active_part_rows,omitempty"`
	SourceActivePartBytes            uint64            `dynamodbav:"source_active_part_bytes,omitempty"`
	DestinationActivePartCount       uint64            `dynamodbav:"destination_active_part_count,omitempty"`
	DestinationActivePartRows        uint64            `dynamodbav:"destination_active_part_rows,omitempty"`
	DestinationActivePartBytes       uint64            `dynamodbav:"destination_active_part_bytes,omitempty"`
	DestinationActivePartitionCounts map[string]uint64 `dynamodbav:"destination_active_partition_counts,omitempty"`
	DestinationFailedMerges          uint64            `dynamodbav:"destination_failed_merges,omitempty"`
	RewriteStage                     string            `dynamodbav:"rewrite_stage,omitempty"`
	RewriteStageStartedAt            string            `dynamodbav:"rewrite_stage_started_at,omitempty"`
	RewriteStageElapsedMs            int64             `dynamodbav:"rewrite_stage_elapsed_ms,omitempty"`
	RewriteTotalElapsedMs            int64             `dynamodbav:"rewrite_total_elapsed_ms,omitempty"`
	RewriteStageDurationsMs          map[string]int64  `dynamodbav:"rewrite_stage_durations_ms,omitempty"`
}

type QueryProgress struct {
	ReadRows     uint64
	ReadBytes    uint64
	WrittenRows  uint64
	WrittenBytes uint64
}

type PartStats struct {
	Count uint64
	Rows  uint64
	Bytes uint64
}

func clonePartitionCounts(counts map[string]uint64) map[string]uint64 {
	if len(counts) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(counts))
	for partitionID, count := range counts {
		if strings.TrimSpace(partitionID) == "" || count == 0 {
			continue
		}
		out[partitionID] = count
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type CompactClaimOptions struct {
	MaxArtifacts         int
	MaxBytes             uint64
	MinInputParts        uint64
	JobID                string
	Bucket               string
	DestinationDatabase  string
	DestinationTable     string
	DestinationSchema    string
	RequiredPartitionIDs []string
}

type CompactBatch struct {
	JobID          string
	Parts          []Part
	InputPartCount uint64
	InputRows      uint64
	InputBytes     uint64
	Generation     int
}

type RewriteProgress struct {
	QueryProgress              *QueryProgress
	SourceActivePartStats      *PartStats
	DestinationActivePartStats *PartStats
	DestinationFailedMerges    *uint64
	StageProgress              *RewriteStageProgress
}

type RewriteStageProgress struct {
	Stage                     string
	StageStartedAt            time.Time
	StageElapsedMs            int64
	TotalElapsedMs            int64
	CompletedStageDurationsMs map[string]int64
}

func New(ctx context.Context, cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.Table) == "" {
		return nil, errors.New("state table is required")
	}
	awsCfg, err := loadAWSConfig(ctx, cfg.Region)
	if err != nil {
		return nil, err
	}
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	return &Store{client: client, table: cfg.Table}, nil
}

func loadAWSConfig(ctx context.Context, region string) (aws.Config, error) {
	loadOptions := []func(*config.LoadOptions) error{
		config.WithRetryMaxAttempts(1),
	}
	if strings.TrimSpace(region) != "" {
		loadOptions = append(loadOptions, config.WithRegion(strings.TrimSpace(region)))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return aws.Config{}, err
	}
	if strings.TrimSpace(region) == "" && awsCfg.Region == "" {
		awsCfg.Region = resolveDynamoRegion(ctx, awsCfg, defaultRegion, imdsRegion)
	}
	return awsCfg, nil
}

func resolveDynamoRegion(ctx context.Context, awsCfg aws.Config, fallback string, getRegion func(context.Context, aws.Config) (string, error)) string {
	if strings.TrimSpace(awsCfg.Region) != "" {
		return strings.TrimSpace(awsCfg.Region)
	}
	region, err := getRegion(ctx, awsCfg)
	if err != nil || strings.TrimSpace(region) == "" {
		return fallback
	}
	return strings.TrimSpace(region)
}

func imdsRegion(ctx context.Context, awsCfg aws.Config) (string, error) {
	imdsCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	out, err := imds.NewFromConfig(awsCfg).GetRegion(imdsCtx, nil)
	if err != nil {
		return "", err
	}
	return out.Region, nil
}

func NewPart(jobID, partID, bucket, sourceKey, finishedKey string, now time.Time) Part {
	createdAt := formatTime(now)
	return Part{
		PK:          jobKey(jobID),
		SK:          partKey(partID),
		GSI1PK:      statusKey(StatusReady),
		GSI1SK:      statusSortKey(createdAt, jobID, partID),
		JobID:       jobID,
		PartID:      partID,
		Status:      StatusReady,
		Bucket:      bucket,
		SourceKey:   sourceKey,
		FinishedKey: finishedKey,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
}

func NewCompactPart(jobID, partID, bucket, finishedKey, database, table, destinationSchema string, inputPartIDs []string, generation int, stats PartStats, partitionCounts map[string]uint64, now time.Time) Part {
	createdAt := formatTime(now)
	return Part{
		PK:                               jobKey(jobID),
		SK:                               partKey(partID),
		GSI1PK:                           statusKey(StatusCompactReady),
		GSI1SK:                           statusSortKey(createdAt, jobID, partID),
		JobID:                            jobID,
		PartID:                           partID,
		Status:                           StatusCompactReady,
		Bucket:                           bucket,
		SourceKey:                        finishedKey,
		FinishedKey:                      finishedKey,
		CreatedAt:                        createdAt,
		UpdatedAt:                        createdAt,
		CompactReadyAt:                   createdAt,
		DestinationDatabase:              database,
		DestinationTable:                 table,
		DestinationSchema:                destinationSchema,
		CompactGeneration:                generation,
		CompactInputPartIDs:              append([]string(nil), inputPartIDs...),
		DestinationActivePartCount:       stats.Count,
		DestinationActivePartRows:        stats.Rows,
		DestinationActivePartBytes:       stats.Bytes,
		DestinationActivePartitionCounts: clonePartitionCounts(partitionCounts),
	}
}

func (s *Store) CreatePart(ctx context.Context, part Part) error {
	if err := validatePart(part); err != nil {
		return err
	}
	item, err := attributevalue.MarshalMap(part)
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.table),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk) AND attribute_not_exists(sk)"),
	})
	if err != nil {
		return fmt.Errorf("create state item for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) MarkCompactReady(ctx context.Context, part Part, workerID, finishedKey, database, table, destinationSchema string, stats PartStats, partitionCounts map[string]uint64, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if strings.TrimSpace(finishedKey) == "" {
		return errors.New("finished key is required")
	}
	if strings.TrimSpace(database) == "" || strings.TrimSpace(table) == "" || strings.TrimSpace(destinationSchema) == "" {
		return errors.New("destination database, table, and schema are required")
	}
	if stats.Count > 0 && len(partitionCounts) == 0 {
		return fmt.Errorf("destination partition counts are required when destination active part count is %d", stats.Count)
	}
	partitionCountsValue, err := attributevalue.Marshal(clonePartitionCounts(partitionCounts))
	if err != nil {
		return fmt.Errorf("marshal destination partition counts: %w", err)
	}
	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :from AND #worker_id = :worker",
		),
		UpdateExpression: aws.String(
			"SET #status = :to, gsi1pk = :gsi1pk, updated_at = :now, finished_key = :finished_key, " +
				"compact_ready_at = :now, " +
				"destination_database = :destination_database, destination_table = :destination_table, destination_schema = :destination_schema, compact_generation = :compact_generation, " +
				"destination_active_part_count = :destination_active_part_count, destination_active_part_rows = :destination_active_part_rows, destination_active_part_bytes = :destination_active_part_bytes, " +
				"destination_active_partition_counts = :destination_active_partition_counts " +
				"REMOVE #worker_id, #error, compact_cooldown_until",
		),
		ExpressionAttributeNames: map[string]string{
			"#error":     "error",
			"#status":    "status",
			"#worker_id": "worker_id",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":compact_generation":                  numberAttr("0"),
			":destination_active_part_count":       uintAttr(stats.Count),
			":destination_active_part_rows":        uintAttr(stats.Rows),
			":destination_active_part_bytes":       uintAttr(stats.Bytes),
			":destination_active_partition_counts": partitionCountsValue,
			":destination_database":                stringAttr(database),
			":destination_schema":                  stringAttr(destinationSchema),
			":destination_table":                   stringAttr(table),
			":finished_key":                        stringAttr(finishedKey),
			":from":                                stringAttr(string(StatusInProgress)),
			":gsi1pk":                              stringAttr(statusKey(StatusCompactReady)),
			":now":                                 stringAttr(formatTime(now)),
			":to":                                  stringAttr(string(StatusCompactReady)),
			":worker":                              stringAttr(workerID),
		},
	})
	if err != nil {
		return fmt.Errorf("mark part %s/%s compact ready: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) ClaimNextReady(ctx context.Context, workerID string, now time.Time) (*Part, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("worker id is required")
	}
	paginator := dynamodb.NewQueryPaginator(s.client, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(readyIndexName),
		KeyConditionExpression: aws.String("#gsi1pk = :ready"),
		ExpressionAttributeNames: map[string]string{
			"#gsi1pk": "gsi1pk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ready": stringAttr(statusKey(StatusReady)),
		},
		Limit: aws.Int32(25),
	})

	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("query ready parts: %w", err)
		}
		for _, item := range out.Items {
			part, err := unmarshalPart(item)
			if err != nil {
				return nil, err
			}
			claimed, err := s.claimPart(ctx, *part, workerID, now)
			if IsConditionalCheckFailed(err) {
				continue
			}
			if err != nil {
				return nil, err
			}
			return claimed, nil
		}
	}
	return nil, nil
}

func (s *Store) ClaimNextCompactBatch(ctx context.Context, workerID string, now time.Time, opts CompactClaimOptions) (*CompactBatch, error) {
	if strings.TrimSpace(workerID) == "" {
		return nil, errors.New("worker id is required")
	}
	if opts.MaxArtifacts < 0 {
		return nil, fmt.Errorf("compact max artifacts must be non-negative, got %d", opts.MaxArtifacts)
	}

	candidates, err := s.listPartsByStatusIndex(ctx, StatusCompactReady)
	if err != nil {
		return nil, fmt.Errorf("query compact-ready parts: %w", err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	compacting, err := s.listPartsByStatusIndex(ctx, StatusCompacting)
	if err != nil {
		return nil, fmt.Errorf("query compacting parts: %w", err)
	}

	groups := compactCandidateGroups(candidates, compacting, now, opts)
	for _, group := range groups {
		selected := selectCompactBatchParts(group, opts)
		if len(selected) == 0 {
			continue
		}
		claimed, err := s.claimCompactParts(ctx, selected, workerID, now)
		if IsConditionalCheckFailed(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return compactBatchFromParts(claimed), nil
	}
	return nil, nil
}

func (s *Store) listPartsByStatusIndex(ctx context.Context, status Status) ([]Part, error) {
	paginator := dynamodb.NewQueryPaginator(s.client, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		IndexName:              aws.String(readyIndexName),
		KeyConditionExpression: aws.String("#gsi1pk = :status"),
		ExpressionAttributeNames: map[string]string{
			"#gsi1pk": "gsi1pk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": stringAttr(statusKey(status)),
		},
		Limit: aws.Int32(100),
	})

	var parts []Part
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			part, err := unmarshalPart(item)
			if err != nil {
				return nil, err
			}
			parts = append(parts, *part)
		}
	}
	return parts, nil
}

func (s *Store) ReleaseCompactBatch(ctx context.Context, batch CompactBatch, workerID string, cooldownUntil time.Time, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	for _, part := range batch.Parts {
		names := map[string]string{
			"#error":     "error",
			"#status":    "status",
			"#worker_id": "worker_id",
		}
		values := map[string]types.AttributeValue{
			":compact_ready":    stringAttr(string(StatusCompactReady)),
			":compact_ready_at": stringAttr(compactReadyAtForRelease(part, now)),
			":compacting":       stringAttr(string(StatusCompacting)),
			":gsi1pk":           stringAttr(statusKey(StatusCompactReady)),
			":now":              stringAttr(formatTime(now)),
			":worker":           stringAttr(workerID),
		}
		updateExpression := "SET #status = :compact_ready, gsi1pk = :gsi1pk, updated_at = :now, compact_ready_at = if_not_exists(compact_ready_at, :compact_ready_at)"
		remove := append([]string{"#worker_id", "compacting_at", "#error"}, compactProgressRemoveAttributes()...)
		if !cooldownUntil.IsZero() {
			updateExpression += ", compact_cooldown_until = :cooldown_until"
			values[":cooldown_until"] = stringAttr(formatTime(cooldownUntil))
		} else {
			remove = append(remove, "compact_cooldown_until")
		}
		updateExpression += " REMOVE " + strings.Join(remove, ", ")

		_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:                 aws.String(s.table),
			Key:                       part.key(),
			ConditionExpression:       aws.String("#status = :compacting AND #worker_id = :worker"),
			UpdateExpression:          aws.String(updateExpression),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		})
		if err != nil {
			return fmt.Errorf("release compacting part %s/%s: %w", part.JobID, part.PartID, err)
		}
	}
	return nil
}

func (s *Store) HeartbeatCompactBatch(ctx context.Context, batch CompactBatch, workerID string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	for _, part := range batch.Parts {
		_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(s.table),
			Key:       part.key(),
			ConditionExpression: aws.String(
				"#status = :compacting AND #worker_id = :worker",
			),
			UpdateExpression: aws.String(
				"SET updated_at = :now",
			),
			ExpressionAttributeNames: map[string]string{
				"#status":    "status",
				"#worker_id": "worker_id",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":compacting": stringAttr(string(StatusCompacting)),
				":now":        stringAttr(formatTime(now)),
				":worker":     stringAttr(workerID),
			},
		})
		if err != nil {
			return fmt.Errorf("heartbeat compacting part %s/%s: %w", part.JobID, part.PartID, err)
		}
	}
	return nil
}

func (s *Store) UpdateCompactProgress(ctx context.Context, batch CompactBatch, outputPartID, workerID string, inputStats, outputStats PartStats, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if strings.TrimSpace(outputPartID) == "" {
		return errors.New("compact output part id is required")
	}
	for _, part := range batch.Parts {
		_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(s.table),
			Key:       part.key(),
			ConditionExpression: aws.String(
				"#status = :compacting AND #worker_id = :worker",
			),
			UpdateExpression: aws.String(
				"SET updated_at = :now, compact_progress_at = :now, compact_output_part_id = :output_part_id, " +
					"compact_input_part_count = :input_parts, compact_input_rows = :input_rows, compact_input_bytes = :input_bytes, " +
					"compact_output_part_count = :output_parts, compact_output_rows = :output_rows, compact_output_bytes = :output_bytes",
			),
			ExpressionAttributeNames: map[string]string{
				"#status":    "status",
				"#worker_id": "worker_id",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":compacting":     stringAttr(string(StatusCompacting)),
				":input_bytes":    uintAttr(inputStats.Bytes),
				":input_parts":    uintAttr(inputStats.Count),
				":input_rows":     uintAttr(inputStats.Rows),
				":now":            stringAttr(formatTime(now)),
				":output_bytes":   uintAttr(outputStats.Bytes),
				":output_part_id": stringAttr(outputPartID),
				":output_parts":   uintAttr(outputStats.Count),
				":output_rows":    uintAttr(outputStats.Rows),
				":worker":         stringAttr(workerID),
			},
		})
		if err != nil {
			return fmt.Errorf("update compact progress for %s/%s: %w", part.JobID, part.PartID, err)
		}
	}
	return nil
}

func (s *Store) ReleaseStaleCompactingParts(ctx context.Context, now time.Time, staleAfter time.Duration, cooldownUntil time.Time) (int, error) {
	if staleAfter <= 0 {
		return 0, fmt.Errorf("compact stale timeout must be greater than zero, got %s", staleAfter)
	}
	parts, err := s.listPartsByStatusIndex(ctx, StatusCompacting)
	if err != nil {
		return 0, fmt.Errorf("query compacting parts: %w", err)
	}
	cutoff := now.Add(-staleAfter)
	released := 0
	for _, part := range parts {
		heartbeatAt, err := compactHeartbeatTime(part)
		if err != nil {
			return released, err
		}
		if heartbeatAt.After(cutoff) {
			continue
		}
		ok, err := s.releaseStaleCompactingPart(ctx, part, cooldownUntil, now)
		if err != nil {
			return released, err
		}
		if ok {
			released++
		}
	}
	return released, nil
}

func (s *Store) releaseStaleCompactingPart(ctx context.Context, part Part, cooldownUntil time.Time, now time.Time) (bool, error) {
	names := map[string]string{
		"#error":  "error",
		"#status": "status",
	}
	values := map[string]types.AttributeValue{
		":compact_ready":    stringAttr(string(StatusCompactReady)),
		":compact_ready_at": stringAttr(compactReadyAtForRelease(part, now)),
		":compacting":       stringAttr(string(StatusCompacting)),
		":gsi1pk":           stringAttr(statusKey(StatusCompactReady)),
		":now":              stringAttr(formatTime(now)),
		":updated_at":       stringAttr(part.UpdatedAt),
	}
	updateExpression := "SET #status = :compact_ready, gsi1pk = :gsi1pk, updated_at = :now, compact_ready_at = if_not_exists(compact_ready_at, :compact_ready_at)"
	remove := append([]string{"worker_id", "compacting_at", "#error"}, compactProgressRemoveAttributes()...)
	if !cooldownUntil.IsZero() {
		updateExpression += ", compact_cooldown_until = :cooldown_until"
		values[":cooldown_until"] = stringAttr(formatTime(cooldownUntil))
	} else {
		remove = append(remove, "compact_cooldown_until")
	}
	updateExpression += " REMOVE " + strings.Join(remove, ", ")

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       part.key(),
		ConditionExpression:       aws.String("#status = :compacting AND updated_at = :updated_at"),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if IsConditionalCheckFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("release stale compacting part %s/%s: %w", part.JobID, part.PartID, err)
	}
	return true, nil
}

func (s *Store) CompleteCompaction(ctx context.Context, batch CompactBatch, output Part, workerID string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if len(batch.Parts) == 0 {
		return errors.New("compact batch has no input parts")
	}
	if len(batch.Parts) > 99 {
		return fmt.Errorf("compact batch has %d input parts, exceeds DynamoDB transaction limit", len(batch.Parts))
	}
	if err := validatePart(output); err != nil {
		return err
	}
	if output.Status != StatusCompactReady {
		return fmt.Errorf("compact output %s/%s is %s, expected %s", output.JobID, output.PartID, output.Status, StatusCompactReady)
	}

	outputItem, err := attributevalue.MarshalMap(output)
	if err != nil {
		return err
	}
	items := []types.TransactWriteItem{
		{
			Put: &types.Put{
				TableName:           aws.String(s.table),
				Item:                outputItem,
				ConditionExpression: aws.String("attribute_not_exists(pk) AND attribute_not_exists(sk)"),
			},
		},
	}
	for _, part := range batch.Parts {
		items = append(items, types.TransactWriteItem{
			Update: &types.Update{
				TableName: aws.String(s.table),
				Key:       part.key(),
				ConditionExpression: aws.String(
					"#status = :compacting AND #worker_id = :worker",
				),
				UpdateExpression: aws.String(
					"SET #status = :superseded, gsi1pk = :gsi1pk, updated_at = :now, superseded_at = :now, superseded_by = :superseded_by REMOVE #worker_id, compacting_at, #error, compact_cooldown_until, " + strings.Join(compactProgressRemoveAttributes(), ", "),
				),
				ExpressionAttributeNames: map[string]string{
					"#error":     "error",
					"#status":    "status",
					"#worker_id": "worker_id",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":compacting":    stringAttr(string(StatusCompacting)),
					":gsi1pk":        stringAttr(statusKey(StatusSuperseded)),
					":now":           stringAttr(formatTime(now)),
					":superseded":    stringAttr(string(StatusSuperseded)),
					":superseded_by": stringAttr(output.PartID),
					":worker":        stringAttr(workerID),
				},
			},
		})
	}
	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		return fmt.Errorf("complete compaction for %s/%s: %w", batch.JobID, output.PartID, err)
	}
	return nil
}

func (s *Store) MarkCompactReadyFinished(ctx context.Context, part Part, now time.Time) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :compact_ready",
		),
		UpdateExpression: aws.String(
			"SET #status = :finished, gsi1pk = :gsi1pk, updated_at = :now, finished_at = :now REMOVE #error, compact_cooldown_until",
		),
		ExpressionAttributeNames: map[string]string{
			"#error":  "error",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":compact_ready": stringAttr(string(StatusCompactReady)),
			":finished":      stringAttr(string(StatusFinished)),
			":gsi1pk":        stringAttr(statusKey(StatusFinished)),
			":now":           stringAttr(formatTime(now)),
		},
	})
	if err != nil {
		return fmt.Errorf("mark compact-ready part %s/%s finished: %w", part.JobID, part.PartID, err)
	}
	return nil
}

type compactGroup struct {
	key                    string
	parts                  []Part
	compactingPartitionIDs []string
}

func compactCandidateGroups(parts, compacting []Part, now time.Time, opts CompactClaimOptions) []compactGroup {
	groupsByKey := map[string][]Part{}
	var order []string
	for _, part := range parts {
		if strings.TrimSpace(part.DestinationDatabase) == "" ||
			strings.TrimSpace(part.DestinationTable) == "" ||
			strings.TrimSpace(part.DestinationSchema) == "" ||
			part.DestinationActivePartCount == 0 ||
			len(part.DestinationActivePartitionCounts) == 0 ||
			!matchesCompactClaimOptions(part, opts) ||
			compactCooldownActive(part, now) {
			continue
		}
		key := compactGroupKey(part)
		if _, ok := groupsByKey[key]; !ok {
			order = append(order, key)
		}
		groupsByKey[key] = append(groupsByKey[key], part)
	}
	compactingPartitionsByKey := compactingPartitionIDsByGroup(compacting)
	groups := make([]compactGroup, 0, len(order))
	for _, key := range order {
		groupParts := groupsByKey[key]
		sort.SliceStable(groupParts, func(i, j int) bool {
			if groupParts[i].CompactGeneration != groupParts[j].CompactGeneration {
				return groupParts[i].CompactGeneration < groupParts[j].CompactGeneration
			}
			if groupParts[i].UpdatedAt != groupParts[j].UpdatedAt {
				return groupParts[i].UpdatedAt < groupParts[j].UpdatedAt
			}
			return groupParts[i].PartID < groupParts[j].PartID
		})
		groups = append(groups, compactGroup{
			key:                    key,
			parts:                  groupParts,
			compactingPartitionIDs: compactingPartitionsByKey[key],
		})
	}
	return groups
}

func compactGroupKey(part Part) string {
	return strings.Join([]string{part.JobID, part.Bucket, part.DestinationDatabase, part.DestinationTable, part.DestinationSchema}, "\x00")
}

func compactingPartitionIDsByGroup(parts []Part) map[string][]string {
	sets := map[string]map[string]struct{}{}
	for _, part := range parts {
		if part.Status != StatusCompacting ||
			strings.TrimSpace(part.DestinationDatabase) == "" ||
			strings.TrimSpace(part.DestinationTable) == "" ||
			strings.TrimSpace(part.DestinationSchema) == "" {
			continue
		}
		key := compactGroupKey(part)
		if _, ok := sets[key]; !ok {
			sets[key] = map[string]struct{}{}
		}
		for _, partitionID := range partPartitionIDs(part) {
			sets[key][partitionID] = struct{}{}
		}
	}
	out := make(map[string][]string, len(sets))
	for key, set := range sets {
		partitionIDs := make([]string, 0, len(set))
		for partitionID := range set {
			partitionIDs = append(partitionIDs, partitionID)
		}
		sort.Strings(partitionIDs)
		out[key] = partitionIDs
	}
	return out
}

func matchesCompactClaimOptions(part Part, opts CompactClaimOptions) bool {
	if opts.JobID != "" && part.JobID != opts.JobID {
		return false
	}
	if opts.Bucket != "" && part.Bucket != opts.Bucket {
		return false
	}
	if opts.DestinationDatabase != "" && part.DestinationDatabase != opts.DestinationDatabase {
		return false
	}
	if opts.DestinationTable != "" && part.DestinationTable != opts.DestinationTable {
		return false
	}
	if opts.DestinationSchema != "" && part.DestinationSchema != opts.DestinationSchema {
		return false
	}
	if len(opts.RequiredPartitionIDs) > 0 && !partOverlapsRequiredPartitions(part, opts.RequiredPartitionIDs) {
		return false
	}
	return true
}

func compactCooldownActive(part Part, now time.Time) bool {
	if strings.TrimSpace(part.CompactCooldownUntil) == "" {
		return false
	}
	until, err := time.Parse(timeFormat, part.CompactCooldownUntil)
	if err != nil {
		return false
	}
	return now.Before(until)
}

func compactHeartbeatTime(part Part) (time.Time, error) {
	for _, value := range []string{part.UpdatedAt, part.CompactingAt} {
		if strings.TrimSpace(value) == "" {
			continue
		}
		t, err := time.Parse(timeFormat, value)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse compact heartbeat time for part %s/%s: %w", part.JobID, part.PartID, err)
		}
		return t, nil
	}
	return time.Time{}, fmt.Errorf("compacting part %s/%s has no updated_at or compacting_at", part.JobID, part.PartID)
}

func compactReadyAtForRelease(part Part, now time.Time) string {
	for _, value := range []string{part.CompactReadyAt, part.ProgressUpdatedAt, part.UpdatedAt, part.CompactingAt} {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return formatTime(now)
}

func selectCompactBatchParts(group compactGroup, opts CompactClaimOptions) []Part {
	minParts := opts.MinInputParts
	if minParts == 0 {
		minParts = 2
	}
	partitions := orderedCandidatePartitions(group.parts, opts.RequiredPartitionIDs)
	preferredPartitions := partitionsWithout(partitions, group.compactingPartitionIDs)
	orderedPartitions := append(preferredPartitions, partitionsWithout(partitions, preferredPartitions)...)
	for _, partitionID := range orderedPartitions {
		selected := selectCompactBatchPartsForPartition(group.parts, partitionID, minParts, opts)
		if len(selected) > 0 {
			return selected
		}
	}
	return nil
}

func partitionsWithout(partitions, excluded []string) []string {
	excludedSet := partitionSet(excluded)
	if len(excludedSet) == 0 {
		return append([]string(nil), partitions...)
	}
	out := make([]string, 0, len(partitions))
	for _, partitionID := range partitions {
		if _, ok := excludedSet[partitionID]; ok {
			continue
		}
		out = append(out, partitionID)
	}
	return out
}

func orderedCandidatePartitions(parts []Part, required []string) []string {
	requiredSet := partitionSet(required)
	seen := map[string]struct{}{}
	var partitions []string
	for _, part := range parts {
		ids := partPartitionIDs(part)
		for _, partitionID := range ids {
			if len(requiredSet) > 0 {
				if _, ok := requiredSet[partitionID]; !ok {
					continue
				}
			}
			if _, ok := seen[partitionID]; ok {
				continue
			}
			seen[partitionID] = struct{}{}
			partitions = append(partitions, partitionID)
		}
	}
	return partitions
}

func selectCompactBatchPartsForPartition(parts []Part, partitionID string, minParts uint64, opts CompactClaimOptions) []Part {
	var selected []Part
	var inputParts, inputBytes uint64
	for _, part := range parts {
		partitionParts := part.DestinationActivePartitionCounts[partitionID]
		if partitionParts == 0 {
			continue
		}
		if opts.MaxArtifacts > 0 && len(selected) >= opts.MaxArtifacts {
			break
		}
		partBytes := part.DestinationActivePartBytes
		if opts.MaxBytes > 0 && inputBytes+partBytes > opts.MaxBytes && len(selected) > 0 {
			break
		}
		selected = append(selected, part)
		inputParts += partitionParts
		inputBytes += partBytes
		if inputParts >= minParts {
			return selected
		}
	}
	return nil
}

func partitionSet(partitionIDs []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, partitionID := range partitionIDs {
		if strings.TrimSpace(partitionID) == "" {
			continue
		}
		out[partitionID] = struct{}{}
	}
	return out
}

func partOverlapsRequiredPartitions(part Part, required []string) bool {
	for partitionID := range partitionSet(required) {
		if part.DestinationActivePartitionCounts[partitionID] > 0 {
			return true
		}
	}
	return false
}

func partPartitionIDs(part Part) []string {
	ids := make([]string, 0, len(part.DestinationActivePartitionCounts))
	for partitionID, count := range part.DestinationActivePartitionCounts {
		if strings.TrimSpace(partitionID) == "" || count == 0 {
			continue
		}
		ids = append(ids, partitionID)
	}
	sort.Strings(ids)
	return ids
}

func (s *Store) claimCompactParts(ctx context.Context, parts []Part, workerID string, now time.Time) ([]Part, error) {
	claimed := make([]Part, 0, len(parts))
	for _, part := range parts {
		out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(s.table),
			Key:       part.key(),
			ConditionExpression: aws.String(
				"#status = :compact_ready",
			),
			UpdateExpression: aws.String(
				"SET #status = :compacting, gsi1pk = :gsi1pk, updated_at = :now, compacting_at = :now, worker_id = :worker REMOVE #error, compact_cooldown_until",
			),
			ExpressionAttributeNames: map[string]string{
				"#error":  "error",
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":compact_ready": stringAttr(string(StatusCompactReady)),
				":compacting":    stringAttr(string(StatusCompacting)),
				":gsi1pk":        stringAttr(statusKey(StatusCompacting)),
				":now":           stringAttr(formatTime(now)),
				":worker":        stringAttr(workerID),
			},
			ReturnValues: types.ReturnValueAllNew,
		})
		if err != nil {
			if len(claimed) > 0 {
				_ = s.ReleaseCompactBatch(ctx, CompactBatch{JobID: part.JobID, Parts: claimed}, workerID, time.Time{}, now)
			}
			return nil, fmt.Errorf("claim compact-ready part %s/%s: %w", part.JobID, part.PartID, err)
		}
		claimedPart, err := unmarshalPart(out.Attributes)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, *claimedPart)
	}
	return claimed, nil
}

func compactBatchFromParts(parts []Part) *CompactBatch {
	if len(parts) == 0 {
		return nil
	}
	batch := &CompactBatch{
		JobID: parts[0].JobID,
		Parts: append([]Part(nil), parts...),
	}
	for _, part := range parts {
		batch.InputPartCount += part.DestinationActivePartCount
		batch.InputRows += part.DestinationActivePartRows
		batch.InputBytes += part.DestinationActivePartBytes
		if part.CompactGeneration >= batch.Generation {
			batch.Generation = part.CompactGeneration + 1
		}
	}
	return batch
}

func (s *Store) MarkFinished(ctx context.Context, part Part, workerID, finishedKey string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	if strings.TrimSpace(finishedKey) == "" {
		return errors.New("finished key is required")
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :from AND #worker_id = :worker",
		),
		UpdateExpression: aws.String(
			"SET #status = :to, gsi1pk = :gsi1pk, updated_at = :now, finished_at = :now, finished_key = :finished_key REMOVE #worker_id, #error",
		),
		ExpressionAttributeNames: map[string]string{
			"#error":     "error",
			"#status":    "status",
			"#worker_id": "worker_id",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":finished_key": stringAttr(finishedKey),
			":from":         stringAttr(string(StatusInProgress)),
			":gsi1pk":       stringAttr(statusKey(StatusFinished)),
			":now":          stringAttr(formatTime(now)),
			":to":           stringAttr(string(StatusFinished)),
			":worker":       stringAttr(workerID),
		},
	})
	if err != nil {
		return fmt.Errorf("mark part %s/%s finished: %w", part.JobID, part.PartID, err)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, part Part, workerID string, cause error, now time.Time) error {
	if cause == nil {
		return errors.New("failure cause is required")
	}
	return s.transitionOwned(ctx, part, workerID, StatusFailed, "failed_at", cause.Error(), now)
}

func (s *Store) ReleaseInProgress(ctx context.Context, part Part, workerID string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :in_progress AND #worker_id = :worker",
		),
		UpdateExpression: aws.String(
			"SET #status = :ready, gsi1pk = :gsi1pk, updated_at = :now REMOVE #worker_id, started_at, #error" + progressRemoveExpression(),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":     "error",
			"#status":    "status",
			"#worker_id": "worker_id",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi1pk":      stringAttr(statusKey(StatusReady)),
			":in_progress": stringAttr(string(StatusInProgress)),
			":now":         stringAttr(formatTime(now)),
			":ready":       stringAttr(string(StatusReady)),
			":worker":      stringAttr(workerID),
		},
	})
	if err != nil {
		return fmt.Errorf("release state item for %s/%s back to %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return nil
}

func (s *Store) UpdateRewriteProgress(ctx context.Context, jobID, partID, workerID string, progress RewriteProgress, now time.Time) error {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(partID) == "" {
		return errors.New("job id and part id are required")
	}
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}

	names := map[string]string{
		"#status":     "status",
		"#updated_at": "updated_at",
		"#worker_id":  "worker_id",
	}
	values := map[string]types.AttributeValue{
		":in_progress": stringAttr(string(StatusInProgress)),
		":now":         stringAttr(formatTime(now)),
		":worker":      stringAttr(workerID),
	}
	set := []string{"#updated_at = :now", "progress_updated_at = :now"}

	if progress.QueryProgress != nil {
		set = append(set,
			"read_rows = :read_rows",
			"read_bytes = :read_bytes",
			"written_rows = :written_rows",
			"written_bytes = :written_bytes",
		)
		values[":read_rows"] = uintAttr(progress.QueryProgress.ReadRows)
		values[":read_bytes"] = uintAttr(progress.QueryProgress.ReadBytes)
		values[":written_rows"] = uintAttr(progress.QueryProgress.WrittenRows)
		values[":written_bytes"] = uintAttr(progress.QueryProgress.WrittenBytes)
	}
	if progress.SourceActivePartStats != nil {
		set = append(set,
			"source_active_part_count = :source_active_part_count",
			"source_active_part_rows = :source_active_part_rows",
			"source_active_part_bytes = :source_active_part_bytes",
		)
		values[":source_active_part_count"] = uintAttr(progress.SourceActivePartStats.Count)
		values[":source_active_part_rows"] = uintAttr(progress.SourceActivePartStats.Rows)
		values[":source_active_part_bytes"] = uintAttr(progress.SourceActivePartStats.Bytes)
	}
	if progress.DestinationActivePartStats != nil {
		set = append(set,
			"destination_active_part_count = :destination_active_part_count",
			"destination_active_part_rows = :destination_active_part_rows",
			"destination_active_part_bytes = :destination_active_part_bytes",
		)
		values[":destination_active_part_count"] = uintAttr(progress.DestinationActivePartStats.Count)
		values[":destination_active_part_rows"] = uintAttr(progress.DestinationActivePartStats.Rows)
		values[":destination_active_part_bytes"] = uintAttr(progress.DestinationActivePartStats.Bytes)
	}
	if progress.DestinationFailedMerges != nil {
		set = append(set, "destination_failed_merges = :destination_failed_merges")
		values[":destination_failed_merges"] = uintAttr(*progress.DestinationFailedMerges)
	}
	if progress.StageProgress != nil {
		set = append(set,
			"rewrite_stage = :rewrite_stage",
			"rewrite_stage_started_at = :rewrite_stage_started_at",
			"rewrite_stage_elapsed_ms = :rewrite_stage_elapsed_ms",
			"rewrite_total_elapsed_ms = :rewrite_total_elapsed_ms",
			"rewrite_stage_durations_ms = :rewrite_stage_durations_ms",
		)
		values[":rewrite_stage"] = stringAttr(progress.StageProgress.Stage)
		values[":rewrite_stage_started_at"] = stringAttr(formatTime(progress.StageProgress.StageStartedAt))
		values[":rewrite_stage_elapsed_ms"] = int64Attr(progress.StageProgress.StageElapsedMs)
		values[":rewrite_total_elapsed_ms"] = int64Attr(progress.StageProgress.TotalElapsedMs)
		values[":rewrite_stage_durations_ms"] = int64MapAttr(progress.StageProgress.CompletedStageDurationsMs)
	}

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       partStateKey(jobID, partID),
		ConditionExpression:       aws.String("#status = :in_progress AND #worker_id = :worker"),
		UpdateExpression:          aws.String("SET " + strings.Join(set, ", ")),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("update rewrite progress for %s/%s: %w", jobID, partID, err)
	}
	return nil
}

func (s *Store) ListJobIDs(ctx context.Context) ([]string, error) {
	paginator := dynamodb.NewScanPaginator(s.client, &dynamodb.ScanInput{
		TableName:            aws.String(s.table),
		ProjectionExpression: aws.String("#job_id"),
		ExpressionAttributeNames: map[string]string{
			"#job_id": "job_id",
		},
	})

	seen := map[string]struct{}{}
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("scan job ids: %w", err)
		}
		for _, item := range out.Items {
			av, ok := item["job_id"]
			if !ok {
				continue
			}
			value, ok := av.(*types.AttributeValueMemberS)
			if !ok {
				return nil, fmt.Errorf("job_id has non-string DynamoDB attribute type")
			}
			if value.Value != "" {
				seen[value.Value] = struct{}{}
			}
		}
	}

	jobIDs := make([]string, 0, len(seen))
	for jobID := range seen {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	return jobIDs, nil
}

func (s *Store) ListJobParts(ctx context.Context, jobID string) ([]Part, error) {
	if strings.TrimSpace(jobID) == "" {
		return nil, errors.New("job id is required")
	}
	paginator := dynamodb.NewQueryPaginator(s.client, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :part_prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "pk",
			"#sk": "sk",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":          stringAttr(jobKey(jobID)),
			":part_prefix": stringAttr("PART#"),
		},
	})

	var parts []Part
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("query job parts for %s: %w", jobID, err)
		}
		for _, item := range out.Items {
			part, err := unmarshalPart(item)
			if err != nil {
				return nil, err
			}
			parts = append(parts, *part)
		}
	}
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartID < parts[j].PartID
	})
	return parts, nil
}

func (s *Store) ListFinishedParts(ctx context.Context, jobID string) ([]Part, error) {
	allParts, err := s.ListJobParts(ctx, jobID)
	if err != nil {
		return nil, err
	}
	var parts []Part
	for _, part := range allParts {
		if part.Status == StatusFinished {
			parts = append(parts, part)
		}
	}
	return parts, nil
}

func (s *Store) DeleteJobParts(ctx context.Context, parts []Part) error {
	if len(parts) == 0 {
		return errors.New("job has no parts to delete")
	}
	jobID := parts[0].JobID
	if strings.TrimSpace(jobID) == "" {
		return errors.New("job id is required")
	}
	for _, part := range parts {
		if err := validatePart(part); err != nil {
			return err
		}
		if part.JobID != jobID {
			return fmt.Errorf("delete job parts got mixed job ids %q and %q", jobID, part.JobID)
		}
		_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(s.table),
			Key:       part.key(),
			ConditionExpression: aws.String(
				"#job_id = :job_id AND #part_id = :part_id",
			),
			ExpressionAttributeNames: map[string]string{
				"#job_id":  "job_id",
				"#part_id": "part_id",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":job_id":  stringAttr(part.JobID),
				":part_id": stringAttr(part.PartID),
			},
		})
		if err != nil {
			return fmt.Errorf("delete state item for %s/%s: %w", part.JobID, part.PartID, err)
		}
	}
	return nil
}

func (s *Store) MarkImporting(ctx context.Context, part Part, now time.Time) error {
	return s.transition(ctx, part, StatusFinished, StatusImporting, "importing_at", "", now)
}

func (s *Store) MarkImported(ctx context.Context, part Part, now time.Time) error {
	return s.transition(ctx, part, StatusImporting, StatusImported, "imported_at", "", now)
}

func (s *Store) MarkImportFailed(ctx context.Context, part Part, cause error, now time.Time) error {
	if cause == nil {
		return errors.New("failure cause is required")
	}
	return s.transition(ctx, part, StatusImporting, StatusFailed, "failed_at", cause.Error(), now)
}

func (s *Store) RetryFailedPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	if part.Status != StatusFailed {
		return "", fmt.Errorf("part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusFailed)
	}
	target := StatusReady
	removeExpression := " REMOVE #error, failed_at, started_at, finished_at, importing_at, imported_at, worker_id" + progressRemoveExpression()
	if part.ImportingAt != "" {
		target = StatusFinished
		removeExpression = " REMOVE #error, failed_at, importing_at, imported_at, worker_id"
	}

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :failed",
		),
		UpdateExpression: aws.String(
			"SET #status = :target, gsi1pk = :gsi1pk, updated_at = :now" + removeExpression,
		),
		ExpressionAttributeNames: map[string]string{
			"#error":  "error",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":failed": stringAttr(string(StatusFailed)),
			":gsi1pk": stringAttr(statusKey(target)),
			":now":    stringAttr(formatTime(now)),
			":target": stringAttr(string(target)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("retry failed state item for %s/%s as %s: %w", part.JobID, part.PartID, target, err)
	}
	return target, nil
}

func (s *Store) RetryInProgressPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	if part.Status != StatusInProgress {
		return "", fmt.Errorf("part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusInProgress)
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :in_progress",
		),
		UpdateExpression: aws.String(
			"SET #status = :ready, gsi1pk = :gsi1pk, updated_at = :now REMOVE #error, started_at, worker_id" + progressRemoveExpression(),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":  "error",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi1pk":      stringAttr(statusKey(StatusReady)),
			":in_progress": stringAttr(string(StatusInProgress)),
			":now":         stringAttr(formatTime(now)),
			":ready":       stringAttr(string(StatusReady)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("retry in-progress state item for %s/%s as %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return StatusReady, nil
}

func (s *Store) RetryStaleInProgressPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	if part.Status != StatusInProgress {
		return "", fmt.Errorf("part %s/%s is %s, expected %s", part.JobID, part.PartID, part.Status, StatusInProgress)
	}
	if strings.TrimSpace(part.ProgressUpdatedAt) == "" {
		return "", fmt.Errorf("part %s/%s has no progress_updated_at", part.JobID, part.PartID)
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :in_progress AND progress_updated_at = :progress_updated_at",
		),
		UpdateExpression: aws.String(
			"SET #status = :ready, gsi1pk = :gsi1pk, updated_at = :now REMOVE #error, started_at, worker_id" + progressRemoveExpression(),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":  "error",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi1pk":              stringAttr(statusKey(StatusReady)),
			":in_progress":         stringAttr(string(StatusInProgress)),
			":now":                 stringAttr(formatTime(now)),
			":progress_updated_at": stringAttr(part.ProgressUpdatedAt),
			":ready":               stringAttr(string(StatusReady)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("retry stale in-progress state item for %s/%s as %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return StatusReady, nil
}

func (s *Store) ForceRetryPart(ctx context.Context, part Part, now time.Time) (Status, error) {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"attribute_exists(pk) AND attribute_exists(sk)",
		),
		UpdateExpression: aws.String(
			"SET #status = :ready, gsi1pk = :gsi1pk, updated_at = :now REMOVE #error, failed_at, started_at, finished_at, importing_at, imported_at, worker_id" + progressRemoveExpression(),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":  "error",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi1pk": stringAttr(statusKey(StatusReady)),
			":now":    stringAttr(formatTime(now)),
			":ready":  stringAttr(string(StatusReady)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("force retry state item for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return StatusReady, nil
}

func (s *Store) ResetOriginalPartToReady(ctx context.Context, part Part, now time.Time) error {
	if err := validateOriginalResetPart(part); err != nil {
		return err
	}
	remove := []string{
		"#error",
		"failed_at",
		"started_at",
		"finished_at",
		"compact_ready_at",
		"compacting_at",
		"superseded_at",
		"importing_at",
		"imported_at",
		"worker_id",
		"compact_cooldown_until",
		"superseded_by",
		"destination_database",
		"destination_table",
		"destination_schema",
		"compact_generation",
		"compact_input_part_ids",
	}
	remove = append(remove, compactProgressRemoveAttributes()...)
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#job_id = :job_id AND #part_id = :part_id AND updated_at = :updated_at",
		),
		UpdateExpression: aws.String(
			"SET #status = :ready, gsi1pk = :gsi1pk, updated_at = :now REMOVE " + strings.Join(remove, ", ") + progressRemoveExpression(),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":   "error",
			"#job_id":  "job_id",
			"#part_id": "part_id",
			"#status":  "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi1pk":     stringAttr(statusKey(StatusReady)),
			":job_id":     stringAttr(part.JobID),
			":now":        stringAttr(formatTime(now)),
			":part_id":    stringAttr(part.PartID),
			":ready":      stringAttr(string(StatusReady)),
			":updated_at": stringAttr(part.UpdatedAt),
		},
	})
	if err != nil {
		return fmt.Errorf("reset original part %s/%s to %s: %w", part.JobID, part.PartID, StatusReady, err)
	}
	return nil
}

func (s *Store) ResetOriginalPartToCompactReady(ctx context.Context, part Part, now time.Time) error {
	if err := validateOriginalResetPart(part); err != nil {
		return err
	}
	remove := []string{
		"#error",
		"failed_at",
		"started_at",
		"finished_at",
		"compacting_at",
		"superseded_at",
		"importing_at",
		"imported_at",
		"worker_id",
		"compact_cooldown_until",
		"superseded_by",
		"compact_input_part_ids",
	}
	remove = append(remove, compactProgressRemoveAttributes()...)
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#job_id = :job_id AND #part_id = :part_id AND updated_at = :updated_at",
		),
		UpdateExpression: aws.String(
			"SET #status = :compact_ready, gsi1pk = :gsi1pk, updated_at = :now, compact_ready_at = :now REMOVE " + strings.Join(remove, ", "),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":   "error",
			"#job_id":  "job_id",
			"#part_id": "part_id",
			"#status":  "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":compact_ready": stringAttr(string(StatusCompactReady)),
			":gsi1pk":        stringAttr(statusKey(StatusCompactReady)),
			":job_id":        stringAttr(part.JobID),
			":now":           stringAttr(formatTime(now)),
			":part_id":       stringAttr(part.PartID),
			":updated_at":    stringAttr(part.UpdatedAt),
		},
	})
	if err != nil {
		return fmt.Errorf("reset original part %s/%s to %s: %w", part.JobID, part.PartID, StatusCompactReady, err)
	}
	return nil
}

func validateOriginalResetPart(part Part) error {
	if err := validatePart(part); err != nil {
		return err
	}
	if len(part.CompactInputPartIDs) > 0 || part.CompactGeneration > 0 {
		return fmt.Errorf("part %s/%s is a generated compact part, not an original source part", part.JobID, part.PartID)
	}
	if strings.TrimSpace(part.UpdatedAt) == "" {
		return fmt.Errorf("part %s/%s is missing updated_at", part.JobID, part.PartID)
	}
	return nil
}

func (s *Store) claimPart(ctx context.Context, part Part, workerID string, now time.Time) (*Part, error) {
	nowValue := formatTime(now)
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key:       part.key(),
		ConditionExpression: aws.String(
			"#status = :ready",
		),
		UpdateExpression: aws.String(
			"SET #status = :in_progress, gsi1pk = :gsi1pk, updated_at = :now, started_at = :now, worker_id = :worker, attempts = if_not_exists(attempts, :zero) + :one REMOVE #error" + progressRemoveExpression(),
		),
		ExpressionAttributeNames: map[string]string{
			"#error":  "error",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":gsi1pk":      stringAttr(statusKey(StatusInProgress)),
			":in_progress": stringAttr(string(StatusInProgress)),
			":now":         stringAttr(nowValue),
			":one":         numberAttr("1"),
			":ready":       stringAttr(string(StatusReady)),
			":worker":      stringAttr(workerID),
			":zero":        numberAttr("0"),
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		return nil, fmt.Errorf("claim state item for %s/%s: %w", part.JobID, part.PartID, err)
	}
	return unmarshalPart(out.Attributes)
}

func (s *Store) transitionOwned(ctx context.Context, part Part, workerID string, to Status, timestampAttr, errorText string, now time.Time) error {
	if strings.TrimSpace(workerID) == "" {
		return errors.New("worker id is required")
	}
	expressionNames := map[string]string{
		"#error":     "error",
		"#status":    "status",
		"#timestamp": timestampAttr,
		"#worker_id": "worker_id",
	}
	expressionValues := map[string]types.AttributeValue{
		":from":   stringAttr(string(StatusInProgress)),
		":gsi1pk": stringAttr(statusKey(to)),
		":now":    stringAttr(formatTime(now)),
		":to":     stringAttr(string(to)),
		":worker": stringAttr(workerID),
	}
	updateExpression := "SET #status = :to, gsi1pk = :gsi1pk, updated_at = :now, #timestamp = :now"
	if errorText != "" {
		updateExpression += ", #error = :error"
		expressionValues[":error"] = stringAttr(errorText)
	} else {
		updateExpression += " REMOVE #worker_id, #error"
	}
	if errorText != "" {
		updateExpression += " REMOVE #worker_id"
	}

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       part.key(),
		ConditionExpression:       aws.String("#status = :from AND #worker_id = :worker"),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  expressionNames,
		ExpressionAttributeValues: expressionValues,
	})
	if err != nil {
		return fmt.Errorf("transition state item for %s/%s to %s: %w", part.JobID, part.PartID, to, err)
	}
	return nil
}

func (s *Store) transition(ctx context.Context, part Part, from, to Status, timestampAttr, errorText string, now time.Time) error {
	expressionNames := map[string]string{
		"#error":     "error",
		"#status":    "status",
		"#timestamp": timestampAttr,
	}
	expressionValues := map[string]types.AttributeValue{
		":from":   stringAttr(string(from)),
		":gsi1pk": stringAttr(statusKey(to)),
		":now":    stringAttr(formatTime(now)),
		":to":     stringAttr(string(to)),
	}
	updateExpression := "SET #status = :to, gsi1pk = :gsi1pk, updated_at = :now, #timestamp = :now"
	if errorText != "" {
		updateExpression += ", #error = :error"
		expressionValues[":error"] = stringAttr(errorText)
	} else {
		updateExpression += " REMOVE #error"
	}

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(s.table),
		Key:                       part.key(),
		ConditionExpression:       aws.String("#status = :from"),
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  expressionNames,
		ExpressionAttributeValues: expressionValues,
	})
	if err != nil {
		return fmt.Errorf("transition state item for %s/%s from %s to %s: %w", part.JobID, part.PartID, from, to, err)
	}
	return nil
}

func IsConditionalCheckFailed(err error) bool {
	var conditional *types.ConditionalCheckFailedException
	return errors.As(err, &conditional)
}

func validatePart(part Part) error {
	if part.JobID == "" || part.PartID == "" || part.Bucket == "" || part.SourceKey == "" || part.FinishedKey == "" {
		return errors.New("part state is missing job_id, part_id, bucket, source_key, or finished_key")
	}
	if part.Status == "" {
		return errors.New("part state is missing status")
	}
	if part.PK != jobKey(part.JobID) || part.SK != partKey(part.PartID) {
		return fmt.Errorf("part state keys do not match %s/%s", part.JobID, part.PartID)
	}
	return nil
}

func unmarshalPart(item map[string]types.AttributeValue) (*Part, error) {
	var part Part
	if err := attributevalue.UnmarshalMap(item, &part); err != nil {
		return nil, err
	}
	if err := validatePart(part); err != nil {
		return nil, err
	}
	return &part, nil
}

func (p Part) key() map[string]types.AttributeValue {
	return partStateKey(p.JobID, p.PartID)
}

func partStateKey(jobID, partID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": stringAttr(jobKey(jobID)),
		"sk": stringAttr(partKey(partID)),
	}
}

func progressRemoveExpression() string {
	return ", progress_updated_at, read_rows, read_bytes, written_rows, written_bytes, source_active_part_count, source_active_part_rows, source_active_part_bytes, destination_active_part_count, destination_active_part_rows, destination_active_part_bytes, destination_active_partition_counts, destination_failed_merges, rewrite_stage, rewrite_stage_started_at, rewrite_stage_elapsed_ms, rewrite_total_elapsed_ms, rewrite_stage_durations_ms"
}

func compactProgressRemoveAttributes() []string {
	return []string{
		"compact_output_part_id",
		"compact_progress_at",
		"compact_input_part_count",
		"compact_input_rows",
		"compact_input_bytes",
		"compact_output_part_count",
		"compact_output_rows",
		"compact_output_bytes",
	}
}

func jobKey(jobID string) string {
	return "JOB#" + jobID
}

func partKey(partID string) string {
	return "PART#" + partID
}

func statusKey(status Status) string {
	return "STATUS#" + string(status)
}

func statusSortKey(createdAt, jobID, partID string) string {
	return createdAt + "#" + jobID + "#" + partID
}

func formatTime(t time.Time) string {
	return t.UTC().Format(timeFormat)
}

func stringAttr(value string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: value}
}

func numberAttr(value string) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: value}
}

func uintAttr(value uint64) types.AttributeValue {
	return numberAttr(fmt.Sprintf("%d", value))
}

func int64Attr(value int64) types.AttributeValue {
	return numberAttr(strconv.FormatInt(value, 10))
}

func int64MapAttr(values map[string]int64) types.AttributeValue {
	attrs := make(map[string]types.AttributeValue, len(values))
	for key, value := range values {
		attrs[key] = int64Attr(value)
	}
	return &types.AttributeValueMemberM{Value: attrs}
}
