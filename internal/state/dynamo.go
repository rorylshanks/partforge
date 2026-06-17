package state

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	StatusReady      Status = "READY"
	StatusInProgress Status = "IN_PROGRESS"
	StatusFinished   Status = "FINISHED"
	StatusImporting  Status = "IMPORTING"
	StatusImported   Status = "IMPORTED"
	StatusFailed     Status = "FAILED"

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
	PK          string `dynamodbav:"pk"`
	SK          string `dynamodbav:"sk"`
	GSI1PK      string `dynamodbav:"gsi1pk"`
	GSI1SK      string `dynamodbav:"gsi1sk"`
	JobID       string `dynamodbav:"job_id"`
	PartID      string `dynamodbav:"part_id"`
	Status      Status `dynamodbav:"status"`
	Bucket      string `dynamodbav:"bucket"`
	SourceKey   string `dynamodbav:"source_key"`
	FinishedKey string `dynamodbav:"finished_key"`
	CreatedAt   string `dynamodbav:"created_at"`
	UpdatedAt   string `dynamodbav:"updated_at"`
	StartedAt   string `dynamodbav:"started_at,omitempty"`
	FinishedAt  string `dynamodbav:"finished_at,omitempty"`
	ImportingAt string `dynamodbav:"importing_at,omitempty"`
	ImportedAt  string `dynamodbav:"imported_at,omitempty"`
	FailedAt    string `dynamodbav:"failed_at,omitempty"`
	WorkerID    string `dynamodbav:"worker_id,omitempty"`
	Attempts    int    `dynamodbav:"attempts"`
	Error       string `dynamodbav:"error,omitempty"`

	ProgressUpdatedAt          string `dynamodbav:"progress_updated_at,omitempty"`
	ReadRows                   uint64 `dynamodbav:"read_rows,omitempty"`
	ReadBytes                  uint64 `dynamodbav:"read_bytes,omitempty"`
	WrittenRows                uint64 `dynamodbav:"written_rows,omitempty"`
	WrittenBytes               uint64 `dynamodbav:"written_bytes,omitempty"`
	SourceActivePartCount      uint64 `dynamodbav:"source_active_part_count,omitempty"`
	SourceActivePartRows       uint64 `dynamodbav:"source_active_part_rows,omitempty"`
	SourceActivePartBytes      uint64 `dynamodbav:"source_active_part_bytes,omitempty"`
	DestinationActivePartCount uint64 `dynamodbav:"destination_active_part_count,omitempty"`
	DestinationActivePartRows  uint64 `dynamodbav:"destination_active_part_rows,omitempty"`
	DestinationActivePartBytes uint64 `dynamodbav:"destination_active_part_bytes,omitempty"`
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

type RewriteProgress struct {
	QueryProgress              *QueryProgress
	SourceActivePartStats      *PartStats
	DestinationActivePartStats *PartStats
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
	return ", progress_updated_at, read_rows, read_bytes, written_rows, written_bytes, source_active_part_count, source_active_part_rows, source_active_part_bytes, destination_active_part_count, destination_active_part_rows, destination_active_part_bytes"
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
