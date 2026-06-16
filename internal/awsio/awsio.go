package awsio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	"github.com/partforge/partforge/internal/manifest"
)

type Config struct {
	Region      string
	S3Endpoint  string
	SQSEndpoint string
}

type Clients struct {
	s3  *s3.Client
	sqs *sqs.Client
}

type QueueEnvelope struct {
	Body          string
	ReceiptHandle string
}

func New(ctx context.Context, cfg Config) (*Clients, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithRetryMaxAttempts(1),
	)
	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
			o.UsePathStyle = true
		}
	})
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.SQSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.SQSEndpoint)
		}
	})
	return &Clients{s3: s3Client, sqs: sqsClient}, nil
}

func (c *Clients) PutFile(ctx context.Context, bucket, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	return err
}

func (c *Clients) PutBytes(ctx context.Context, bucket, key string, b []byte) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(b),
	})
	return err
}

func (c *Clients) DownloadToFile(ctx context.Context, bucket, key, path string) error {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, out.Body)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func (c *Clients) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return false, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "404" || apiErr.ErrorCode() == "NoSuchKey") {
		return false, nil
	}
	return false, err
}

func (c *Clients) ListKeys(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := c.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, object := range out.Contents {
			if object.Key != nil {
				keys = append(keys, *object.Key)
			}
		}
		if out.NextContinuationToken == nil {
			return keys, nil
		}
		token = out.NextContinuationToken
	}
}

func (c *Clients) SendQueueMessage(ctx context.Context, queueURL string, msg manifest.QueueMessage) error {
	body, err := manifest.MarshalQueueMessage(msg)
	if err != nil {
		return err
	}
	_, err = c.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	})
	return err
}

func (c *Clients) ReceiveQueueMessage(ctx context.Context, queueURL string) (*QueueEnvelope, error) {
	out, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     10,
	})
	if err != nil {
		return nil, err
	}
	if len(out.Messages) == 0 {
		return nil, nil
	}
	msg := out.Messages[0]
	if msg.Body == nil || msg.ReceiptHandle == nil {
		return nil, fmt.Errorf("received SQS message without body or receipt handle")
	}
	return &QueueEnvelope{Body: *msg.Body, ReceiptHandle: *msg.ReceiptHandle}, nil
}

func (c *Clients) DeleteQueueMessage(ctx context.Context, queueURL, receiptHandle string) error {
	_, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func (c *Clients) PutJSON(ctx context.Context, bucket, key string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return c.PutBytes(ctx, bucket, key, b)
}
