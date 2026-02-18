package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/michaelmcclelland/nimbus-crawler/internal/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// maxObjectSize is the read limit for MinIO objects.
// Matches crawler.maxBodyBytes to avoid reading more than was stored.
const maxObjectSize = 10 * 1024 * 1024 // 10MB

type MinIOClient struct {
	client *minio.Client
}

func NewMinIOClient(ctx context.Context, cfg config.MinIOConfig) (*MinIOClient, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating minio client: %w", err)
	}

	mc := &MinIOClient{client: client}
	if err := mc.ensureBuckets(ctx); err != nil {
		return nil, err
	}

	return mc, nil
}

func (m *MinIOClient) ensureBuckets(ctx context.Context) error {
	for _, bucket := range []string{HTMLBucket, TextBucket} {
		exists, err := m.client.BucketExists(ctx, bucket)
		if err != nil {
			return fmt.Errorf("checking bucket %s: %w", bucket, err)
		}
		if !exists {
			if err := m.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
				return fmt.Errorf("creating bucket %s: %w", bucket, err)
			}
		}
	}
	return nil
}

func (m *MinIOClient) PutObject(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	reader := bytes.NewReader(data)
	_, err := m.client.PutObject(ctx, bucket, key, reader, int64(len(data)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("putting object %s/%s: %w", bucket, key, err)
	}
	return nil
}

func (m *MinIOClient) GetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	obj, err := m.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting object %s/%s: %w", bucket, key, err)
	}
	defer obj.Close()

	data, err := io.ReadAll(io.LimitReader(obj, maxObjectSize))
	if err != nil {
		return nil, fmt.Errorf("reading object %s/%s: %w", bucket, key, err)
	}
	return data, nil
}
