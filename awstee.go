package awstee

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/smithy-go"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

//go:generate mockgen -source=$GOFILE -destination=mock_test.go -package=awstee

type S3Client interface {
	s3.HeadObjectAPIClient
	manager.UploadAPIClient
}

type CloudwatchLogsClient interface {
	DescribeLogStreams(ctx context.Context, params *cloudwatchlogs.DescribeLogStreamsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
	PutLogEvents(ctx context.Context, input *cloudwatchlogs.PutLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
	CreateLogGroup(ctx context.Context, input *cloudwatchlogs.CreateLogGroupInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(ctx context.Context, input *cloudwatchlogs.CreateLogStreamInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
}

type AWSClient struct {
	S3             S3Client
	CloudwatchLogs CloudwatchLogsClient
}

type AWSTee struct {
	cfg    *Config
	client AWSClient
}

func New(ctx context.Context, cfg *Config) (*AWSTee, error) {
	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithRegion(cfg.AWSRegion),
	}
	if endpointsResolver, ok := cfg.EndpointResolver(); ok {
		opts = append(opts, awsConfig.WithEndpointResolver(endpointsResolver))
	}
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	client := AWSClient{
		S3:             s3.NewFromConfig(awsCfg),
		CloudwatchLogs: cloudwatchlogs.NewFromConfig(awsCfg),
	}
	return NewWithClient(cfg, client)
}

func NewWithClient(cfg *Config, client AWSClient) (*AWSTee, error) {
	return &AWSTee{
		cfg:    cfg,
		client: client,
	}, nil
}

type AWSTeeReader struct {
	writeClosers []io.WriteCloser
	r            io.Reader
	isClosed     bool
}

func (app *AWSTee) TeeReader(r io.Reader, outputName string) (*AWSTeeReader, error) {
	log.Println("[debug] try create aws tee reader")
	writeClosers := make([]io.WriteCloser, 0)
	if app.cfg.EnableS3() {
		w, err := newS3Writer(app.client.S3, app.cfg.S3, outputName)
		if err != nil {
			return nil, fmt.Errorf("s3 writer: %w", err)
		}
		writeClosers = append(writeClosers, w)
		log.Println("[info] s3 destination: ", w)
	}
	if app.cfg.EnableCloudwatchLogs() {
		w, err := newCloudWatchLogsWriter(app.client.CloudwatchLogs, app.cfg.Cloudwatch, outputName)
		if err != nil {
			return nil, fmt.Errorf("cloudwatch logs writer: %w", err)
		}
		writeClosers = append(writeClosers, w)
		log.Println("[info] cloudwatch logs destination: ", w)
	}
	if len(writeClosers) == 0 {
		return nil, errors.New("no destination")
	}
	return newAWSTeeReader(r, writeClosers), nil
}

func newAWSTeeReader(r io.Reader, writeClosers []io.WriteCloser) *AWSTeeReader {

	t := &AWSTeeReader{
		writeClosers: writeClosers,
	}
	writers := lo.Map(t.writeClosers, func(w io.WriteCloser, _ int) io.Writer { return w })
	t.r = io.TeeReader(r, io.MultiWriter(writers...))
	return t
}

func (t *AWSTeeReader) Close() error {
	log.Println("[debug] closing aws tee writer")
	eg := errgroup.Group{}
	for _, writeCloser := range t.writeClosers {
		w := writeCloser
		eg.Go(w.Close)
	}
	err := eg.Wait()
	t.isClosed = true
	if err != nil {
		return err
	}

	log.Println("[debug] close complete aws tee writer")
	return nil
}

func (t *AWSTeeReader) Read(p []byte) (int, error) {
	if t.isClosed {
		return 0, io.EOF
	}
	return t.r.Read(p)
}

type backgroundWriter struct {
	errCh  chan error
	wg     sync.WaitGroup
	pw     *io.PipeWriter
	cancel context.CancelFunc
}

func newBackgroundWriter(worker func(context.Context, *io.PipeReader, chan<- error)) (*backgroundWriter, error) {
	if worker == nil {
		return nil, errors.New("worker is nil")
	}
	w := &backgroundWriter{
		errCh: make(chan error, 10),
	}
	var pr *io.PipeReader
	pr, w.pw = io.Pipe()
	w.wg.Add(1)
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	go func() {
		defer w.wg.Done()
		worker(ctx, pr, w.errCh)
		close(w.errCh)
		pr.Close()
	}()
	if err := w.Err(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *backgroundWriter) Write(p []byte) (int, error) {
	n, err := w.pw.Write(p)
	if err != nil {
		return n, err
	}
	return n, w.Err()
}

func (w *backgroundWriter) Err() error {
	select {
	case err, ok := <-w.errCh:
		if ok {
			return err
		}
	case <-time.After(5 * time.Millisecond):
	}
	return nil
}

func (w *backgroundWriter) Close() error {
	err := w.pw.Close()
	w.cancel()
	w.wg.Wait()
	if err != nil {
		return err
	}
	return w.Err()
}

type s3Writer struct {
	bucket string
	key    string
	*backgroundWriter
}

func newS3Writer(client S3Client, cfg *S3Config, outputName string) (*s3Writer, error) {
	bucket := cfg.urlPrefix.Host
	key := cfg.urlPrefix.Path
	if strings.HasSuffix(key, "/") {
		key = filepath.Join(key, outputName)
	} else {
		key += outputName
	}
	key = strings.TrimLeft(key, "/")
	ctx := context.Background()
	if exists, err := s3ObjectAlreadyExists(ctx, client, bucket, key); err != nil {
		if !cfg.AllowOverwrite {
			return nil, err
		}
	} else {
		if exists && !cfg.AllowOverwrite {
			return nil, fmt.Errorf("s3://%s/%s is already exists, not allow overwrite", bucket, key)
		}
	}
	uploader := manager.NewUploader(client)
	bw, err := newBackgroundWriter(func(_ context.Context, pr *io.PipeReader, c chan<- error) {
		log.Println("[debug] start s3 writer")
		defer func() {
			log.Println("[debug] end s3 writer")
		}()
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   pr,
		})
		if err != nil {
			c <- err
		}
	})
	if err != nil {
		return nil, err
	}
	w := &s3Writer{
		bucket:           bucket,
		key:              key,
		backgroundWriter: bw,
	}
	return w, nil
}

func s3ObjectAlreadyExists(ctx context.Context, client S3Client, bucket, key string) (bool, error) {
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "NotFound" {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func (w *s3Writer) Close() error {
	log.Println("[debug] close s3 writer")
	return w.backgroundWriter.Close()
}

func (w *s3Writer) String() string {
	return fmt.Sprintf("s3://%s/%s", w.bucket, w.key)
}

type cloudwatchLogsWriter struct {
	logGroup  string
	logStream string
	*backgroundWriter
}

func newCloudWatchLogsWriter(client CloudwatchLogsClient, cfg *CloudwatchLogsConfig, outputName string) (*cloudwatchLogsWriter, error) {
	logGroup := cfg.LogGroup
	logStream := strings.TrimSuffix(outputName, filepath.Ext(outputName))
	logStream = strings.ReplaceAll(strings.TrimLeft(logStream, "/"), "/", "-")
	sequenceToken, err := prepareCloudwatchLogs(context.Background(), client, logGroup, logStream, cfg.CreateLogGroup)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch logs destination initialize: %w", err)
	}
	bg, err := newBackgroundWriter(func(ctx context.Context, pr *io.PipeReader, c chan<- error) {
		log.Println("[debug] start cloudwatch logs writer")
		defer func() {
			log.Println("[debug] end cloudwatch logs writer")
		}()
		s := bufio.NewScanner(pr)
		lines := make(chan cwtypes.InputLogEvent, 0)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			log.Println("[debug] start cloudwatch logs buffering worker")
			defer func() {
				log.Println("[debug] end cloudwatch logs buffering worker")
				wg.Done()
			}()
			for s.Scan() {
				if text := s.Text(); text != "" {
					lines <- cwtypes.InputLogEvent{
						Message:   aws.String(s.Text()),
						Timestamp: aws.Int64(time.Now().UnixMilli()),
					}
				}
			}
			if err := s.Err(); err != nil && err != io.EOF {
				c <- err
			}
			close(lines)
		}()

		t := time.NewTicker(cfg.flushInterval)
		defer t.Stop()
		events := make([]cwtypes.InputLogEvent, 0)
		isDone := false
		for !isDone {
			select {
			case line, ok := <-lines:
				if ok {
					events = append(events, line)
				}
				if len(events) >= cfg.BufferLines {
					log.Printf("[debug] over limit put log %d events", len(events))
					output, err := client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
						LogGroupName:  aws.String(logGroup),
						LogStreamName: aws.String(logStream),
						LogEvents:     events,
						SequenceToken: sequenceToken,
					})
					if err != nil {
						log.Println("[error] put log events: ", err)
						c <- err
					}
					sequenceToken = output.NextSequenceToken
					events = make([]cwtypes.InputLogEvent, 0, len(events))
				}
			case <-t.C:
				if len(events) > 0 {
					log.Printf("[debug] flush interval put log %d events", len(events))
					output, err := client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
						LogGroupName:  aws.String(logGroup),
						LogStreamName: aws.String(logStream),
						LogEvents:     events,
						SequenceToken: sequenceToken,
					})
					if err != nil {
						log.Println("[error] put log events: ", err)
						c <- err
					}
					sequenceToken = output.NextSequenceToken
					events = make([]cwtypes.InputLogEvent, 0, len(events))
				}
			case <-ctx.Done():
				isDone = true
			}
		}
		wg.Wait()
		for line := range lines {
			events = append(events, line)
		}
		if len(events) > 0 {
			log.Printf("[debug] on close put log %d events", len(events))
			_, err := client.PutLogEvents(context.Background(), &cloudwatchlogs.PutLogEventsInput{
				LogGroupName:  aws.String(logGroup),
				LogStreamName: aws.String(logStream),
				LogEvents:     events,
				SequenceToken: sequenceToken,
			})
			if err != nil {
				log.Println("[error] put log events: ", err)
				c <- err
			}
		}

	})
	if err != nil {
		return nil, err
	}
	w := &cloudwatchLogsWriter{
		logGroup:         logGroup,
		logStream:        logStream,
		backgroundWriter: bg,
	}
	return w, nil
}

func prepareCloudwatchLogs(ctx context.Context, client CloudwatchLogsClient, logGroupName string, logStreamName string, createLogGroup bool) (*string, error) {
	output, err := client.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(logGroupName),
		LogStreamNamePrefix: aws.String(logStreamName),
	})

	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() != "ResourceNotFoundException" {
				return nil, err
			}
			if !strings.Contains(ae.ErrorMessage(), "log group does not exist") {
				return nil, err
			}
			if !createLogGroup {
				return nil, err
			}
			log.Println("[info] create log group ")
			_, err := client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
				LogGroupName: aws.String(logGroupName),
				Tags: map[string]string{
					"GeneratedBy": "awstee",
				},
			})
			if err != nil {
				return nil, err
			}
		}

	}

	if len(output.LogStreams) != 0 {
		for _, logStream := range output.LogStreams {
			if *logStream.LogStreamName == logStreamName {
				if logStream.UploadSequenceToken != nil {
					return aws.String(*logStream.UploadSequenceToken), nil
				}
				return nil, nil
			}
		}
	}

	// log stream not found
	_, err = client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(logGroupName),
		LogStreamName: aws.String(logStreamName),
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (w *cloudwatchLogsWriter) Close() error {
	log.Println("[debug] close cloudwatch log writer")
	io.WriteString(w.backgroundWriter, "\n")
	return w.backgroundWriter.Close()
}

func (w *cloudwatchLogsWriter) String() string {
	return fmt.Sprintf("LogGroup=%s, LogStream=%s", w.logGroup, w.logStream)
}
