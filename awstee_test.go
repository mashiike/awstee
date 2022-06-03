package awstee

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/smithy-go"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestAWSTeeReader(t *testing.T) {
	expected := "hoge\nfuga\n\n"
	var closeCount int32
	var buf1, buf2 bytes.Buffer
	teeReader := newAWSTeeReader(
		strings.NewReader(expected),
		[]io.WriteCloser{
			newTestWriteCloser(&buf1, func() error {
				atomic.AddInt32(&closeCount, 1)
				return nil
			}),
			newTestWriteCloser(&buf2, func() error {
				atomic.AddInt32(&closeCount, 1)
				return nil
			}),
		},
	)
	bs, err := io.ReadAll(teeReader)
	require.NoError(t, err)
	require.EqualValues(t, expected, string(bs))
	require.NoError(t, teeReader.Close())
	require.EqualValues(t, closeCount, 2)
	require.EqualValues(t, expected, buf1.String())
	require.EqualValues(t, expected, buf2.String())
}

func TestS3WriterPutObject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockS3Client(ctrl)
	var buf bytes.Buffer
	s3Client.EXPECT().HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, &smithy.GenericAPIError{
				Code: "NotFound",
			}
		},
	).Times(1)
	s3Client.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
			io.Copy(&buf, input.Body)
			return &s3.PutObjectOutput{}, nil
		},
	).Times(1)
	cfg := &S3Config{
		URLPrefix: "s3://awstee-example-com/logs/",
	}
	require.NoError(t, cfg.Restrict())
	w, err := newS3Writer(s3Client, cfg, "/test/hogehoge.log")
	require.NoError(t, err)
	require.EqualValues(t, "s3://awstee-example-com/logs/test/hogehoge.log", w.String())
	require.EqualValues(t, "awstee-example-com", w.bucket)
	require.EqualValues(t, "logs/test/hogehoge.log", w.key)
	require.EqualValues(t, 0, buf.Len())

	n, err := io.WriteString(w, "hogehoge")
	require.NoError(t, err)
	require.EqualValues(t, 8, n)
	require.EqualValues(t, 0, buf.Len())

	require.NoError(t, w.Close())
	require.EqualValues(t, 8, buf.Len())
}

func TestS3WriterMultiPart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s3Client := NewMockS3Client(ctrl)
	var buf bytes.Buffer
	exceptedPart := make([]byte, manager.MinUploadPartSize)
	size, err := rand.Read(exceptedPart)

	s3Client.EXPECT().HeadObject(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{}, &smithy.GenericAPIError{
				Code: "NotFound",
			}
		},
	).Times(1)
	s3Client.EXPECT().CreateMultipartUpload(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
			require.EqualValues(t, aws.String("awstee-example-com"), input.Bucket)
			require.EqualValues(t, aws.String("logs/test/hogehoge.log"), input.Key)
			return &s3.CreateMultipartUploadOutput{
				Bucket:   input.Bucket,
				Key:      input.Key,
				UploadId: aws.String("updated_id"),
			}, nil
		},
	).Times(1)
	n := 2
	var wg sync.WaitGroup
	wg.Add(n)
	var mu sync.Mutex
	s3Client.EXPECT().UploadPart(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			require.EqualValues(t, aws.String("awstee-example-com"), input.Bucket)
			require.EqualValues(t, aws.String("logs/test/hogehoge.log"), input.Key)
			var temp bytes.Buffer
			_, err := io.ReadAll(io.TeeReader(input.Body, &temp))
			buf.Write(temp.Bytes())
			require.NoError(t, err)
			require.EqualValues(t, exceptedPart, temp.Bytes())
			return &s3.UploadPartOutput{}, nil
		},
	).Times(2)
	s3Client.EXPECT().CompleteMultipartUpload(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
			require.EqualValues(t, aws.String("awstee-example-com"), input.Bucket)
			require.EqualValues(t, aws.String("logs/test/hogehoge.log"), input.Key)
			require.EqualValues(t, aws.String("updated_id"), input.UploadId)
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
	).Times(1)
	cfg := &S3Config{
		URLPrefix: "s3://awstee-example-com/logs",
	}

	require.NoError(t, cfg.Restrict())
	w, err := newS3Writer(s3Client, cfg, "/test/hogehoge.log")
	require.NoError(t, err)
	require.EqualValues(t, 0, buf.Len())
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		n, err := w.Write(exceptedPart)
		require.NoError(t, err)
		require.EqualValues(t, size, n)
	}
	wg.Wait()
	require.EqualValues(t, size*n, buf.Len())

	require.NoError(t, w.Close())
}

func TestCloudwatchLogsWriter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cloudwatchLogsClient := NewMockCloudwatchLogsClient(ctrl)
	var putCount int32
	lines := make(chan string, 5)
	cloudwatchLogsClient.EXPECT().DescribeLogStreams(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&cloudwatchlogs.DescribeLogStreamsOutput{
			LogStreams: []types.LogStream{
				{
					LogStreamName:       aws.String("test-hogehoge"),
					UploadSequenceToken: aws.String("token"),
				},
			},
		},
		nil,
	).Times(1)
	var mu sync.Mutex
	cloudwatchLogsClient.EXPECT().PutLogEvents(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
			mu.Lock()
			defer mu.Unlock()
			require.EqualValues(t, "token", *input.SequenceToken)
			require.EqualValues(t, "/awstee/hoge", *input.LogGroupName)
			require.EqualValues(t, "test-hogehoge", *input.LogStreamName)
			for _, event := range input.LogEvents {
				require.EqualValues(t, aws.String("hoge"), event.Message)
				atomic.AddInt32(&putCount, 1)
				lines <- *event.Message
			}
			return &cloudwatchlogs.PutLogEventsOutput{
				NextSequenceToken: aws.String("token"),
			}, nil
		},
	).AnyTimes()
	cfg := &CloudwatchLogsConfig{
		LogGroup:      "/awstee/hoge",
		flushInterval: 1 * time.Millisecond,
	}
	require.NoError(t, cfg.Restrict())
	w, err := newCloudWatchLogsWriter(cloudwatchLogsClient, cfg, "/test/hogehoge.log")
	require.NoError(t, err)
	require.EqualValues(t, "LogGroup=/awstee/hoge, LogStream=test-hogehoge", w.String())
	require.EqualValues(t, "/awstee/hoge", w.logGroup)
	require.EqualValues(t, "test-hogehoge", w.logStream)
	require.EqualValues(t, 0, len(lines))

	n, err := io.WriteString(w, "hoge\nhoge\nhoge\nhoge")
	require.NoError(t, err)
	require.EqualValues(t, 19, n)
	select {
	case l := <-lines:
		require.EqualValues(t, "hoge", l)
	case <-time.After(5 * time.Second):
		t.Error("wait put events deadline")
	}

	require.NoError(t, w.Close())
	require.EqualValues(t, 4, putCount)
	close(lines)
}

type testWriteCloser struct {
	w  io.Writer
	fn func() error
}

func newTestWriteCloser(w io.Writer, fn func() error) io.WriteCloser {
	return testWriteCloser{w: w, fn: fn}
}

func (w testWriteCloser) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

func (w testWriteCloser) Close() error {
	return w.fn()
}
