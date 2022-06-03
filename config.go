package awstee

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	gv "github.com/hashicorp/go-version"
	gc "github.com/kayac/go-config"
)

type Config struct {
	RequiredVersion string                `yaml:"required_version,omitempty"`
	AWSRegion       string                `yaml:"aws_region,omitempty"`
	S3              *S3Config             `yaml:"s3,omitempty"`
	Cloudwatch      *CloudwatchLogsConfig `yaml:"cloudwatch,omitempty"`
	Endpoints       *EndpointsConfig      `yaml:"endpoints,omitempty"`

	//private field
	versionConstraints gv.Constraints `yaml:"-,omitempty"`
}

type S3Config struct {
	URLPrefix             string `yaml:"url_prefix,omitempty"`
	AllowOverwrite        bool   `yaml:"allow_overwrite,omitempty"`
	FirstlyPutEmptyObject bool   `yaml:"firstly_put_empty_object,omitempty"`
	urlPrefix             *url.URL
}

type CloudwatchLogsConfig struct {
	LogGroup       string `yaml:"log_group,omitempty"`
	FlushInterval  string `yaml:"flush_interval,omitempty"`
	BufferLines    int    `yaml:"buffer_lines,omitempty"`
	CreateLogGroup bool   `yaml:"create_log_group,omitempty"`

	flushInterval time.Duration
}

type EndpointsConfig struct {
	CloudWatchLogs string `yaml:"cloudwatchlogs,omitempty"`
	STS            string `yaml:"sts,omitempty"`
	S3             string `yaml:"s3,omitempty"`
}

func (cfg *Config) Load(path string) error {
	loader := gc.New()
	if err := loader.LoadWithEnv(cfg, path); err != nil {
		return fmt.Errorf("config load:%w", err)
	}
	return cfg.Restrict()
}

func (cfg *Config) EnableS3() bool {
	return cfg.S3 != nil && cfg.S3.URLPrefix != ""
}

func (cfg *Config) EnableCloudwatchLogs() bool {
	return cfg.Cloudwatch != nil && cfg.Cloudwatch.LogGroup != ""
}

// Restrict restricts a configuration.
func (cfg *Config) Restrict() error {
	if cfg.RequiredVersion != "" {
		constraints, err := gv.NewConstraint(cfg.RequiredVersion)
		if err != nil {
			return fmt.Errorf("required_version has invalid format: %w", err)
		}
		cfg.versionConstraints = constraints
	}

	if cfg.EnableS3() {
		if err := cfg.S3.Restrict(); err != nil {
			return err
		}
	}
	if cfg.EnableCloudwatchLogs() {
		if err := cfg.Cloudwatch.Restrict(); err != nil {
			return err
		}
	}
	return nil
}

func (cfg *Config) SetFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.AWSRegion, "aws-region", cfg.AWSRegion, "aws region")
	if cfg.S3 == nil {
		cfg.S3 = &S3Config{}
	}
	cfg.S3.SetFlags(f)
	if cfg.Cloudwatch == nil {
		cfg.Cloudwatch = &CloudwatchLogsConfig{}
	}
	cfg.Cloudwatch.SetFlags(f)
}

func (cfg *S3Config) Restrict() error {
	u, err := url.Parse(cfg.URLPrefix)
	if err != nil {
		return fmt.Errorf("s3 url_prefix is invalid format: %w", err)
	}
	if u.Scheme != "s3" {
		return fmt.Errorf("s3 url_prefix schema is not `s3`: schema is `%s`", u.Scheme)
	}
	cfg.urlPrefix = u
	return nil
}

func (cfg *S3Config) SetFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.URLPrefix, "s3-url-prefix", cfg.URLPrefix, "destination s3 url prefix")
	flag.BoolVar(&cfg.AllowOverwrite, "s3-allow-overwrite", false, "allow overwriting if the s3 object already exists?")
	flag.BoolVar(&cfg.FirstlyPutEmptyObject, "s3-firstly-put-empty-object", false, "put object from first for authority checks, etc.")
}

func (cfg *CloudwatchLogsConfig) Restrict() error {
	if cfg.LogGroup == "" {
		return fmt.Errorf("cloudwatch log_group is required")
	}
	if cfg.FlushInterval == "" {
		cfg.flushInterval = 5 * time.Second
	} else {
		var err error
		cfg.flushInterval, err = time.ParseDuration(cfg.FlushInterval)
		if err != nil {
			return fmt.Errorf("cloudwatch flush_interval is invalid format")
		}
	}
	if cfg.BufferLines == 0 {
		cfg.BufferLines = 50
	}
	return nil
}
func (cfg *CloudwatchLogsConfig) SetFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.LogGroup, "log-group-name", cfg.LogGroup, "destination cloudwatch logs log group name")
	flag.StringVar(&cfg.FlushInterval, "flush-interval", "5s", "cloudwatch logs output flush interval duration")
	flag.IntVar(&cfg.BufferLines, "buffer-lines", 50, "cloudwatch logs output buffered lines")
	flag.BoolVar(&cfg.CreateLogGroup, "create-log-group", false, "cloudwatch logs log group if not exists, create target log group")
}

// ValidateVersion validates a version satisfies required_version.
func (cfg *Config) ValidateVersion(version string) error {
	if cfg.versionConstraints == nil {
		return nil
	}
	versionParts := strings.SplitN(version, "-", 2)
	v, err := gv.NewVersion(versionParts[0])
	if err != nil {
		log.Printf("[warn] Invalid version format \"%s\". Skip checking required_version.", version)
		// invalid version string (e.g. "current") always allowed
		return nil
	}
	if !cfg.versionConstraints.Check(v) {
		return fmt.Errorf("version %s does not satisfy constraints required_version: %s", version, cfg.versionConstraints)
	}
	return nil
}

const (
	defaultConfigPath = ".config/awstee/default"
)

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func newConfig() *Config {
	cfg := &Config{
		AWSRegion: os.Getenv("AWS_REGION"),
	}
	return cfg
}

func DefaultConfig() *Config {
	cfg := newConfig()
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return cfg
	}
	path := filepath.Join(homeDir, defaultConfigPath+".yaml")
	if fileExists(path) {
		cfg.Load(path)
	}
	path = filepath.Join(homeDir, defaultConfigPath+".yml")
	if fileExists(path) {
		cfg.Load(path)
	}
	return cfg
}

func (cfg *Config) EndpointResolver() (aws.EndpointResolver, bool) {
	if cfg.Endpoints == nil {
		return nil, false
	}
	return aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if cfg.AWSRegion != region {
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		}
		switch service {
		case cloudwatchlogs.ServiceID:
			if cfg.Endpoints.CloudWatchLogs != "" {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           cfg.Endpoints.CloudWatchLogs,
					SigningRegion: cfg.AWSRegion,
				}, nil
			}
		case sts.ServiceID:
			if cfg.Endpoints.STS != "" {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           cfg.Endpoints.STS,
					SigningRegion: cfg.AWSRegion,
				}, nil
			}
		case s3.ServiceID:
			if cfg.Endpoints.STS != "" {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           cfg.Endpoints.STS,
					SigningRegion: cfg.AWSRegion,
				}, nil
			}
		}

		return aws.Endpoint{}, &aws.EndpointNotFoundError{}

	}), true
}
