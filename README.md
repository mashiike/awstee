# awstee

[![Documentation](https://godoc.org/github.com/mashiike/awstee?status.svg)](https://godoc.org/github.com/mashiike/awstee)
![Latest GitHub release](https://img.shields.io/github/release/mashiike/awstee.svg)
![Github Actions test](https://github.com/mashiike/awstee/workflows/Test/badge.svg?branch=main)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/awstee/blob/master/LICENSE)

`awstee` is a tee command-like tool with AWS as the output destination.

The `awstee` command reads from standard input and writes to standard output and AWS S3 and CloudWatch Logs.
awstee is the util tool for one time script for mission critical (especially for preventing rerunning it).

## Usage 

Basically, it can be used as follows
```shell
$ your_command |  awstee -s3-url-prefix s3://awstee-example-com/logs/  -log-group-name /awstee/logs hoge.log
2022/06/03 17:28:48 [info] s3 destination:  s3://awstee-example-com/logs//hoge.log
2022/06/03 17:28:49 [info] cloudwatch logs destination:  LogGroup=/awstee/test, LogStream=hoge
...
```

with default config `~/.config/awstee/default.yaml` or `~/.config/awstee/default.yml`.

```yaml
aws_region: "ap-northeast-1"

s3:
  url_prefix: "s3://awstee-example-com/logs/" # Required if used. If blank, output setting is turned off
  allow_overwrite: true # Whether to allow overwriting if the object already exists

cloudwatch:
  log_group: "/awstee/logs" # Required if used. If blank, output setting is turned off
  flush_interval: "5s" # Duration of buffer flush output to cloudwatch logs
  buffer_lines: 50 # If more than this number of lines are output within the flush period, it is output once to Cloudwatch logs.
  create_log_group: true # Whether to create a LogGroup if it does not exist
```

```shell
$ your_command |  awstee hoge.log
2022/06/03 17:28:48 [info] s3 destination:  s3://awstee-example-com/logs//hoge.log
2022/06/03 17:28:49 [info] cloudwatch logs destination:  LogGroup=/awstee/test, LogStream=hoge
...
```

### Install 
#### Homebrew (macOS and Linux)

```console
$ brew install mashiike/tap/awstee
```
#### Binary packages

[Releases](https://github.com/mashiike/awstee/releases)

### Options

```shell
$ awstee -h    
awstee is a tee command-like tool with AWS as the output destination
version: v0.1.0 
  -aws-region string
        aws region
  -buffer-lines int
        cloudwatch logs output buffered lines (default 50)
  -config string
        config file path
  -create-log-group
        cloudwatch logs log group if not exists, create target log group
  -flush-interval string
        cloudwatch logs output flush interval duration (default "5s")
  -i    receive interrupt signal
  -log-group-name string
        destination cloudwatch logs log group name
  -log-level string
        awstee log level (default "info")
  -s3-url-prefix string
        destination s3 url prefix
  -x    exit if an error occurs during initialization
```

## LICENSE

MIT License

Copyright (c) 2022 IKEDA Masashi
