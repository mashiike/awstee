package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/mashiike/awstee"
)

var (
	Version string = "current"
)

func main() {
	cfg := awstee.DefaultConfig()
	cfg.SetFlags(flag.CommandLine)
	var (
		config      string
		interrupt   bool
		minLevel    string
		exitOnError bool
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "awstee is a tee command-like tool with AWS as the output destination")
		fmt.Fprintln(flag.CommandLine.Output(), "version:", Version)
		flag.CommandLine.PrintDefaults()
	}
	flag.StringVar(&config, "config", "", "config file path")
	flag.StringVar(&minLevel, "log-level", "info", "awstee log level")
	flag.BoolVar(&interrupt, "i", false, "receive interrupt signal")
	flag.BoolVar(&exitOnError, "x", false, "exit if an error occurs during initialization")
	flag.Parse()

	filter := &logutils.LevelFilter{
		Levels: []logutils.LogLevel{"debug", "info", "notice", "warn", "error"},
		ModifierFuncs: []logutils.ModifierFunc{
			logutils.Color(color.FgHiBlack),
			nil,
			logutils.Color(color.FgHiBlue),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.BgBlack),
		},
		MinLevel: logutils.LogLevel(strings.ToLower(minLevel)),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)

	ctx := context.Background()
	if interrupt {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, os.Interrupt, os.Kill)
		defer stop()
	}

	var r io.Reader
	if awsTeeReader, err := prepare(ctx, cfg, config); err != nil {
		if exitOnError {
			log.Fatal("[error] ", err)
		} else {
			log.Println("[error] ", err)
		}
		log.Println("[warn] error occurred during initialization, so only standard output is performed")
		r = os.Stdin
	} else {
		r = awsTeeReader
		defer awsTeeReader.Close()
	}

	s := bufio.NewScanner(r)
	for s.Scan() {
		fmt.Println(s.Text())
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				log.Fatal(err)
			}
			return
		default:
		}
	}
}

func prepare(ctx context.Context, cfg *awstee.Config, config string) (*awstee.AWSTeeReader, error) {
	if config == "" {
		if err := cfg.Restrict(); err != nil {
			return nil, fmt.Errorf("configuration restrict: %w", err)
		}
	} else {
		if err := cfg.Load(config); err != nil {
			return nil, fmt.Errorf("configuration load: %w", err)
		}
	}
	if err := cfg.ValidateVersion(Version); err != nil {
		return nil, fmt.Errorf("version validate: %w", err)
	}
	app, err := awstee.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("awstee initialize: %w", err)
	}
	outputName := flag.Arg(0)
	if outputName == "" {
		return nil, fmt.Errorf("output name is empty")
	}

	r, err := app.TeeReader(os.Stdin, outputName)
	if err != nil {
		return nil, fmt.Errorf("create tee reader: %w", err)
	}
	return r, nil
}
