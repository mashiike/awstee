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
	"time"

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
		config          string
		ignoreInterrupt bool
		minLevel        string
		exitOnError     bool
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "awstee is a tee command-like tool with AWS as the output destination")
		fmt.Fprintln(flag.CommandLine.Output(), "version:", Version)
		flag.CommandLine.PrintDefaults()
	}
	flag.StringVar(&config, "config", "", "config file path")
	flag.StringVar(&minLevel, "log-level", "info", "awstee log level")
	flag.BoolVar(&ignoreInterrupt, "i", false, "ignore interrupt signal")
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var r io.Reader
	if awsTeeReader, err := prepare(ctx, cfg, config); err != nil {
		if exitOnError {
			log.Fatal("[error]", err)
		} else {
			log.Println("[error] ", err)
		}
		log.Println("[warn] error occurred during initialization, so only standard output is performed")
		r = os.Stdin
	} else {
		r = awsTeeReader
		defer func() {
			if err := awsTeeReader.Close(); err != nil {
				log.Println("[error] close tee reader:", err)
			}
		}()
	}

	s := bufio.NewScanner(r)
	mainLoopEnd := make(chan struct{})
	go func() {
		log.Println("[debug] start main loop")
		for s.Scan() {
			fmt.Println(s.Text())
		}
		log.Println("[debug] end main loop")
		close(mainLoopEnd)
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	condition := func() bool {
		select {
		case <-c:
			log.Println("[debug] receive interrupt")
			return ignoreInterrupt
		case <-mainLoopEnd:
			return false
		default:
			return true
		}
	}
	for condition() {
		time.Sleep(100 * time.Microsecond)
	}
	close(c)
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
