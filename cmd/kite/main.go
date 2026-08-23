package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Master290/kite/internal/config"
	"github.com/Master290/kite/internal/server"
	"golang.org/x/crypto/bcrypt"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "kite:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return serve(args)
	}
	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "validate":
		return validate(args[1:])
	case "hash-password":
		return hashPassword(args[1:])
	case "version", "--version", "-version":
		fmt.Println(version)
		return nil
	case "help", "--help", "-h":
		usage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := fs.String("config", "kite.yaml", "configuration file")
	logLevel := fs.String("log-level", "info", "debug, info, warn, or error")
	if err := fs.Parse(args); err != nil {
		return err
	}
	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	srv, err := server.New(cfg, *configPath, logger)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Start(ctx); err != nil {
		return err
	}
	logger.Info("kite started", "version", version, "http", cfg.Server.HTTPAddress, "https", cfg.Server.HTTPSAddress, "http3", cfg.Server.HTTP3Address, "admin", cfg.Admin.Address)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	logger.Info("kite stopped")
	return nil
}

func validate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	p := fs.String("config", "kite.yaml", "configuration file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*p)
	if err != nil {
		return err
	}
	fmt.Printf("valid config v%d: %d mount(s)\n", cfg.Version, len(cfg.Mounts))
	return nil
}
func hashPassword(args []string) error {
	fs := flag.NewFlagSet("hash-password", flag.ContinueOnError)
	cost := fs.Int("cost", 12, "bcrypt cost")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: kite hash-password [--cost 12] PASSWORD")
	}
	b, err := bcrypt.GenerateFromPassword([]byte(fs.Arg(0)), *cost)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
func usage() {
	fmt.Println("Kite streaming server\n\nUsage:\n  kite serve [-config kite.yaml]\n  kite validate [-config kite.yaml]\n  kite hash-password PASSWORD\n  kite version")
}
