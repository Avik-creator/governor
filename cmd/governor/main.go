// Command governor is the command-line client of governord.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/hook"
	"github.com/Avik-creator/governor/internal/transport"
	"github.com/Avik-creator/governor/internal/tui"
)

// dialTimeout bounds how long the screen waits for governord before giving up.
const dialTimeout = 5 * time.Second

// exitBlock is the exit code that tells Claude Code and Codex to refuse the tool.
const exitBlock = 2

// exitFailed is the exit code of a command, other than the hook, that could not do its work.
const exitFailed = 1

const usage = `usage: governor hook [flags]
       governor ui [-config file]

hook reads a Claude Code or Codex hook event on stdin and decides whether the
tool may run. Install it as a PreToolUse command hook.

ui shows the tasks governord knows, with what they have used, and changes their
caps and the defaults new tasks start with.

Both read GOVERNOR_ADDR and GOVERNOR_API_KEY from the environment. If governord
serves TLS, set GOVERNOR_TLS=1, or GOVERNOR_CA_FILE to the certificate to trust.
`

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdin, os.Stderr, os.Getenv))
}

// run executes one command and returns the process exit code.
func run(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer, getenv func(string) string) int {
	switch {
	case len(args) > 0 && args[0] == "ui":
		return runUI(ctx, args[1:], stderr, getenv)
	case len(args) == 0 || args[0] != "hook":
		fmt.Fprint(stderr, usage)
		return exitBlock
	}
	cfg := hook.Config{Addr: governor.DefaultAddr, APIKey: getenv(governor.EnvAPIKey)}
	if addr := getenv(governor.EnvAddr); addr != "" {
		cfg.Addr = addr
	}
	cfg.Credentials = func() (credentials.TransportCredentials, error) { return transport.FromEnv(getenv) }
	flags := flag.NewFlagSet("governor hook", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.Source, "source", "agent", "name of the CLI being governed, such as claude or codex")
	flags.Int64Var(&cfg.ToolCalls, "tool-calls", 0, "tool calls one run may make, unless the tenant has a default; 0 sets no cap")
	flags.Int64Var(&cfg.Agents, "agents", 0, "subagents one run may use, unless the tenant has a default; 0 sets no cap")
	flags.Int64Var(&cfg.AgentToolCalls, "agent-tool-calls", 0,
		"tool calls each subagent may make, unless the tenant has a default; 0 sets no cap")
	flags.DurationVar(&cfg.TTL, "ttl", 24*time.Hour, "how long after its first tool call a run is refused everything")
	flags.DurationVar(&cfg.Timeout, "timeout", 5*time.Second, "how long to wait for governord before refusing")
	if err := flags.Parse(args[1:]); err != nil {
		// A hook that cannot start must refuse, or a typo would switch the governor off.
		return exitBlock
	}

	err := hook.Run(ctx, cfg, stdin)
	if err == nil {
		return 0
	}
	// Both CLIs show stderr to the model when the hook exits with the blocking code.
	if b, ok := errors.AsType[*hook.Blocked](err); ok {
		fmt.Fprintln(stderr, b.Error())
	} else {
		fmt.Fprintln(stderr, "Governor blocked this tool:", err)
	}
	return exitBlock
}

// runUI opens a session and shows the terminal screen until the user quits.
func runUI(ctx context.Context, args []string, stderr io.Writer, getenv func(string) string) int {
	flags := flag.NewFlagSet("governor ui", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "", "governord configuration file to take the address and a key from")
	if err := flags.Parse(args); err != nil {
		return exitFailed
	}
	addr, key := governor.DefaultAddr, getenv(governor.EnvAPIKey)
	if env := getenv(governor.EnvAddr); env != "" {
		addr = env
	}
	creds, err := transport.FromEnv(getenv)
	if *path != "" {
		addr, key, creds, err = fromConfig(*path)
	}
	if err != nil {
		fmt.Fprintln(stderr, "governor ui:", err)
		return exitFailed
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	client, err := governor.Dial(dialCtx, governor.WithAddr(addr), governor.WithAPIKey(key),
		governor.WithDialOptions(grpc.WithTransportCredentials(creds)))
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "governor ui: governord at %s: %v\n", addr, err)
		return exitFailed
	}
	defer client.Close()
	if err := tui.Run(ctx, client, addr); err != nil {
		fmt.Fprintln(stderr, "governor ui:", err)
		return exitFailed
	}
	return 0
}

// fromConfig takes governord's address, the key that sees the most and how to trust it from its configuration file.
func fromConfig(path string) (addr, key string, creds credentials.TransportCredentials, err error) {
	cfg, err := config.Load(path)
	if err != nil {
		return "", "", nil, err
	}
	switch {
	case cfg.AdminKey != "":
		key = cfg.AdminKey
	case len(cfg.Tenants) == 1:
		key = cfg.Tenants[0].APIKey
	default:
		return "", "", nil, fmt.Errorf("%s has no admin_key and not exactly one tenant; set %s instead", path, governor.EnvAPIKey)
	}
	creds = insecure.NewCredentials()
	if cfg.TLS != nil {
		// The certificate governord presents is the one to trust, whoever signed it.
		if creds, err = transport.Trusting(cfg.TLS.CertFile); err != nil {
			return "", "", nil, fmt.Errorf("tls: %w", err)
		}
	}
	return cfg.Listen, key, creds, nil
}
