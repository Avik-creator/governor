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

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/hook"
)

// exitBlock is the exit code that tells Claude Code and Codex to refuse the tool.
const exitBlock = 2

const usage = `usage: governor hook [flags]

hook reads a Claude Code or Codex hook event on stdin and decides whether the
tool may run. Install it as a PreToolUse command hook. It reads GOVERNOR_ADDR
and GOVERNOR_API_KEY from the environment.
`

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdin, os.Stderr, os.Getenv))
}

// run executes one command and returns the process exit code.
func run(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer, getenv func(string) string) int {
	if len(args) == 0 || args[0] != "hook" {
		fmt.Fprint(stderr, usage)
		return exitBlock
	}
	cfg := hook.Config{Addr: governor.DefaultAddr, APIKey: getenv(governor.EnvAPIKey)}
	if addr := getenv(governor.EnvAddr); addr != "" {
		cfg.Addr = addr
	}
	flags := flag.NewFlagSet("governor hook", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&cfg.Source, "source", "agent", "name of the CLI being governed, such as claude or codex")
	flags.Int64Var(&cfg.ToolCalls, "tool-calls", 0, "tool calls one run may make; 0 sets no cap of its own")
	flags.Int64Var(&cfg.Agents, "agents", 0, "subagents one run may use; 0 sets no cap of its own")
	flags.Int64Var(&cfg.AgentToolCalls, "agent-tool-calls", 0,
		"tool calls each subagent may make; 0 sets no cap of its own")
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
