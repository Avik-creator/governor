package main

import (
	"context"
	"strings"
	"testing"

	"github.com/Avik-creator/governor"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/daemon"
	"github.com/Avik-creator/governor/internal/transport/certtest"
)

const preToolUse = `{"session_id": "s1", "hook_event_name": "PreToolUse", "tool_use_id": "t1"}`

func TestRun(t *testing.T) {
	// Nothing listens on this address, so any call to governord is refused quickly.
	env := map[string]string{governor.EnvAddr: "127.0.0.1:1", governor.EnvAPIKey: "key"}
	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		stdin      string
		wantCode   int
		wantStderr string
	}{
		{"no command", nil, env, "", exitBlock, "usage: governor hook"},
		{"ui without a key", []string{"ui"}, map[string]string{}, "", exitFailed, "no API key"},
		{"ui with a missing config", []string{"ui", "-config", "/nonexistent.yaml"}, env, "", exitFailed, "nonexistent.yaml"},
		{"ui with governord unreachable", []string{"ui"}, env, "", exitFailed, "governord at 127.0.0.1:1"},
		{"unknown command", []string{"status"}, env, "", exitBlock, "usage: governor hook"},
		{"unknown flag", []string{"hook", "-nope"}, env, preToolUse, exitBlock, "flag provided but not defined"},
		{"event that cannot refuse", []string{"hook"}, env, `{"session_id": "s1", "hook_event_name": "SessionEnd"}`, 0, ""},
		{"no API key", []string{"hook"}, map[string]string{}, preToolUse, exitBlock, "no API key"},
		{"governord unreachable", []string{"hook", "-timeout", "500ms", "-source", "codex", "-tool-calls", "5"}, env,
			preToolUse, exitBlock, "governord at 127.0.0.1:1 gave no answer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stderr strings.Builder
			getenv := func(name string) string { return tc.env[name] }
			code := run(t.Context(), tc.args, strings.NewReader(tc.stdin), &stderr, getenv)
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d", code, tc.wantCode)
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) || (tc.wantStderr == "" && stderr.Len() != 0) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestRunOverTLS(t *testing.T) {
	certFile, keyFile := certtest.SelfSigned(t)
	otherCert, _ := certtest.SelfSigned(t)
	ctx, cancel := context.WithCancel(context.Background())
	d, err := daemon.Start(ctx, &config.Config{
		Listen:        "127.0.0.1:0",
		TLS:           &config.TLS{CertFile: certFile, KeyFile: keyFile},
		ReapInterval:  config.DefaultReapInterval,
		DrainTimeout:  config.DefaultDrainTimeout,
		NodeRetention: config.DefaultNodeRetention,
		SnapshotEvery: config.DefaultSnapshotEvery,
		Tenants:       []config.Tenant{{Name: "a", APIKey: "key"}},
	})
	if err != nil {
		cancel()
		t.Fatalf("daemon.Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = d.Wait()
	})

	const sessionEnd = `{"session_id": "s1", "hook_event_name": "SessionEnd"}`
	hook := []string{"hook", "-timeout", "2s"}
	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		stdin      string
		wantCode   int
		wantStderr string
	}{
		{"trusting the certificate", hook, map[string]string{governor.EnvCAFile: certFile}, preToolUse, 0, ""},
		{"plain text", hook, nil, preToolUse, exitBlock, "gave no answer"},
		{"the system's roots", hook, map[string]string{governor.EnvTLS: "1"}, preToolUse, exitBlock, "gave no answer"},
		{"another certificate", hook, map[string]string{governor.EnvCAFile: otherCert}, preToolUse, exitBlock, "gave no answer"},
		{"settings that cannot be read", hook, map[string]string{governor.EnvTLS: "maybe"}, preToolUse, exitBlock, "TLS settings"},
		// An event that cannot refuse a tool must not be failed by the settings either.
		{"settings that cannot be read, on another event", hook, map[string]string{governor.EnvTLS: "maybe"}, sessionEnd, 0, ""},
		{"ui with settings that cannot be read", []string{"ui"}, map[string]string{governor.EnvTLS: "maybe"}, "", exitFailed, "GOVERNOR_TLS"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{governor.EnvAddr: d.Addr(), governor.EnvAPIKey: "key"}
			for name, value := range tc.env {
				env[name] = value
			}
			var stderr strings.Builder
			code := run(t.Context(), tc.args, strings.NewReader(tc.stdin), &stderr, func(name string) string { return env[name] })
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d", code, tc.wantCode)
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) || (tc.wantStderr == "" && stderr.Len() != 0) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}
