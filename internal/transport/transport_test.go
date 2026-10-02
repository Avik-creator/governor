package transport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Avik-creator/governor/internal/transport/certtest"
)

func TestFromEnv(t *testing.T) {
	certFile, _ := certtest.SelfSigned(t)
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	tests := []struct {
		name    string
		env     map[string]string
		want    string // the security protocol of the credentials
		wantErr string // a fragment of the error
	}{
		{"nothing set", nil, "insecure", ""},
		{"tls on", map[string]string{EnvTLS: "1"}, "tls", ""},
		{"tls spelled out", map[string]string{EnvTLS: "true"}, "tls", ""},
		{"tls off", map[string]string{EnvTLS: "false"}, "insecure", ""},
		{"a CA file alone", map[string]string{EnvCAFile: certFile}, "tls", ""},
		{"a CA file with tls on", map[string]string{EnvTLS: "1", EnvCAFile: certFile}, "tls", ""},
		{"tls that cannot be read", map[string]string{EnvTLS: "yes please"}, "", EnvTLS},
		{"a CA file with tls off", map[string]string{EnvTLS: "0", EnvCAFile: certFile}, "", "turns TLS off"},
		{"a CA file that is missing", map[string]string{EnvCAFile: "/nonexistent/ca.crt"}, "", "nonexistent"},
		{"a CA file without a certificate", map[string]string{EnvCAFile: empty}, "", "holds no certificate"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			creds, err := FromEnv(func(name string) string { return tc.env[name] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("FromEnv = %v, want an error mentioning %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("FromEnv: %v", err)
			}
			if got := creds.Info().SecurityProtocol; got != tc.want {
				t.Errorf("security protocol = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestServer(t *testing.T) {
	certFile, keyFile := certtest.SelfSigned(t)
	otherCert, _ := certtest.SelfSigned(t)
	if _, err := Server(certFile, keyFile); err != nil {
		t.Errorf("Server: %v", err)
	}
	tests := []struct {
		name          string
		cert, private string
	}{
		{"no key", certFile, ""},
		{"a missing certificate", "/nonexistent/server.crt", keyFile},
		{"a key in place of the certificate", keyFile, keyFile},
		{"a key of another certificate", otherCert, keyFile},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Server(tc.cert, tc.private); err == nil {
				t.Error("Server accepted it")
			}
		})
	}
}
