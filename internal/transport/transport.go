// Package transport chooses how the gRPC connection between a client and governord is secured.
package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Environment variables that tell a client how to reach a governord that serves TLS.
const (
	// EnvTLS turns TLS on when true; the server is then checked against the system's roots.
	EnvTLS = "GOVERNOR_TLS"

	// EnvCAFile names a PEM file of the only certificates to trust, and turns TLS on.
	EnvCAFile = "GOVERNOR_CA_FILE"
)

// minVersion is the oldest protocol version either side accepts.
const minVersion = tls.VersionTLS13

// FromEnv returns the credentials the environment asks a client for; plain text if it asks for none.
func FromEnv(getenv func(string) string) (credentials.TransportCredentials, error) {
	on, caFile := false, getenv(EnvCAFile)
	if raw := getenv(EnvTLS); raw != "" {
		var err error
		// A value that cannot be read must not quietly mean plain text.
		if on, err = strconv.ParseBool(raw); err != nil {
			return nil, fmt.Errorf("%s: %q is neither true nor false", EnvTLS, raw)
		}
		if !on && caFile != "" {
			return nil, fmt.Errorf("%s turns TLS off, but %s is set", EnvTLS, EnvCAFile)
		}
	}
	switch {
	case caFile != "":
		return Trusting(caFile)
	case on:
		return credentials.NewTLS(&tls.Config{MinVersion: minVersion}), nil
	default:
		return insecure.NewCredentials(), nil
	}
}

// Trusting returns client credentials that accept only servers vouched for by the PEM file.
func Trusting(caFile string) (credentials.TransportCredentials, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s holds no certificate", caFile)
	}
	return credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: minVersion}), nil
}

// Server returns the credentials governord serves with: the certificate and key in the two PEM files.
func Server(certFile, keyFile string) (credentials.TransportCredentials, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("the certificate and the key must both be named")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: minVersion}), nil
}
