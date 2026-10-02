// Package gen holds the code generated from the files under proto.
package gen

//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.73.0 lint ../..
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.73.0 generate ../.. --template ../../buf.gen.yaml --output ../..
