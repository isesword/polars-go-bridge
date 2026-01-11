go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

export PATH=$PATH:$GOPATH/bin && protoc --go_out=. --go_opt=paths=source_relative proto/polars_bridge.proto

