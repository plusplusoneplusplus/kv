module rocksdb_svc/benchmark

go 1.18

require (
	github.com/apache/thrift v0.16.0
	github.com/tecbot/gorocksdb v0.0.0-20191217155057-f0fad39f321c
	google.golang.org/grpc v1.50.0
	rocksdb_svc v0.0.0-00010101000000-000000000000
)

replace rocksdb_svc => ../

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.12.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230815205213-6bfd019c3878 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
