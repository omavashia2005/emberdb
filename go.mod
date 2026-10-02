module github.com/omavashia2005/emberdb

go 1.24.5

require (
	github.com/Fusl/go-resp v0.0.0-20250403034534-c4c34ae89024
	github.com/bytechan/resp3 v0.1.2
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.6.0
	github.com/lobaro/crc16 v0.1.0
	github.com/panjf2000/gnet/v2 v2.10.0
)

require (
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/panjf2000/ants/v2 v2.12.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
)

replace github.com/Fusl/go-resp => ./utils/go-resp
