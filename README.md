# raft

go raft consensus implementation

## Install
```go
import "github.com/treeforest/raft"
// go mod vendor
```

## Usage
**tip**: you can see the example in /example folder.

Step 1: init a raft node
```go
node := raft.New(...)
```

Step 2: start node
```go
node.Start()
```

Step 3: stop node
```go
node.Stop()
```

## Benchmark

**Environment**  
CPU: 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz   2.42 GHz   
Memory: 16G  
Go: 1.17.3  
OS: Windows 11

**QPS**  
≈300 request/second