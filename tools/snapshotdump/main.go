package main

import (
	"flag"
	"fmt"
	"os"

	matchingv1 "github.com/Grizzly1127/trading_matchengine/pkg/pb/matching/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func main() {
	var (
		inPath = flag.String("in", "", "snapshot 文件路径，例如 data/snapshots/shard-0/BTC-USDT/snapshot_15.pb")
		pretty = flag.Bool("pretty", true, "是否格式化 JSON 输出")
	)
	flag.Parse()

	if *inPath == "" {
		_, _ = fmt.Fprintln(os.Stderr, "missing -in")
		flag.Usage()
		os.Exit(2)
	}

	b, err := os.ReadFile(*inPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "read %s: %v\n", *inPath, err)
		os.Exit(1)
	}

	s := &matchingv1.Snapshot{}
	if err := proto.Unmarshal(b, s); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "unmarshal snapshot: %v\n", err)
		os.Exit(1)
	}

	m := protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: false,
		Indent:          "",
	}
	if *pretty {
		m.Indent = "  "
	}

	out, err := m.Marshal(s)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(out)
	_, _ = os.Stdout.Write([]byte("\n"))
}

