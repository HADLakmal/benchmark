package main

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/linkedin/goavro/v2"
)

// Sample schema for encoding
var schema = `{
	"type": "record",
	"name": "User",
	"fields": [
		{"name": "id", "type": "int"},
		{"name": "name", "type": "string"}
	]
}`

var sampleData = map[string]interface{}{
	"id":   1,
	"name": "John Doe",
}

func BenchmarkHambaAvro(b *testing.B) {
	// Parse schema with Hamba
	parsedSchema, err := avro.Parse(schema)
	if err != nil {
		b.Fatalf("Failed to parse schema: %v", err)
	}

	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		err = avro.NewEncoderForSchema(parsedSchema, buf).Encode(sampleData)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}

	}
}

func BenchmarkGoAvro(b *testing.B) {
	// Parse schema with goavro
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}

	for i := 0; i < b.N; i++ {
		// Create a byte buffer to hold the encoded result
		buf := new(bytes.Buffer)
		binaryData, err := codec.BinaryFromNative(nil, sampleData)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}

		_, err = buf.Write(binaryData)
		if err != nil {
			b.Fatalf("Failed to write to buffer: %v", err)
		}
	}
}

func main() {
	// Run benchmarks
	benchmarks := []testing.InternalBenchmark{
		{Name: "BenchmarkHambaAvro", F: BenchmarkHambaAvro},
		{Name: "BenchmarkGoAvro", F: BenchmarkGoAvro},
	}

	for _, bm := range benchmarks {
		start := time.Now()
		result := testing.Benchmark(bm.F)
		duration := time.Since(start)
		fmt.Println(bm.Name, result, "Duration:", duration.String())
	}
}
