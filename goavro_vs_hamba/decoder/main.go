package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/hamba/avro/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/linkedin/goavro/v2"
)

type SimpleRecord struct {
	ID   int64  `avro:"id" json:"id"`
	Name string `avro:"name" json:"name"`
}

// Avro schema for the SimpleRecord struct
var schema = `{
	"type": "record",
	"name": "SimpleRecord",
	"fields": [
		{"name": "id", "type": "long"},
		{"name": "name", "type": "string"}
	]
}`

var sampleData = SimpleRecord{
	ID:   1,
	Name: "John Doe",
}

// Benchmark Hamba Avro for Encoding
func BenchmarkHambaAvroEncode(b *testing.B) {
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

// Benchmark GoAvro for Encoding
func BenchmarkGoAvroEncode(b *testing.B) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}

	for i := 0; i < b.N; i++ {
		s := SimpleRecord{}
		byt, err := json.Marshal(&s)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}
		native, _, err := codec.NativeFromTextual(byt)
		if err != nil {
			b.Fatalf("native from textual failed: %v", err)
		}
		buf := new(bytes.Buffer)
		binaryData, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}

		_, err = buf.Write(binaryData)
		if err != nil {
			b.Fatalf("Failed to write to buffer: %v", err)
		}
	}
}

// Benchmark GoAvro for Encoding
func BenchmarkGoAvroGoJsonEncode(b *testing.B) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}

	for i := 0; i < b.N; i++ {
		byt, err := gojson.Marshal(&sampleData)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}
		native, _, err := codec.NativeFromTextual(byt)
		if err != nil {
			b.Fatalf("native from textual failed: %v", err)
		}
		buf := new(bytes.Buffer)
		binaryData, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}

		_, err = buf.Write(binaryData)
		if err != nil {
			b.Fatalf("Failed to write to buffer: %v", err)
		}
	}
}

func BenchmarkGoAvroJsonIteratorEncode(b *testing.B) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	for i := 0; i < b.N; i++ {
		byt, err := json.Marshal(&sampleData)
		if err != nil {
			b.Fatalf("failed to marshal: %v", err)
		}
		native, _, err := codec.NativeFromTextual(byt)
		if err != nil {
			b.Fatalf("native from textual failed: %v", err)
		}
		buf := new(bytes.Buffer)
		binaryData, err := codec.BinaryFromNative(nil, native)
		if err != nil {
			b.Fatalf("Failed to encode: %v", err)
		}

		_, err = buf.Write(binaryData)
		if err != nil {
			b.Fatalf("Failed to write to buffer: %v", err)
		}
	}
}

// Benchmark Hamba Avro for Decoding
func BenchmarkHambaAvroDecode(b *testing.B) {
	parsedSchema, err := avro.Parse(schema)
	if err != nil {
		b.Fatalf("Failed to parse schema: %v", err)
	}

	// Encode sample data for decoding test
	buf := new(bytes.Buffer)
	err = avro.NewEncoderForSchema(parsedSchema, buf).Encode(sampleData)
	if err != nil {
		b.Fatalf("Failed to encode sample data: %v", err)
	}

	encodedData := buf.Bytes()

	for i := 0; i < b.N; i++ {
		decoder := avro.NewDecoderForSchema(parsedSchema, bytes.NewReader(encodedData))
		var decoded SimpleRecord
		err = decoder.Decode(&decoded)
		if err != nil {
			b.Fatalf("Failed to decode: %v", err)
		}
	}
}

// Benchmark GoAvro for Decoding
func BenchmarkGoAvroDecode(b *testing.B) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}

	// Encode sample data for decoding test
	binaryData, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"id":   sampleData.ID,
		"name": sampleData.Name,
	})
	if err != nil {
		b.Fatalf("Failed to encode sample data: %v", err)
	}

	for i := 0; i < b.N; i++ {
		native, _, err := codec.NativeFromBinary(binaryData)
		if err != nil {
			b.Fatalf("Failed to decode: %v", err)
		}
		byt, err := codec.TextualFromNative(nil, native)
		if err != nil {
			b.Fatalf("Failed to textual from native: %v", err)
		}
		s := SimpleRecord{}
		err = gojson.Unmarshal(byt, &s)
		if err != nil {
			b.Fatalf("Failed to unmarshal: %v", err)
		}
		_ = s
	}
}

func BenchmarkGoAvroGoJsonDecode(b *testing.B) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}

	// Encode sample data for decoding test
	binaryData, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"id":   sampleData.ID,
		"name": sampleData.Name,
	})
	if err != nil {
		b.Fatalf("Failed to encode sample data: %v", err)
	}

	for i := 0; i < b.N; i++ {
		native, _, err := codec.NativeFromBinary(binaryData)
		if err != nil {
			b.Fatalf("Failed to decode: %v", err)
		}
		byt, err := codec.TextualFromNative(nil, native)
		if err != nil {
			b.Fatalf("Failed to textual from native: %v", err)
		}
		s := SimpleRecord{}
		err = gojson.Unmarshal(byt, &s)
		if err != nil {
			b.Fatalf("Failed to unmarshal: %v", err)
		}
		_ = s
	}
}

func BenchmarkGoAvroJsonIteratorDecode(b *testing.B) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		b.Fatalf("Failed to create codec: %v", err)
	}

	// Encode sample data for decoding test
	binaryData, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"id":   sampleData.ID,
		"name": sampleData.Name,
	})
	if err != nil {
		b.Fatalf("Failed to encode sample data: %v", err)
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	for i := 0; i < b.N; i++ {
		native, _, err := codec.NativeFromBinary(binaryData)
		if err != nil {
			b.Fatalf("Failed to decode: %v", err)
		}
		byt, err := codec.TextualFromNative(nil, native)
		if err != nil {
			b.Fatalf("Failed to textual from native: %v", err)
		}
		s := SimpleRecord{}
		err = json.Unmarshal(byt, &s)
		if err != nil {
			b.Fatalf("Failed to unmarshal: %v", err)
		}
		_ = s
	}
}

func main() {
	// Run benchmarks
	benchmarks := []testing.InternalBenchmark{
		{Name: "BenchmarkHambaAvroEncode", F: BenchmarkHambaAvroEncode},
		{Name: "BenchmarkGoLinkdinEncode", F: BenchmarkGoAvroEncode},
		{Name: "BenchmarkGoJSONEncode", F: BenchmarkGoAvroGoJsonEncode},
		{Name: "BenchmarkJSONIteratorEncode", F: BenchmarkGoAvroJsonIteratorEncode},
		{Name: "BenchmarkHambaAvroDecode", F: BenchmarkHambaAvroDecode},
		{Name: "BenchmarkGoLinkdinDecode", F: BenchmarkGoAvroDecode},
		{Name: "BenchmarkGoJSONDecode", F: BenchmarkGoAvroGoJsonDecode},
		{Name: "BenchmarkJSONIteratorDecode", F: BenchmarkGoAvroJsonIteratorDecode},
	}

	for _, bm := range benchmarks {
		start := time.Now()
		result := testing.Benchmark(bm.F)
		duration := time.Since(start)
		fmt.Println(bm.Name, result, "Duration:", duration.String())
	}
}
