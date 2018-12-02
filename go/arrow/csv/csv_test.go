// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package csv_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/csv"
	"github.com/apache/arrow/go/arrow/memory"
)

func Example() {
	f, err := os.Open("testdata/simple.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	r := csv.NewReader(f, schema, csv.WithComment('#'), csv.WithComma(';'))
	defer r.Release()

	n := 0
	for r.Next() {
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", i, rec.ColumnName(i), col)
		}
		n++
	}

	// Output:
	// rec[0]["i64"]: [0]
	// rec[1]["f64"]: [0]
	// rec[2]["str"]: ["str-0"]
	// rec[0]["i64"]: [1]
	// rec[1]["f64"]: [1]
	// rec[2]["str"]: ["str-1"]
	// rec[0]["i64"]: [2]
	// rec[1]["f64"]: [2]
	// rec[2]["str"]: ["str-2"]
	// rec[0]["i64"]: [3]
	// rec[1]["f64"]: [3]
	// rec[2]["str"]: ["str-3"]
	// rec[0]["i64"]: [4]
	// rec[1]["f64"]: [4]
	// rec[2]["str"]: ["str-4"]
	// rec[0]["i64"]: [5]
	// rec[1]["f64"]: [5]
	// rec[2]["str"]: ["str-5"]
	// rec[0]["i64"]: [6]
	// rec[1]["f64"]: [6]
	// rec[2]["str"]: ["str-6"]
	// rec[0]["i64"]: [7]
	// rec[1]["f64"]: [7]
	// rec[2]["str"]: ["str-7"]
	// rec[0]["i64"]: [8]
	// rec[1]["f64"]: [8]
	// rec[2]["str"]: ["str-8"]
	// rec[0]["i64"]: [9]
	// rec[1]["f64"]: [9]
	// rec[2]["str"]: ["str-9"]
}

func Example_withChunk() {
	f, err := os.Open("testdata/simple.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	r := csv.NewReader(
		f, schema,
		csv.WithComment('#'), csv.WithComma(';'),
		csv.WithChunk(3),
	)
	defer r.Release()

	n := 0
	for r.Next() {
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Printf("rec[%d][%q]: %v\n", i, rec.ColumnName(i), col)
		}
		n++
	}

	// Output:
	// rec[0]["i64"]: [0 1 2]
	// rec[1]["f64"]: [0 1 2]
	// rec[2]["str"]: ["str-0" "str-1" "str-2"]
	// rec[0]["i64"]: [3 4 5]
	// rec[1]["f64"]: [3 4 5]
	// rec[2]["str"]: ["str-3" "str-4" "str-5"]
	// rec[0]["i64"]: [6 7 8]
	// rec[1]["f64"]: [6 7 8]
	// rec[2]["str"]: ["str-6" "str-7" "str-8"]
	// rec[0]["i64"]: [9]
	// rec[1]["f64"]: [9]
	// rec[2]["str"]: ["str-9"]
}

func TestCSVReader(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	raw, err := ioutil.ReadFile("testdata/types.csv")
	if err != nil {
		t.Fatal(err)
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "bool", Type: arrow.FixedWidthTypes.Boolean},
			arrow.Field{Name: "i8", Type: arrow.PrimitiveTypes.Int8},
			arrow.Field{Name: "i16", Type: arrow.PrimitiveTypes.Int16},
			arrow.Field{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
			arrow.Field{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
			arrow.Field{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
			arrow.Field{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
			arrow.Field{Name: "f32", Type: arrow.PrimitiveTypes.Float32},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	r := csv.NewReader(bytes.NewReader(raw), schema,
		csv.WithAllocator(mem),
		csv.WithComment('#'), csv.WithComma(';'),
	)
	defer r.Release()

	r.Retain()
	r.Release()

	if got, want := r.Schema(), schema; !got.Equal(want) {
		t.Fatalf("invalid schema: got=%v, want=%v", got, want)
	}

	out := new(bytes.Buffer)

	n := 0
	for r.Next() {
		rec := r.Record()
		for i, col := range rec.Columns() {
			fmt.Fprintf(out, "rec[%d][%q]: %v\n", i, rec.ColumnName(i), col)
		}
		n++
	}

	if got, want := n, 2; got != want {
		t.Fatalf("invalid number of rows: got=%d, want=%d", got, want)
	}

	want := `rec[0]["bool"]: [true]
rec[1]["i8"]: [-1]
rec[2]["i16"]: [-1]
rec[3]["i32"]: [-1]
rec[4]["i64"]: [-1]
rec[5]["u8"]: [1]
rec[6]["u16"]: [1]
rec[7]["u32"]: [1]
rec[8]["u64"]: [1]
rec[9]["f32"]: [1.1]
rec[10]["f64"]: [1.1]
rec[11]["str"]: ["str-1"]
rec[0]["bool"]: [false]
rec[1]["i8"]: [-2]
rec[2]["i16"]: [-2]
rec[3]["i32"]: [-2]
rec[4]["i64"]: [-2]
rec[5]["u8"]: [2]
rec[6]["u16"]: [2]
rec[7]["u32"]: [2]
rec[8]["u64"]: [2]
rec[9]["f32"]: [2.2]
rec[10]["f64"]: [2.2]
rec[11]["str"]: ["str-2"]
`

	if got, want := out.String(), want; got != want {
		t.Fatalf("invalid output:\ngot= %s\nwant=%s\n", got, want)
	}

	if r.Err() != nil {
		t.Fatalf("unexpected error: %v", r.Err())
	}

	// test error modes
	{
		r := csv.NewReader(bytes.NewReader(raw), schema,
			csv.WithAllocator(mem),
			csv.WithComment('#'), csv.WithComma(';'),
		)

		r.Next()
		r.Record()

		r.Release()
	}
}

func TestCSVReaderWithChunk(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	raw, err := ioutil.ReadFile("testdata/simple.csv")
	if err != nil {
		t.Fatal(err)
	}

	schema := arrow.NewSchema(
		[]arrow.Field{
			arrow.Field{Name: "i64", Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: "f64", Type: arrow.PrimitiveTypes.Float64},
			arrow.Field{Name: "str", Type: arrow.BinaryTypes.String},
		},
		nil,
	)

	for _, tc := range []struct {
		name    string
		opts    []csv.Option
		records int
		want    string
	}{
		{
			name:    "chunk=default",
			opts:    []csv.Option{csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';')},
			records: 10,
			want: `rec[0]["i64"]: [0]
rec[1]["f64"]: [0]
rec[2]["str"]: ["str-0"]
rec[0]["i64"]: [1]
rec[1]["f64"]: [1]
rec[2]["str"]: ["str-1"]
rec[0]["i64"]: [2]
rec[1]["f64"]: [2]
rec[2]["str"]: ["str-2"]
rec[0]["i64"]: [3]
rec[1]["f64"]: [3]
rec[2]["str"]: ["str-3"]
rec[0]["i64"]: [4]
rec[1]["f64"]: [4]
rec[2]["str"]: ["str-4"]
rec[0]["i64"]: [5]
rec[1]["f64"]: [5]
rec[2]["str"]: ["str-5"]
rec[0]["i64"]: [6]
rec[1]["f64"]: [6]
rec[2]["str"]: ["str-6"]
rec[0]["i64"]: [7]
rec[1]["f64"]: [7]
rec[2]["str"]: ["str-7"]
rec[0]["i64"]: [8]
rec[1]["f64"]: [8]
rec[2]["str"]: ["str-8"]
rec[0]["i64"]: [9]
rec[1]["f64"]: [9]
rec[2]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=0",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(0),
			},
			records: 10,
			want: `rec[0]["i64"]: [0]
rec[1]["f64"]: [0]
rec[2]["str"]: ["str-0"]
rec[0]["i64"]: [1]
rec[1]["f64"]: [1]
rec[2]["str"]: ["str-1"]
rec[0]["i64"]: [2]
rec[1]["f64"]: [2]
rec[2]["str"]: ["str-2"]
rec[0]["i64"]: [3]
rec[1]["f64"]: [3]
rec[2]["str"]: ["str-3"]
rec[0]["i64"]: [4]
rec[1]["f64"]: [4]
rec[2]["str"]: ["str-4"]
rec[0]["i64"]: [5]
rec[1]["f64"]: [5]
rec[2]["str"]: ["str-5"]
rec[0]["i64"]: [6]
rec[1]["f64"]: [6]
rec[2]["str"]: ["str-6"]
rec[0]["i64"]: [7]
rec[1]["f64"]: [7]
rec[2]["str"]: ["str-7"]
rec[0]["i64"]: [8]
rec[1]["f64"]: [8]
rec[2]["str"]: ["str-8"]
rec[0]["i64"]: [9]
rec[1]["f64"]: [9]
rec[2]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=1",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(1),
			},
			records: 10,
			want: `rec[0]["i64"]: [0]
rec[1]["f64"]: [0]
rec[2]["str"]: ["str-0"]
rec[0]["i64"]: [1]
rec[1]["f64"]: [1]
rec[2]["str"]: ["str-1"]
rec[0]["i64"]: [2]
rec[1]["f64"]: [2]
rec[2]["str"]: ["str-2"]
rec[0]["i64"]: [3]
rec[1]["f64"]: [3]
rec[2]["str"]: ["str-3"]
rec[0]["i64"]: [4]
rec[1]["f64"]: [4]
rec[2]["str"]: ["str-4"]
rec[0]["i64"]: [5]
rec[1]["f64"]: [5]
rec[2]["str"]: ["str-5"]
rec[0]["i64"]: [6]
rec[1]["f64"]: [6]
rec[2]["str"]: ["str-6"]
rec[0]["i64"]: [7]
rec[1]["f64"]: [7]
rec[2]["str"]: ["str-7"]
rec[0]["i64"]: [8]
rec[1]["f64"]: [8]
rec[2]["str"]: ["str-8"]
rec[0]["i64"]: [9]
rec[1]["f64"]: [9]
rec[2]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=3",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(3),
			},
			records: 4,
			want: `rec[0]["i64"]: [0 1 2]
rec[1]["f64"]: [0 1 2]
rec[2]["str"]: ["str-0" "str-1" "str-2"]
rec[0]["i64"]: [3 4 5]
rec[1]["f64"]: [3 4 5]
rec[2]["str"]: ["str-3" "str-4" "str-5"]
rec[0]["i64"]: [6 7 8]
rec[1]["f64"]: [6 7 8]
rec[2]["str"]: ["str-6" "str-7" "str-8"]
rec[0]["i64"]: [9]
rec[1]["f64"]: [9]
rec[2]["str"]: ["str-9"]
`,
		},
		{
			name: "chunk=6",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(6),
			},
			records: 2,
			want: `rec[0]["i64"]: [0 1 2 3 4 5]
rec[1]["f64"]: [0 1 2 3 4 5]
rec[2]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5"]
rec[0]["i64"]: [6 7 8 9]
rec[1]["f64"]: [6 7 8 9]
rec[2]["str"]: ["str-6" "str-7" "str-8" "str-9"]
`,
		},
		{
			name: "chunk=10",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(10),
			},
			records: 1,
			want: `rec[0]["i64"]: [0 1 2 3 4 5 6 7 8 9]
rec[1]["f64"]: [0 1 2 3 4 5 6 7 8 9]
rec[2]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5" "str-6" "str-7" "str-8" "str-9"]
`,
		},
		{
			name: "chunk=11",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(11),
			},
			records: 1,
			want: `rec[0]["i64"]: [0 1 2 3 4 5 6 7 8 9]
rec[1]["f64"]: [0 1 2 3 4 5 6 7 8 9]
rec[2]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5" "str-6" "str-7" "str-8" "str-9"]
`,
		},
		{
			name: "chunk=-1",
			opts: []csv.Option{
				csv.WithAllocator(mem), csv.WithComment('#'), csv.WithComma(';'),
				csv.WithChunk(-1),
			},
			records: 1,
			want: `rec[0]["i64"]: [0 1 2 3 4 5 6 7 8 9]
rec[1]["f64"]: [0 1 2 3 4 5 6 7 8 9]
rec[2]["str"]: ["str-0" "str-1" "str-2" "str-3" "str-4" "str-5" "str-6" "str-7" "str-8" "str-9"]
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := csv.NewReader(bytes.NewReader(raw), schema, tc.opts...)

			defer r.Release()

			r.Retain()
			r.Release()

			if got, want := r.Schema(), schema; !got.Equal(want) {
				t.Fatalf("invalid schema: got=%v, want=%v", got, want)
			}

			out := new(bytes.Buffer)

			n := 0
			for r.Next() {
				rec := r.Record()
				for i, col := range rec.Columns() {
					fmt.Fprintf(out, "rec[%d][%q]: %v\n", i, rec.ColumnName(i), col)
				}
				n++
			}

			if got, want := n, tc.records; got != want {
				t.Fatalf("invalid number of records: got=%d, want=%d", got, want)
			}

			if got, want := out.String(), tc.want; got != want {
				t.Fatalf("invalid output:\ngot:\n%s\nwant:\n%s\n", got, want)
			}

			if r.Err() != nil {
				t.Fatalf("unexpected error: %v", r.Err())
			}
		})
	}
}
