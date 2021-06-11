// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

extern crate arrow;
#[macro_use]
extern crate criterion;
extern crate datafusion;

use std::sync::{Arc, Mutex};

use arrow::{
    array::Float64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::Criterion;
use tokio::runtime::Runtime;

use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;

fn query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();

    // execute the query
    let df = ctx.lock().unwrap().sql(&sql).unwrap();
    rt.block_on(df.collect()).unwrap();
}

fn create_context(
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<ExecutionContext>>> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("f32", DataType::Float64, false),
        Field::new("f64", DataType::Float64, false),
    ]));

    // define data.
    let batches = (0..array_len / batch_size)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Float64Array::from(vec![i as f64; batch_size])),
                    Arc::new(Float64Array::from(vec![i as f64; batch_size])),
                ],
            ).unwrap()
        })
        .collect::<Vec<_>>();

    let mut ctx = ExecutionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("t", Arc::new(provider))?;

    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 6000001; // 2^20
    let batch_size = 4096; // 2^9
    let ctx = create_context(array_len, batch_size).unwrap();
    c.bench_function("add_2_column_stream", |b| {
        b.iter(|| query(ctx.clone(), "SELECT f32+f64 FROM t"))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
