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

use std::sync::Arc;

use arrow::{
    // array::{Float32Array, Float64Array},
    array::{Float64Array},
    compute,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow::array::ArrayRef;
use criterion::Criterion;

use datafusion::error::Result;

fn query(ctx: Arc<Vec<RecordBatch>>) {
    for batch in ctx.iter() {
        let col0: &ArrayRef = batch.column(0);
        let col1: &ArrayRef = batch.column(1);

        let _ret = compute::add(col0.as_any().downcast_ref::<Float64Array>().unwrap(), col1.as_any().downcast_ref::<Float64Array>().unwrap()).unwrap();
    }
}

fn create_context(
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Vec<RecordBatch>>> {
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
            )
                .unwrap()
        })
        .collect::<Vec<_>>();

    Ok(Arc::new(batches))
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 6000001; // 2^20
    let batch_size = 4096; // 2^9
    let ctx = create_context(array_len, batch_size).unwrap();
    c.bench_function("add_2_column_native", |b| b.iter(|| query(ctx.clone())));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
