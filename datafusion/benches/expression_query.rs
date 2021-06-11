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
    array::Float64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::Criterion;

use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::Operator;
use datafusion::physical_plan::expressions::{BinaryExpr, col};
use datafusion::physical_plan::PhysicalExpr;

fn query(ctx: Arc<Vec<RecordBatch>>) {
    let expressions = vec![BinaryExpr::new(col("f32"), Operator::Plus, col("f64"))];

    for batch in ctx.iter() {
        let _ret = expressions
            .iter()
            .map(|expr| expr.evaluate(&batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()
            .map_or_else(
                |e| Err(DataFusionError::into_arrow_external_error(e)),
                |arrays| RecordBatch::try_new(batch.schema(), arrays),
            );
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
            ).unwrap()
        })
        .collect::<Vec<_>>();

    Ok(Arc::new(batches))
}

fn criterion_benchmark(c: &mut Criterion) {
    let array_len = 6000001; // 2^20
    let batch_size = 4096; // 2^9
    let ctx = create_context(array_len, batch_size).unwrap();
    c.bench_function("add_2_column_expr", |b| b.iter(|| query(ctx.clone())));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
