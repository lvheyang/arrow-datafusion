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

//! Execution plan for reading IPC files


use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::datasource::datasource::Statistics;
use crate::physical_plan::{ExecutionPlan, Partitioning};

/// Execution plan for reading ipc batches of data
#[derive(Debug, Clone)]
pub struct IPCExec {
    /// File path
    path: String,
    /// File extension
    file_extension: String,
    /// Schema representing the data after the optional projection is applied
    schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    batch_size: usize,
    /// Limit in nr. of rows
    limit: Option<usize>,
    /// Statistics for the data set (sum of statistics for all partitions)
    statistics: Statistics,
}

#[async_trait]
impl ExecutionPlan for IPCExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        todo!()
    }
}

