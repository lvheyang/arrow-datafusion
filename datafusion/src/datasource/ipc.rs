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


use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::datasource::datasource::Statistics;
use crate::error::Result;

/// Represents a IPC file
pub struct IPCTable {
    path: String,
    file_extension: String,
    schema: SchemaRef,
    statistics: Statistics,
    max_concurrency: usize,
}


impl IPCTable {
    /// Attempt to initialize a new `IPCTable` from a file path
    pub fn try_new(path: &str, max_concurrency: usize) -> Result<Self> {
        Ok(IPCTable {
            path: path.to_string(),
            file_extension: ".arrow_file".to_string(),
            schema: Arc::new(()),
            statistics: Default::default(),
            max_concurrency,
        })
    }
}
}