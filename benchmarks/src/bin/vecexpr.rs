use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use arrow::array::Float64Array;
use arrow::compute::kernels::arithmetic;
use arrow::record_batch::RecordBatch;

fn main() {
    let args: Vec<String> = env::args().collect();
    let chunk_size = args[1].parse::<usize>().unwrap();
    let path = "/Users/wenzhengcui/opensource/arrow-workspace/arrow-datafusion/benchmarks/data/lineitem.tbl";
    let file = File::open(path).unwrap();
    let buf_reader = BufReader::new(file);
    let mut v1: Vec<f64> = Vec::new();
    let mut v2: Vec<f64> = Vec::new();

    for line in buf_reader.lines() {
        let linestr = line.unwrap();
        let parts: Vec<&str> = linestr.split('|').collect();
        v1.push(parts[6].parse::<f64>().unwrap());
        v2.push(parts[7].parse::<f64>().unwrap());
    }

    let mut chunks = Vec::new();
    for (chunk1, chunk2) in (v1.chunks(chunk_size)).zip(v2.chunks(chunk_size)) {
        chunks.push((Float64Array::from(Vec::from(chunk1)), Float64Array::from(Vec::from(chunk2))));
    }

    let start = Instant::now();
    for _ in 0..100 {
        for (arr1, arr2) in chunks.iter() {
            let _v3 = arithmetic::add(&arr1, &arr2).unwrap();
        }
    }
    println!("elapsed {} ms", start.elapsed().as_millis() / 100);
}