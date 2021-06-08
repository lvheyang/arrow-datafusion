use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

fn main() {
    let args: Vec<String> = env::args().collect();
    let chunk_size = args[1].parse::<usize>().unwrap();
    let path = "./data/lineitem.tbl";
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

    let start = Instant::now();
    for _ in 0..100 {
        for (chunk1, chunk2) in (v1.chunks(chunk_size)).zip(v2.chunks(chunk_size)) {
            let _v3: Vec<f64> = chunk1.iter().zip(chunk2.iter()).map(|x| x.0 + x.1).collect();
        }
    }
    println!("elapsed {} ms", start.elapsed().as_millis() / 100)
}