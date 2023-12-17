mod scanner;

use std::env;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <directory_to_scan> <output_csv_file>", args[0]);
        process::exit(1);
    }

    let directory_to_scan = &args[1];
    let output_csv_file = &args[2];

    if let Err(e) = scanner::scan_large_files(directory_to_scan, output_csv_file) {
        eprintln!("Error occurred: {}", e);
        process::exit(1);
    }

    println!("Scan complete. Results are in '{}'", output_csv_file);
}
