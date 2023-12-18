use walkdir::WalkDir;
use std::path::Path;
use std::io;
use csv::Writer;
use humansize::{FileSize, file_size_opts as options};
use rayon::prelude::*;
use std::sync::Mutex;
use std::sync::Arc;
use std::time::Instant;
use rayon::ThreadPoolBuilder;

/// Scans the filesystem for files larger than a certain size and writes their details to a CSV file.
///
/// # Arguments
/// * `start_path` - The path to start scanning from.
/// * `output_csv_path` - The path to the output CSV file.
///
/// # Returns
/// * `io::Result<()>` - Result of the operation.
pub fn scan_large_files(start_path: &str, output_csv_path: &str) -> io::Result<()> {
    const MIN_FILE_SIZE: u64 = 100 * 1024 * 1024; // 100MB in bytes
    let start_time = Instant::now();
    let start_path_obj = Path::new(start_path);
    if !start_path_obj.exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "Start path does not exist."));
    }
    if !start_path_obj.is_dir() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Start path is not a directory."));
    }

    let wtr = Arc::new(Mutex::new(Writer::from_path(output_csv_path)?));
    wtr.lock().unwrap().write_record(&["File Name", "Size", "Full Path"])?;

    let pool = ThreadPoolBuilder::new().num_threads(4).build().unwrap();

    pool.install(|| {
        WalkDir::new(start_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .par_bridge() // Use par_bridge to create a parallel iterator
            .for_each(|entry| {
                let file = entry.path();
                if file.is_file() {
                    if let Ok(metadata) = file.metadata() {
                        if metadata.len() > MIN_FILE_SIZE {
                            let file_name = file.file_name().unwrap().to_string_lossy().to_string();
                            let file_size = metadata.len().file_size(options::CONVENTIONAL).unwrap();
                            let full_path = file.to_string_lossy().to_string();
                            let mut wtr = wtr.lock().unwrap();
                            wtr.write_record(&[&file_name, &file_size, &full_path]).unwrap();
                        }
                    }
                }
            });
    });

    wtr.lock().unwrap().flush()?;

    let duration = start_time.elapsed();

    println!("Time elapsed is: {:?}", duration);

    Ok(())
}