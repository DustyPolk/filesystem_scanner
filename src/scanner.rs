use walkdir::WalkDir;
use std::path::Path;
use std::io;
use csv::Writer;
use humansize::{FileSize, file_size_opts as options};
use rayon::prelude::*;

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

    let start_path_obj = Path::new(start_path);
    if !start_path_obj.exists() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "Start path does not exist."));
    }
    if !start_path_obj.is_dir() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Start path is not a directory."));
    }

    let mut wtr = Writer::from_path(output_csv_path)?;
    wtr.write_record(&["File Name", "Size", "Full Path"])?;

    let entries: Vec<_> = WalkDir::new(start_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .collect();

    let results: Vec<_> = entries.par_iter()
        .filter_map(|entry| {
            let file = entry.path();
            if file.is_file() {
                if let Ok(metadata) = file.metadata() {
                    if metadata.len() > MIN_FILE_SIZE {
                        let file_name = file.file_name()?.to_string_lossy().to_string();
                        let file_size = metadata.len().file_size(options::CONVENTIONAL).ok()?;
                        let full_path = file.to_string_lossy().to_string();
                        return Some((file_name, file_size, full_path));
                    }
                }
            }
            None
        })
        .collect();

    for (file_name, file_size, full_path) in results {
        wtr.write_record(&[&file_name, &file_size, &full_path])?;
    }

    wtr.flush()?;
    Ok(())
}
