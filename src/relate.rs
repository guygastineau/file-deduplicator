/// Find files in a directory hierarchy with the same contents, and group them based on content.

use std::{fs, time, path::PathBuf, io, collections::HashSet};
use sha2::{Sha256, Digest};
use walkdir::WalkDir;

/// This type tracks content equality of files via a SDha256 hash and content size on bytes according to the operating system.
/// The system path is tracked to differentiate files on the filesystem.
/// The creation time is included, so we can prioritize files with equivalent contents using the age.
#[derive(Clone, Debug)]
pub struct FileInfo {
    pub name: PathBuf,
    pub hash: String,
    pub size: u64,
    pub created: time::SystemTime,
}

#[derive(Debug)]
pub enum ErrorType {
    IO(io::Error),
    WrongSize(u64, u64),
    NoCreatedTime(io::Error),
}

#[derive(Debug)]
pub struct Error {
    path: PathBuf,
    error_type: ErrorType,
}

fn io_error<'a>(path: &'a PathBuf) -> impl FnOnce(io::Error) -> Error {
    let path = path.clone();
    move |e| {
        Error {
            path,
            error_type: ErrorType::IO(e),
        }
    }
}

fn wrong_size<'a>(path: &'a PathBuf, expected: u64, actual: u64) -> Error {
    Error {
        path: path.clone(),
        error_type: ErrorType::WrongSize(expected, actual),
    }
}

fn no_created<'a>(path: &'a PathBuf) -> impl FnOnce(io::Error) -> Error {
    let path = path.clone();
    move |e| {
        Error {
            path,
            error_type: ErrorType::NoCreatedTime(e),
        }
    }
}

/// Open file at `path', and produce a `FileInfo' or an `Error'.
pub fn file_info<'a>(path: &'a PathBuf) -> Result<FileInfo, Error> {
    let mut file = fs::File::open(path).map_err(io_error(path))?;
    let metadata = file.metadata().map_err(io_error(path))?;
    let size = metadata.len();
    let created = metadata.created().map_err(no_created(path))?;
    let mut hasher = Sha256::new();
    let n = io::copy(&mut file, &mut hasher).map_err(io_error(path))?;
    if size != n {
        return Err(wrong_size(path, size, n));
    }
    let hash = format!("{:x}", hasher.finalize());
    Ok(FileInfo {
        name: path.clone(),
        hash,
        size,
        created,
    })
}

/// Check the length and hash of two files, `FileInfo', are equal ignoring the path.
pub fn file_content_equal<'a>(file_a: &'a FileInfo, file_b: &'a FileInfo) -> bool {
    file_a.size == file_b.size && file_a.hash == file_b.hash
}

/// Return all unique PathBufs found recursively in `path'.
pub fn walk(path: PathBuf) -> HashSet<PathBuf> {
    WalkDir::new(path)
        .into_iter()
        .filter_map(|e| e.ok())
        .map(|e| e.path().to_path_buf())
        .collect()
}
