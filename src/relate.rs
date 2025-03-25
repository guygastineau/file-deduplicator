/// Find files in a directory hierarchy with the same contents, and group them based on content.

use std::{
    fs, time, time::Duration,
    path::PathBuf, io,
    collections::{HashSet, HashMap},
    sync::mpsc, sync::mpsc::{Sender, Receiver, RecvTimeoutError},
    thread,
};
use sha2::{Sha256, Digest};
use walkdir::WalkDir;
use itertools::Itertools;

/// This type tracks content equality of files via a SDha256 hash and content size on bytes according to the operating system.
/// The system path is tracked to differentiate files on the filesystem.
/// The creation time is included, so we can prioritize files with equivalent contents using the age.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HashedFile {
    pub hash: String,
    pub info: FileInfo,
}

unsafe impl Send for HashedFile {}

#[derive(Debug)]
pub enum ErrorType {
    IO(io::Error),
    WalkDir(walkdir::Error),
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

fn walkdir_error<'a>(path: &'a PathBuf) -> impl FnOnce(walkdir::Error) -> Error {
    let path = path.clone();
    move |e| {
        Error {
            path,
            error_type: ErrorType::WalkDir(e),
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
pub fn hash_from_file_info<'a>(info: &'a FileInfo) -> Result<HashedFile, Error> {
    let mut file = fs::File::open(&info.name).map_err(io_error(&info.name))?;
    let mut hasher = Sha256::new();
    let n = io::copy(&mut file, &mut hasher).map_err(io_error(&info.name))?;
    if info.size != n {
        return Err(wrong_size(&info.name, info.size, n));
    }
    let hash = format!("{:x}", hasher.finalize());
    Ok(HashedFile {
        hash,
        info: info.clone(),
    })
}

/// Check the length and hash of two files, `FileInfo', are equal ignoring the path.
pub fn file_content_equal<'a>(file_a: &'a HashedFile, file_b: &'a HashedFile) -> bool {
    file_a.info.size == file_b.info.size && file_a.hash == file_b.hash
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileInfo {
    pub name: PathBuf,
    pub size: u64,
    pub created: time::SystemTime,
}

pub struct WalkInfo {
    pub total_size: u64,
    pub files: HashSet<FileInfo>,
    pub errors: Vec<Error>,
}

impl FileInfo {
    fn from_entry(entry: walkdir::DirEntry) -> Result<Self, Error> {
        let metadata = entry.metadata().map_err(walkdir_error(&entry.path().to_path_buf()))?;
        let size = metadata.len();
        let created = metadata.created().map_err(no_created(&entry.path().to_path_buf()))?;
        Ok(Self {
            name: entry.path().to_path_buf(),
            size,
            created,
        })
    }
}

unsafe impl Send for FileInfo {}

impl WalkInfo {
    fn new() -> Self {
        WalkInfo {
            total_size: 0,
            files: HashSet::new(),
            errors: Vec::new(),
        }
    }

    fn insert_error(self, error: Error) -> Self {
        let total_size = self.total_size;
        let files = self.files;
        let mut errors = self.errors;
        errors.push(error);
        Self {
            total_size,
            files,
            errors,
        }
    }

    fn insert_entry(self, entry: walkdir::DirEntry) -> Self {
        match FileInfo::from_entry(entry) {
            Err(e) => self.insert_error(e),
            Ok(fi) => {
                let total_size = self.total_size + fi.size;
                let mut files = self.files;
                let errors = self.errors;
                files.insert(fi);
                Self { total_size, files, errors }
            }
        }
    }

    /// Return all unique PathBufs found recursively in `path'.
    pub fn walk(path: PathBuf) -> Self {
        WalkDir::new(path)
            .into_iter()
            .fold(WalkInfo::new(), |acc, entry| {
                match entry {
                    Err(e) => acc.insert_error(Error { path: "<no path>".to_owned().into(), error_type: ErrorType::IO(e.into()) }),
                    Ok(entry) => acc.insert_entry(entry),
                }
            })
    }
}

pub struct RelatedFiles {
    pub files: HashMap<String, HashSet<FileInfo>>,
    pub errors: Vec<Error>,
}

impl RelatedFiles {
    pub fn relate<'a, 'b>(walk: &'a WalkInfo, conf: &'b RelateConf, report: Sender<f32>) -> Self {
        if walk.total_size as usize > conf.size_threshold && walk.files.len() > conf.size_threshold {
            return Self::relate_sequential(walk, report);
        }
        // We've met the criteria for parallel execution.
        let (tx, rx): (Sender<Result<HashedFile, Error>>, Receiver<Result<HashedFile, Error>>) = mpsc::channel();
        let mut done = 0;
        let mut threads = Vec::new();
        let total = walk.total_size;
        let chunk_size = total / conf.max_threads as u64;
        for chunk in &walk.files.iter().chunks(if chunk_size > 1 { chunk_size as usize } else { 1 }) {
            let tx = tx.clone();
            let chunk = chunk.into_iter().cloned().collect::<Vec<FileInfo>>();
            let child = thread::spawn(move || {
                chunk.into_iter().for_each(|info| {
                    let file = hash_from_file_info(&info);
                    tx.send(file).expect("Relate manager died unexpectedly!");
                })
            });
            threads.push(child);
        }
        let mut files: HashMap<String, HashSet<FileInfo>> = HashMap::new();
        let mut errors = Vec::new();
        while !threads.iter().all(|th| th.is_finished()) {
            match rx.recv_timeout(Duration::from_millis(500)) {
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
                Ok(result) => {
                    match result {
                        Err(err) => {
                            errors.push(err);
                        },
                        Ok(file) => {
                            match files.get_mut(&file.hash) {
                                Some(hs) => {
                                    hs.insert(file.info);
                                },
                                None => {
                                    let mut hs = HashSet::new();
                                    hs.insert(file.info);
                                    files.insert(file.hash, hs);
                                }
                            }
                        },
                    }
                    done += 1;
                    report.send(done as f32 / total as f32).expect("Failed to send results to parent!");
                },
            }
        }
        report.send(1.0).expect("Failed to send results to parent!");
        threads.into_iter().for_each(|th| {
            let _ = th.join();
        });
        Self { files, errors }
    }

    pub fn relate_sequential<'a>(walk: &'a WalkInfo, report: Sender<f32>) -> Self {
        let mut done = 0;
        let total = walk.total_size;
        let mut files: HashMap<String, HashSet<FileInfo>> = HashMap::new();
        let mut errors = Vec::new();
        walk.files
            .iter()
            .for_each(|info| {
                match hash_from_file_info(&info) {
                    Err(err) => {
                        errors.push(err);
                    },
                        Ok(file) => {
                            match files.get_mut(&file.hash) {
                                Some(hs) => {
                                    hs.insert(file.info);
                                },
                                None => {
                                    let mut hs = HashSet::new();
                                    hs.insert(file.info);
                                    files.insert(file.hash, hs);
                                }
                            }
                        },
                }
                done += 1;
                report.send(done as f32 / total as f32).expect("Failed to send results to parent!");
            });
        Self { files, errors }
    }
}

/// Configure the relating process, since it could be expensive with lots of large files.
pub struct RelateConf {
    /// Max number of threads to utilize when it is deemed worthwhile.
    /// `0` will be changed to 1.
    pub max_threads: u16,
    /// How many files present before parallelizing.
    pub file_threshold: usize,
    /// Total size of files before parallelizing.
    pub size_threshold: usize,
}
