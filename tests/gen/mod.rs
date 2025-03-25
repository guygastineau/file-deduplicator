use std::collections::{HashSet, HashMap, BTreeSet};
use rand::prelude::*;
use itertools::Itertools;

pub type GenInfo = BTreeSet<(usize, BTreeSet<String>)>;

const DIRS: [&'static str; 9] = [
    "abc",
    "def",
    "ghi",
    "abc/def",
    "abc/ghi",
    "def/ghi",
    "ghi/abc",
    "ghi/def",
    "ghi/abc/def",
];



fn grouped_names<'a, 'b>(rng: &'a mut impl rand::Rng, dir: &'b str, n: usize, group_m: u64) -> impl Iterator<Item=BTreeSet<String>> {
    let mut filenames: Vec<String> = (0..n)
        .map(|x| format!("{:}.txt", x))
        .collect();
    filenames.shuffle(rng);
    let prefixes = (0..).into_iter().flat_map(|_| DIRS.into_iter());
    let mut rng = rand::rng();
    let mut map: HashMap<usize,BTreeSet<String>> = HashMap::new();
    for (prefix, filename) in prefixes
        .into_iter()
        .zip(filenames)
    {
        let group = (rng.random::<u64>() % group_m) as usize;
        let path = format!("{:}/{:}/{:}", dir, prefix, filename);
        if let Some(set) = map.get_mut(&group) {
            set.insert(path);
        } else {
            let mut set = BTreeSet::new();
            set.insert(path);
            map.insert(group, set);
        }
    }
    map.into_values()
}

fn groups<'a>(cfg: &'a Cfg, it: impl Iterator<Item=BTreeSet<String>>) -> GenInfo {
    let mut rng = rand::rng();
    let mut xs = BTreeSet::new();
    for group in it {
        let mut size: u64 = rng.random::<u64>() % cfg.max_size;
        while size < cfg.min_size {
            size = rng.random::<u64>() % cfg.max_size;
        }
        xs.insert((size as usize, group));
    }
    xs
}


pub struct Cfg {
    file_count: u64,
    group_count: u64,
    min_size: u64,
    max_size: u64,
}

impl Cfg {
    pub fn new(file_count: u64, group_count: u64, min_size: u64, max_size: u64) -> Option<Cfg>{
        if min_size >= max_size {
            None
        } else {
            Some(
                Cfg {
                    file_count,
                    group_count,
                    min_size,
                    max_size,
                }
            )
        }
    }
}

struct RandReader<'a, R: Rng> {
    rng: &'a mut R,
    size: usize,
}

impl<'a, R: Rng> std::io::Read for RandReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = if self.size < buf.len() {
            self.size
        } else {
            buf.len()
        };
        eprintln!("Writing {:} bytes from src of size {:} to buffer of size {:}", n, self.size, buf.len());
        if n == 0 {
            return Ok(0);
        }
        self.rng.fill_bytes(&mut buf[0..n]);
        eprintln!("Bytes written");
        self.size = self.size - n;
        eprintln!("New size is {:}", self.size);
        Ok(n)
    }
}

pub fn gen<'a>(base_path: &'a str, cfg: Cfg) -> Option<GenInfo> {
    let mut rng = rand::rng();
    let groups = groups(&cfg, grouped_names(&mut rng, base_path, cfg.file_count as usize, cfg.group_count));
    // Create all necessary directories.
    if let Ok(_) = std::fs::create_dir(base_path) {
        for dir in DIRS {
            if let Err(_) = std::fs::create_dir(format!("{:}/{:}", base_path, dir)) {
                eprintln!("Failed to create directory {:}", format!("{:}/{:}", base_path, dir));
                return None;
            }
        }
    } else {
        eprintln!("Failed to create base directory {:}", base_path);
        return None;
    }
    // Write the random contents to the first file, then we copy that file to all equal files.
    for (size, group) in &groups {
        let mut contents = RandReader {
            rng: &mut rng,
            size: *size,
        };
        let mut group = group.iter();
        let first_path = group.next().expect("There are no file groups");
        eprintln!("Creating first file for group {:}", first_path);
        match std::fs::File::create(first_path) {
            Err(_) => {
                eprintln!("Failed to open file {:}", first_path);
                return None;
            },
            Ok(mut file) => {
                if let Ok(written) = std::io::copy(&mut contents, &mut file) {
                    if written != *size as u64 {
                        eprintln!("Failed to write {:} bytes to file {:}", *size, first_path);
                        return None;
                    }
                } else {
                    eprintln!("Failed to write to {:}", first_path);
                    return None;
                }
            },
        }
        for path in group {
            eprintln!("Copying to file {:}", path);
            match std::fs::File::create(path) {
                Err(_) => {
                    eprintln!("Failed to open file {:}", path);
                    return None;
                },
                Ok(mut file) => {
                    let mut first = std::fs::File::open(first_path).expect(&format!("Failed to read file {:}", first_path));
                    if let Ok(written) = std::io::copy(&mut first, &mut file) {
                        if written != *size as u64 {
                            eprintln!("Failed to write {:} bytes to file {:}", *size, path);
                            return None;
                        }
                    } else {
                        eprintln!("Failed to copy bytes to {:}", path);
                        return None;
                    }
                }
            }
        }
    }
    Some(groups)
}
