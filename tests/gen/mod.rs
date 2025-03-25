use std::collections::{HashSet, HashMap, BTreeSet};
use rand::prelude::*;
use itertools::Itertools;

pub type GenInfo = BTreeSet<(usize, BTreeSet<String>)>;

const GROUP_M: u8 = 50;

const DIRS: [&'static str; 9] = [
    "abc",
    "def",
    "ghi",
    "abd/def",
    "abc/ghi",
    "def/ghi",
    "ghi/abc",
    "ghi/def",
    "ghi/abc/def",
];



fn grouped_names<'a, 'b>(rng: &'a mut impl rand::Rng, dir: &'b str, n: usize) -> impl Iterator<Item=BTreeSet<String>> {
    let mut filenames: Vec<String> = (0..n)
        .map(|x| format!("{:}.txt", x))
        .collect();
    filenames.shuffle(rng);
    let prefixes = (0..).into_iter().flat_map(|_| DIRS.into_iter());
    let mut rng = rand::rng();
    let mut map: HashMap<u8,BTreeSet<String>> = HashMap::new();
    for (prefix, filename) in prefixes
        .into_iter()
        .zip(filenames)
    {
        let group = rng.random::<u8>() % GROUP_M;
        let path = format!("{:}/{:}", prefix, filename);
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
        let mut size: u64 = rng.random();
        while size < cfg.min_size && size > cfg.max_size - 1 {
            size = rng.random();
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

pub fn new_conf(file_count: u64, group_count: u64, min_size: u64, max_size: u64) -> Option<Cfg>{
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
        self.rng.fill_bytes(&mut buf[0..n]);
        self.size = self.size - n;
        Ok(n)
    }
}

pub fn gen<'a>(dir: &'a str, cfg: Cfg) -> Option<GenInfo> {
    let mut rng = rand::rng();
    let groups = groups(&cfg, grouped_names(&mut rng, dir, cfg.file_count as usize));
    // Create all necessary directories.
    if let Ok(_) = std::fs::create_dir(dir) {
        for dir in DIRS {
            if let Err(_) = std::fs::create_dir(dir) {
                return None;
            }
        }
    } else {
        return None;
    }
    // Write the random contents to the first file, then we copy that file to all equal files.
    for (size, group) in &groups {
        let mut contents = RandReader {
            rng: &mut rng,
            size: *size,
        };
        let mut group = group.into_iter();
        if let Some(mut first) = group
            .next()
            .map_or(None, |path| match std::fs::File::open(path) {
                Err(_) => None,
                Ok(mut file) => {
                    if let Ok(written) = std::io::copy(&mut contents, &mut file) {
                        if written != *size as u64 {
                            None
                        } else {
                            Some(file)
                        }
                    } else {
                        None
                    }
                },
            })
        {
            for path in group {
                match std::fs::File::open(path) {
                    Err(_) => return None,
                    Ok(mut file) => {
                        if let Ok(written) = std::io::copy(&mut first, &mut file) {
                            if written != *size as u64 {
                                return None;
                            }
                        }
                    }
                }
            }
        } else {
            return None;
        }
    }
    Some(groups)
}
