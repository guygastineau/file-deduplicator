use file_deduplicator::relate;
use std::{fs,
          sync::mpsc, sync::mpsc::{Sender, Receiver},
          thread,
          collections::BTreeSet,
};
use serial_test::serial;

mod gen;

use gen::{Cfg, gen};

const TEST_DIR: &'static str = "scratch/data";

const RELATE_CONF: relate::RelateConf = relate::RelateConf {
    max_threads: 12,
    file_threshold: 100,
    size_threshold: 4_000_000_000,
};

fn check_related<'a, 'b>(gen_info: &'a gen::GenInfo, related: &'b relate::RelatedFiles) {
    let related_as_gen_info = related
        .files
        .values()
        .map(|group| {
            let mut size = 0;
            let group = group
                .iter()
                .map(|fi| {
                    size = fi.size;
                    fi.name.to_str().expect(&format!("Failed to convert path, {:?}, to String", fi.name)).to_owned()
                }).collect::<BTreeSet<String>>();
            (size as usize, group)
        }).collect::<gen::GenInfo>();
    assert_eq!(gen_info, &related_as_gen_info);
}

fn test_with_config(cfg: Cfg) {
    let _ = fs::remove_dir_all(TEST_DIR);

    let file_count = cfg.file_count();
    let gen_info = gen(TEST_DIR, cfg).expect(&format!("Failed to generate test data in {:}", TEST_DIR));
    println!("{:?}", &gen_info);
    let (progress_tx, progress_rx): (Sender<f32>, Receiver<f32>) = mpsc::channel();
    let (result_tx, result_rx): (Sender<relate::RelatedFiles>, Receiver<relate::RelatedFiles>) = mpsc::channel();
    let th = thread::spawn(move || {
        let walk_info = relate::WalkInfo::walk(TEST_DIR.into());
        let related = relate::RelatedFiles::relate(&walk_info, &RELATE_CONF, progress_tx);
        let _ = result_tx.send(related);
    });
    let mut progress = 0.0;
    for _ in 0..file_count {
        let new_progress = progress_rx.recv().expect("Failed to get progress during file relation.");
        assert!(progress < new_progress, "Progress did not go up as expected.");
        progress = new_progress;
    }
    assert!(progress > 0.0, "Unexpected progress value");
    let result = result_rx.recv().expect("Failed to get result from RelatedFile::relate");
    println!("{:?}", result);
    let _ = th.join();
    check_related(&gen_info, &result);

    let _ = fs::remove_dir_all(TEST_DIR);
}

#[test]
#[serial]
fn test_single_file() {
    test_with_config(Cfg::new(1, 1, 1, 10_000_000).unwrap());
}

#[test]
#[serial]
fn test_single_group() {
    test_with_config(Cfg::new(10, 1, 1, 10_000_000).unwrap());
}

#[test]
#[serial]
fn test_multiple_groups() {
    test_with_config(Cfg::new(20, 4, 1, 10_000_000).unwrap());
}

#[test]
#[serial]
fn test_with_lots_of_groups_and_files() {
    test_with_config(Cfg::new(200, 30, 1, 10_000_000).unwrap());
}
