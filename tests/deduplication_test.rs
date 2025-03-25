use file_deduplicator::relate;
use std::{fs,
          sync::mpsc, sync::mpsc::{Sender, Receiver},
          thread,
          collections::BTreeSet,
};

mod gen;

use gen::{Cfg, gen};

const TEST_DIR: &'static str = "test-data";

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

#[test]
fn test_single_file() {
    let _ = fs::remove_dir_all(TEST_DIR);
    let cfg = Cfg::new(1, 1, 1, 10_000_000).unwrap();
    let gen_info = gen(TEST_DIR, cfg).expect(&format!("Failed to generate test data in {:}", TEST_DIR));
    eprintln!("{:?}", &gen_info);
    let (progress_tx, progress_rx): (Sender<f32>, Receiver<f32>) = mpsc::channel();
    let (result_tx, result_rx): (Sender<relate::RelatedFiles>, Receiver<relate::RelatedFiles>) = mpsc::channel();
    let th = thread::spawn(move || {
        let walk_info = relate::WalkInfo::walk(TEST_DIR.into());
        let related = relate::RelatedFiles::relate(&walk_info, &RELATE_CONF, progress_tx);
        let _ = result_tx.send(related);
    });
    let progress = progress_rx.recv().expect("Failed to get progress during file relation.");
    assert!(progress > 0.0, "Unexpected progress value");
    let result = result_rx.recv().expect("Failed to get result from RelatedFile::relate");
    let _ = th.join();
    check_related(&gen_info, &result);
    let _ = fs::remove_dir_all(TEST_DIR);
}
