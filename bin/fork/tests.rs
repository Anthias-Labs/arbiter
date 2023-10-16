use arbiter_core::environment::fork::Fork;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};

use super::*;

const FORK_CONFIG_PATH: &str = "example_fork/weth_config.toml";
const PATH_TO_DISK_STORAGE: &str = "example_fork/test.json";

#[test]
fn create_forked_db() {
    let fork_config = ForkConfig::new(FORK_CONFIG_PATH).unwrap();
    let fork = fork_config.into_fork().unwrap();
    assert!(!fork.db.accounts.is_empty());
}

#[test]
fn write_out() {
    let fork_config = ForkConfig::new(FORK_CONFIG_PATH);
    assert!(fork_config.is_ok());
    let fork_config = fork_config.unwrap();

    // Use par_iter to parallelize the loop
    (0..10).into_par_iter().for_each(|_| {
        let disk_op = fork_config.clone().write_to_disk(&true);
        assert!(disk_op.is_ok());
    });

    fs::remove_file(PATH_TO_DISK_STORAGE).unwrap();
}

#[test]
fn read_in() {
    // First write out so we know the file exists.
    let fork_config = ForkConfig::new(FORK_CONFIG_PATH);
    assert!(fork_config.is_ok());
    let fork_config = fork_config.unwrap();
    let disk_op = fork_config.clone().write_to_disk(&true);
    assert!(disk_op.is_ok());

    let thing = Path::new(PATH_TO_DISK_STORAGE).try_exists().unwrap();
    if thing {
        assert!(thing)
    } else {
        // try again
        let disk_op = fork_config.clone().write_to_disk(&true);
        assert!(disk_op.is_ok());
    }
    // Use par_iter to parallelize the loop
    (0..10).into_par_iter().for_each(|_| {
        let forked_db = Fork::from_disk(PATH_TO_DISK_STORAGE);
        assert!(forked_db.is_ok());
    });
    fs::remove_file(PATH_TO_DISK_STORAGE).unwrap();
}
