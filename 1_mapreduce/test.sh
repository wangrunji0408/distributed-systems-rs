cargo build
cargo run --bin master -- data/pg*.txt &
wait 1
cargo run --bin worker &
cargo run --bin worker &
cargo run --bin worker &
