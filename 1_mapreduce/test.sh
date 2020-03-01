export ARGS=--release
export APP=word_count

# build master & worker
cargo build ${ARGS}

# build app
rustc -O --crate-type cdylib src/mrapps/${APP}.rs -o target/${APP}.so

# run
cargo run ${ARGS} --bin master -- data/pg*.txt &
sleep 1
cargo run ${ARGS} --bin worker -- target/${APP}.so &
cargo run ${ARGS} --bin worker -- target/${APP}.so &
cargo run ${ARGS} --bin worker -- target/${APP}.so &

wait

# merge outputs
sort mr-temp/mr-out* | grep . > mr-temp/mr-wc-all
