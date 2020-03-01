use log::*;
use mapreduce::{MasterClient, Task};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs::create_dir_all;
use std::hash::Hasher;
use std::io;
use std::path::{Path, PathBuf};
use tarpc::{
    client,
    context::{self, Context},
};
use tokio::fs::File;
use tokio::prelude::*;
use tokio_serde::formats::Json;

struct MapReduceWorker {
    client: MasterClient,
    context: Context,
    tempdir: PathBuf,
    map_fn: MapFn,
    reduce_fn: ReduceFn,
}

impl MapReduceWorker {
    /// Run worker until no more task received.
    async fn run(&mut self) -> io::Result<()> {
        while let Some(task) = self.client.get_task(self.context).await? {
            info!("get task: {:?}", task);
            match &task {
                Task::Map {
                    id,
                    filename,
                    reduce_n,
                } => {
                    self.map(*id, filename, *reduce_n).await?;
                }
                &Task::Reduce { id, map_n } => {
                    self.reduce(id, map_n).await?;
                }
            }
            self.client.finish_task(self.context, task).await?;
        }
        Ok(())
    }

    /// Do a map task.
    async fn map(&self, id: usize, filename: &str, reduce_n: usize) -> io::Result<()> {
        // read input file
        let content = read_to_string(filename).await?;

        // do map
        let out = (self.map_fn)(&content);

        // group by reduce ID
        let mut reduce_kvs = vec![vec![]; reduce_n];
        for kv in out {
            let reduce_id = hash(&kv.0) as usize % reduce_n;
            reduce_kvs[reduce_id].push(kv);
        }

        // write to intermediate file
        for (reduce_id, kvs) in reduce_kvs.iter().enumerate() {
            let filename = format!("mr-{}-{}", id, reduce_id);
            let mut file = File::create(self.tempdir.join(filename)).await?;
            let content = serde_json::to_string(kvs).unwrap();
            file.write_all(content.as_bytes()).await?;
        }
        Ok(())
    }

    /// Do a reduce task.
    async fn reduce(&self, id: usize, map_n: usize) -> io::Result<()> {
        // read intermediate files
        let mut kvss = BTreeMap::<String, Vec<String>>::new();
        for map_id in 0..map_n {
            let filename = format!("mr-{}-{}", map_id, id);
            let content = read_to_string(self.tempdir.join(filename)).await?;
            let kvs: Vec<(String, String)> = serde_json::from_str(&content).unwrap();
            for (key, value) in kvs {
                kvss.entry(key).or_default().push(value);
            }
        }

        // do reduces and write output to file
        let filename = format!("mr-out-{}", id);
        let mut file = File::create(self.tempdir.join(filename)).await?;
        for (key, values) in kvss {
            let out = (self.reduce_fn)(&key, values);
            file.write_all(format!("{} {}\n", key, out).as_bytes())
                .await?;
        }
        Ok(())
    }
}

/// Hash a string.
fn hash(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(s.as_ref());
    hasher.finish()
}

/// Read the entire contents of a file into a string.
async fn read_to_string(path: impl AsRef<Path>) -> std::io::Result<String> {
    let mut file = File::open(path).await?;
    let mut content = String::new();
    file.read_to_string(&mut content).await?;
    Ok(content)
}

type MapFn = fn(content: &str) -> Vec<(String, String)>;
type ReduceFn = fn(key: &str, values: Vec<String>) -> String;

/// Load map and reduce function from dylib.
fn load_fn(path: &str) -> std::io::Result<(MapFn, ReduceFn)> {
    let lib = libloading::Library::new(path)?;
    unsafe {
        let map_fn = lib.get::<MapFn>(b"map")?;
        let reduce_fn = lib.get::<ReduceFn>(b"reduce")?;
        // leak library and get raw functions
        let map_fn: MapFn = core::mem::transmute(map_fn.into_raw().into_raw());
        let reduce_fn: ReduceFn = core::mem::transmute(reduce_fn.into_raw().into_raw());
        core::mem::forget(lib);
        Ok((map_fn, reduce_fn))
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    let app_path = std::env::args().nth(1).expect("no input: xxx.so");
    let (map_fn, reduce_fn) = load_fn(&app_path)?;

    let tempdir = PathBuf::from("mr-temp");
    create_dir_all(&tempdir)?;

    // construct worker
    let transport = tarpc::serde_transport::tcp::connect("localhost:8000", Json::default()).await?;
    let client = MasterClient::new(client::Config::default(), transport).spawn()?;
    let context = context::current();
    let mut worker = MapReduceWorker {
        client,
        context,
        tempdir,
        map_fn,
        reduce_fn,
    };

    // run
    worker.run().await?;

    Ok(())
}
