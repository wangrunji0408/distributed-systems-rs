use futures::{
    future::{self, Ready},
    prelude::*,
};
use log::*;
use mapreduce::*;
use tarpc::{
    context,
    server::{self, Handler},
};
use tokio_serde::formats::Json;

#[derive(Clone)]
struct MasterServer {}

impl Master for MasterServer {
    type GetTaskFut = Ready<Option<Task>>;

    fn get_task(self, _: context::Context) -> Self::GetTaskFut {
        info!("get task");
        future::ready(Some(Task::Map {
            id: 0,
            filename: "".to_string(),
            reduce_n: 0,
        }))
    }

    type FinishTaskFut = Ready<()>;

    fn finish_task(self, _: context::Context, _task: Task) -> Self::FinishTaskFut {
        info!("finish task");
        future::ready(())
    }
}

struct MapReduceMaster {}

impl MapReduceMaster {
    async fn run(&mut self) {}
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let transport = tarpc::serde_transport::tcp::listen("localhost:8000", Json::default)
        .await?
        .filter_map(|r| async { r.ok() });

    let server = server::new(server::Config::default())
        .incoming(transport)
        .respond_with(MasterServer {}.serve());
    server.await;
    Ok(())
}
