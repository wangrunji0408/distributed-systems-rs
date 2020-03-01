use async_std::{
    prelude::*,
    sync::{channel, Receiver, Sender},
};
use log::*;
use mapreduce::*;
use std::{future::Future, pin::Pin};
use tarpc::{
    context,
    server::{self, Handler},
};
use tokio_serde::formats::Json;

#[derive(Clone)]
struct MasterServer {
    pending_tasks_receiver: Receiver<Task>,
    finish_tasks_sender: Sender<Task>,
}

impl Master for MasterServer {
    type GetTaskFut = Pin<Box<dyn Future<Output = Option<Task>> + Send + 'static>>;

    fn get_task(self, _: context::Context) -> Self::GetTaskFut {
        Box::pin(async move {
            let task = self.pending_tasks_receiver.recv().await;
            info!("get task: {:?}", task);
            task
        })
    }

    type FinishTaskFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

    fn finish_task(self, _: context::Context, task: Task) -> Self::FinishTaskFut {
        Box::pin(async move {
            info!("finish task: {:?}", task);
            self.finish_tasks_sender.send(task).await;
        })
    }
}

struct MapReduceMaster {
    input_files: Vec<String>,
    reduce_n: usize,
    pending_tasks_sender: Sender<Task>,
    pending_tasks_receiver: Receiver<Task>,
    finish_tasks_sender: Sender<Task>,
    finish_tasks_receiver: Receiver<Task>,
}

impl MapReduceMaster {
    fn new(input_files: Vec<String>, reduce_n: usize) -> Self {
        // FIXME: fix block when buffer is small
        let (sender1, receiver1) = channel(10);
        let (sender2, receiver2) = channel(10);
        MapReduceMaster {
            input_files,
            reduce_n,
            pending_tasks_sender: sender1,
            pending_tasks_receiver: receiver1,
            finish_tasks_sender: sender2,
            finish_tasks_receiver: receiver2,
        }
    }

    /// Get RPC server
    fn server(&self) -> MasterServer {
        MasterServer {
            pending_tasks_receiver: self.pending_tasks_receiver.clone(),
            finish_tasks_sender: self.finish_tasks_sender.clone(),
        }
    }

    /// Run the MapReduce task.
    async fn run(self) {
        // map tasks
        for (id, path) in self.input_files.iter().enumerate() {
            let task = Task::Map {
                id,
                filename: path.clone(),
                reduce_n: self.reduce_n,
            };
            self.pending_tasks_sender.send(task).await;
        }
        // wait for complete
        for _ in self.input_files.iter() {
            self.finish_tasks_receiver.recv().await;
        }
        // reduce tasks
        for id in 0..self.reduce_n {
            let task = Task::Reduce {
                id,
                map_n: self.input_files.len(),
            };
            self.pending_tasks_sender.send(task).await;
        }
        // wait for complete
        for _ in 0..self.reduce_n {
            self.finish_tasks_receiver.recv().await;
        }
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let inputs = std::env::args().skip(1).collect();
    let reduce_n = 3;

    let master = MapReduceMaster::new(inputs, reduce_n);

    let transport = tarpc::serde_transport::tcp::listen("localhost:8000", Json::default)
        .await?
        .filter_map(|r| r.ok());
    let server = server::new(server::Config::default())
        .incoming(transport)
        .respond_with(master.server().serve());
    tokio::spawn(server);

    master.run().await;
    Ok(())
}
