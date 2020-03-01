use log::*;
use mapreduce::MasterClient;
use std::io;
use tarpc::{client, context};
use tokio_serde::formats::Json;

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    let server_addr = "localhost:8000";
    let transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default()).await?;

    let mut client = MasterClient::new(client::Config::default(), transport).spawn()?;
    let context = context::current();

    while let Some(task) = client.get_task(context).await? {
        info!("get task: {:#x?}", task);
        client.finish_task(context, task).await?;
    }
    Ok(())
}
