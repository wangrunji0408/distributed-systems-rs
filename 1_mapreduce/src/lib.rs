use serde::{Deserialize, Serialize};

pub const DEFAULT_LISTENING_ADDRESS: &str = "localhost:8000";

#[tarpc::service]
pub trait Master {
    /// Get a task from master.
    async fn get_task() -> Option<Task>;

    /// Inform that a task is finished.
    async fn finish_task(task: Task);
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Task {
    Map {
        id: usize,
        filename: String,
        reduce_n: usize,
    },
    Reduce {
        id: usize,
        map_n: usize,
    },
}
