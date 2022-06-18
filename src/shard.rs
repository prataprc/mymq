use uuid::Uuid;

use crate::{Session, Shardable};

pub struct Shard {
    pub uuid: Uuid,
    sessions: Vec<Session>,
}

impl Default for Shard {
    fn default() -> Shard {
        Shard { uuid: Uuid::new_v4(), sessions: Vec::default() }
    }
}

impl Shardable for Shard {
    fn uuid(&self) -> Uuid {
        self.uuid
    }
}
