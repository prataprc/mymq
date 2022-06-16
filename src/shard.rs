use uuid::Uuid;

use crate::{Result, Session, Shardable};

pub struct Shard {
    uuid: Uuid,
    sessions: Vec<Session>,
}

impl Shard {
    pub fn new() -> Result<Shard> {
        let uuid = Uuid::new_v4();

        let val = Shard { uuid, sessions: Vec::default() };
        Ok(val)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl Shardable for Shard {
    fn uuid(&self) -> Uuid {
        self.uuid
    }
}
