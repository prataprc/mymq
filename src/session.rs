use crate::{ClientID, Result};

pub struct Session {
    client_id: ClientID,
}

impl Session {
    pub fn close(self) -> Result<()> {
        todo!()
    }
}
