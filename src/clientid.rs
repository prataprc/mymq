use std::ops::{Deref, DerefMut};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct ClientID(String);

impl Deref for ClientID {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.as_str()
    }
}

impl DerefMut for ClientID {
    fn deref_mut(&mut self) -> &mut str {
        self.0.as_mut_str()
    }
}

impl TryFrom<String> for ClientID {
    type Error = Error;

    fn try_from(_val: String) -> Result<ClientID> {
        todo!()
    }
}
