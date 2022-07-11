use std::ops::Deref;

use crate::v5;

pub enum ResponseInfo {
    ClientID,
}

impl ResponseInfo {
    fn get_response_info(&self, connect: &v5::Connect) -> Option<String> {
        match connect.properties.as_ref()?.request_response_info.as_ref()? {
            true => match self {
                ResponseInfo::ClientID => Some(connect.payload.client_id.deref().clone()),
            },
            false => None,
        }
    }
}
