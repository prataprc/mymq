use std::ops::{Deref, DerefMut};

use crate::{Error, ErrorKind, ReasonCode, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct TopicName(String);

impl Deref for TopicName {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.as_str()
    }
}

impl DerefMut for TopicName {
    fn deref_mut(&mut self) -> &mut str {
        self.0.as_mut_str()
    }
}

impl TryFrom<String> for TopicName {
    type Error = Error;

    fn try_from(val: String) -> Result<TopicName> {
        if val.len() == 0 {
            err!(ProtocolError, code: InvalidTopicName, "empty topic-name {:?}", val)
        } else if val.chars().any(|c| c == '#' || c == '+') {
            err!(ProtocolError, code: InvalidTopicName, "has wildcards {:?}", val)
        } else {
            Ok(TopicName(val))
        }
    }
}

impl TopicName {
    pub fn as_levels(&self) -> Vec<&str> {
        self.split('/').collect()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicFilter(String);

impl Deref for TopicFilter {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.as_str()
    }
}

impl DerefMut for TopicFilter {
    fn deref_mut(&mut self) -> &mut str {
        self.0.as_mut_str()
    }
}

impl TryFrom<String> for TopicFilter {
    type Error = Error;

    fn try_from(val: String) -> Result<TopicFilter> {
        if val.len() == 0 {
            err!(ProtocolError, code: InvalidTopicName, "empty topic-name {:?}", val)
        } else {
            Ok(TopicFilter(val))
        }
    }
}

impl TopicFilter {
    pub fn as_levels(&self) -> Vec<&str> {
        self.split('/').collect()
    }
}
