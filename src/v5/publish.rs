#[derive(Debug, Clone, PartialEq)]
pub struct Publish {
    pub fixed_header: FixedHeader,
    pub raw: Bytes,
}

impl Publish {
    pub fn view_meta(&self) -> Result<(&str, u8, u16, bool, bool), Error> {
        let qos = (self.fixed_header.byte1 & 0b0110) >> 1;
        let dup = (self.fixed_header.byte1 & 0b1000) != 0;
        let retain = (self.fixed_header.byte1 & 0b0001) != 0;

        // FIXME: Remove indexes and use get method
        let stream = &self.raw[self.fixed_header.fixed_header_len..];
        let topic_len = view_u16(&stream)? as usize;

        let stream = &stream[2..];
        let topic = view_str(stream, topic_len)?;

        let pkid = match qos {
            0 => 0,
            1 => {
                let stream = &stream[topic_len..];
                let pkid = view_u16(stream)?;
                pkid
            }
            v => return Err(Error::InvalidQoS(v)),
        };

        if qos == 1 && pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        Ok((topic, qos, pkid, dup, retain))
    }

    pub fn view_topic(&self) -> Result<&str, Error> {
        // FIXME: Remove indexes
        let stream = &self.raw[self.fixed_header.fixed_header_len..];
        let topic_len = view_u16(&stream)? as usize;

        let stream = &stream[2..];
        let topic = view_str(stream, topic_len)?;
        Ok(topic)
    }

    pub fn take_topic_and_payload(mut self) -> Result<(Bytes, Bytes), Error> {
        let qos = (self.fixed_header.byte1 & 0b0110) >> 1;

        let variable_header_index = self.fixed_header.fixed_header_len;
        self.raw.advance(variable_header_index);
        let topic = read_mqtt_bytes(&mut self.raw)?;

        match qos {
            0 => (),
            1 => self.raw.advance(2),
            v => return Err(Error::InvalidQoS(v)),
        };

        let payload = self.raw;
        Ok((topic, payload))
    }

    pub fn read(fixed_header: FixedHeader, bytes: Bytes) -> Result<Self, Error> {
        let publish = Publish { fixed_header, raw: bytes };

        Ok(publish)
    }
}

pub struct PublishBytes(pub Bytes);

impl From<PublishBytes> for Result<Publish, Error> {
    fn from(raw: PublishBytes) -> Self {
        let fixed_header = check(raw.0.iter(), 100 * 1024 * 1024)?;
        Ok(Publish { fixed_header, raw: raw.0 })
    }
}

pub fn write(
    topic: &str,
    qos: QoS,
    pkid: u16,
    dup: bool,
    retain: bool,
    payload: &[u8],
    buffer: &mut BytesMut,
) -> Result<usize, Error> {
    let mut len = 2 + topic.len();
    if qos != QoS::AtMostOnce {
        len += 2;
    }

    len += payload.len();

    let dup = dup as u8;
    let qos = qos as u8;
    let retain = retain as u8;

    buffer.put_u8(0b0011_0000 | retain | qos << 1 | dup << 3);

    let count = write_remaining_length(buffer, len)?;
    write_mqtt_string(buffer, topic);

    if qos != 0 {
        if pkid == 0 {
            return Err(Error::PacketIdZero);
        }

        buffer.put_u16(pkid);
    }

    buffer.extend_from_slice(&payload);

    // TODO: Returned length is wrong in other packets. Fix it
    Ok(1 + count + len)
}
