use std::intrinsics::unlikely;
use std::io;
use tokio_core::io::{Codec, EasyBuf};
use commitlog::{Offset, MessageSet};
use byteorder::{LittleEndian, ByteOrder};

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}

pub enum Req {
    Append(EasyBuf),
    Read(u64),
    LastOffset,
}

pub enum Res {
    Offset(Offset),
    Messages(MessageSet),
}

impl From<Offset> for Res {
    fn from(other: Offset) -> Res {
        Res::Offset(other)
    }
}

impl From<MessageSet> for Res {
    fn from(other: MessageSet) -> Res {
        Res::Messages(other)
    }
}

/// Custom protocol, since most of the libraries out there are not zero-copy :(
///
/// REQUEST PROTOCOL:
///
/// - Length (4 bytes, LE u32) [0..4]
///     Total message length including this field
/// - RequestID (4 bytes, LE u32) [4..8]
///     Forward compatibility for multiplexing, ignored for now
/// - Operation (1 byte) [8..9]
///     0 - Append
///     1 - Read last offset
///     2 - Read from offset
/// - Rest: operation-specific fields [9..]
///
/// APPEND REQUEST:
///    Payload
///
/// READ LAST OFFSET REQUEST:
///     Nothing
///
/// READ REQUEST:
///    LE u64 StartingOffset
///
///
///
/// RESPONSE PROTOCOL:
/// - Length (4 bytes, LE u32)
///     Total mesage length including this field
/// - RequestID (4 bytes, LE)
///     Forward compatibility for multiplexing, ignored for now
/// - Response Type (1 byte, LE)
///     0 - Offset
///     1 - MessageSet
/// - Rest: Payload
///
/// OFFSET RESPONSE:
///     BE u64 offset
/// MESSAGE SET:
///     message set encoding
///     .......
///
#[derive(Default)]
pub struct Protocol;

impl Codec for Protocol {
    /// The type of decoded frames.
    type In = Req;

    /// The type of frames to be encoded.
    type Out = Res;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        // must have at least 9 bytes
        if probably_not!(buf.len() < 9) {
            trace!("Not enough characters: {}", buf.len());
            return Ok(None);
        }

        // read the length of the message
        let len = {
            let data = buf.as_slice();
            LittleEndian::read_u32(&data[0..4]) as usize
        };

        // ensure we have enough
        if probably_not!(buf.len() < len) {
            return Ok(None);
        }

        // drain to the length and request ID (not used yet), then remove the length field
        let mut buf = buf.drain_to(len);
        buf.drain_to(8);

        // parse by op code
        let op = buf.drain_to(1).as_slice()[0];
        match op {
            0u8 => Ok(Some(Req::Append(buf))),
            1u8 => Ok(Some(Req::LastOffset)),
            2u8 => {
                // parse out the starting offset
                if probably_not!(buf.len() < 8) {
                    Err(io::Error::new(io::ErrorKind::Other, "Offset not specified for read query"))
                } else {
                    let data = buf.as_slice();
                    let starting_off = LittleEndian::read_u64(&data[0..8]);
                    Ok(Some(Req::Read(starting_off)))
                }
            },
            op => {
                error!("Invalid operation op={:X}", op);
                Err(io::Error::new(io::ErrorKind::Other, "Invalid operation"))
            },

        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            Res::Offset(off) => {
                let mut wbuf = [0u8; 8];
                LittleEndian::write_u32(&mut wbuf[0..4], 17);
                // extend w/ size and zero as request ID
                buf.extend(&wbuf);
                // op code
                buf.push(0u8);
                LittleEndian::write_u64(&mut wbuf, off.0);
                // add the offset
                buf.extend(&wbuf);
            },
            Res::Messages(ms) => {
                let buf_start_len = buf.len();

                // fake out the length, we'll update if after the
                // message set is added
                let mut wbuf = [0u8; 8];
                buf.extend(&wbuf);

                // op code
                buf.push(1u8);

                ms.serialize(buf);

                // now set the real length
                let msg_len = buf.len() - buf_start_len;
                LittleEndian::write_u32(&mut wbuf[0..4], msg_len as u32);

                buf[buf_start_len] = wbuf[0];
                buf[buf_start_len + 1] = wbuf[1];
                buf[buf_start_len + 2] = wbuf[2];
                buf[buf_start_len + 3] = wbuf[3];
            },
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_core::io::{Codec, EasyBuf};
    use byteorder::{ByteOrder, LittleEndian};
    use commitlog::{Message, MessageSet};

    macro_rules! op {
        ($code: expr, $payload: expr) => ({
            let mut v = Vec::new();
            let p = $payload;

            let mut size = [0u8; 4];
            LittleEndian::write_u32(&mut size, (9 + p.len()) as u32);
            v.extend(&size);

            let requestid = [0u8; 4];
            v.extend(&requestid);


            v.push($code);
            v.extend(p);
            v
        })
    }

    fn easy_buf_of(bytes: Vec<u8>) -> EasyBuf {
        let mut buf = EasyBuf::new();

        {
            let mut bufmut = buf.get_mut();
            bufmut.extend_from_slice(bytes.as_ref());
        }

        buf
    }

    #[test]
    pub fn decode_append_request() {
        let mut codec = Protocol;
        let mut buf = easy_buf_of(op!(0u8, b"foobarbaz"));
        match codec.decode(&mut buf) {
            Ok(Some(Req::Append(buf))) => {
                assert_eq!(b"foobarbaz", buf.as_slice());
            },
            _ => panic!("Invalid decode")
        }
    }

    #[test]
    pub fn decode_last_offset_request() {
        let mut codec = Protocol;
        let mut buf = easy_buf_of(op!(1u8, b"...extra ignored params..."));
        match codec.decode(&mut buf) {
            Ok(Some(Req::LastOffset)) => {},
            _ => panic!("Invalid decode")
        }
    }

    #[test]
    pub fn decode_read_by_offset() {
        let mut codec = Protocol;

        let mut offset = [0u8; 8];
        LittleEndian::write_u64(&mut offset, 12345u64);
        let mut buf = easy_buf_of(op!(2u8, &offset));
        match codec.decode(&mut buf) {
            Ok(Some(Req::Read(off))) => {
                assert_eq!(12345u64, off);
            },
            _ => panic!("Invalid decode")
        }
    }

    #[test]
    pub fn encode_offset() {
        let mut codec = Protocol;

        let mut vec = Vec::new();
        codec.encode(Res::Offset(Offset(9876543210u64)), &mut vec).unwrap();
        assert_eq!(17, vec.len());
        assert_eq!(17, LittleEndian::read_u32(&vec[0..4]));
        assert_eq!(0u8, vec[8]);
        assert_eq!(9876543210u64, LittleEndian::read_u64(&vec[9..]));
    }

    #[test]
    pub fn encode_messageset() {
        let mut codec = Protocol;

        let mut msg_set_bytes = Vec::new();
        Message::serialize(&mut msg_set_bytes, 10, b"1234567");
        Message::serialize(&mut msg_set_bytes, 11, b"abc");
        Message::serialize(&mut msg_set_bytes, 12, b"foobarbaz");

        let msg_set = msg_set_bytes.clone();
        let msg_set = MessageSet::from_bytes(msg_set).unwrap();

        let mut output = Vec::new();
        let extras = b"some_extra_crap";
        output.extend(extras);

        codec.encode(Res::Messages(msg_set), &mut output).unwrap();
        assert_eq!(extras.len() + msg_set_bytes.len() + 9, output.len());

        let msg_slice = &output[extras.len()..];
        let res_size = LittleEndian::read_u32(&msg_slice[0..4]);
        assert_eq!(res_size as usize, msg_slice.len());

        // op code
        assert_eq!(1u8, msg_slice[8]);

        assert_eq!(msg_set_bytes.as_slice(), &msg_slice[9..]);
    }
}
