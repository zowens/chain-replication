use std::intrinsics::unlikely;
use std::io;
use tokio_core::io::{Codec, EasyBuf};
use tokio_proto::multiplex::RequestId;
use commitlog::{Offset, MessageBuf, MessageSet};
use byteorder::{LittleEndian, ByteOrder};
use super::asynclog::Messages;

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

    // TODO: move this onto separate server
    //
    // reserved for replicator
    ReplicateFrom(u64),
}

pub enum Res {
    Offset(Offset),
    Messages(Messages),
}

impl From<Offset> for Res {
    fn from(other: Offset) -> Res {
        Res::Offset(other)
    }
}

impl From<Messages> for Res {
    fn from(other: Messages) -> Res {
        Res::Messages(other)
    }
}

/// Custom protocol, since most of the libraries out there are not zero-copy :(
///
/// u32, u64 = <Little Endedian, Unsigned>
///
/// Request = Length RequestId Request
///     Length : u32 = <length of entire request (including headers)>
///     RequestId : u64
///     Request : AppendRequest | ReadLastOffsetRequest | ReadFromOffsetRequest |
///     ReplicateFromRequest
///
/// AppendRequest = OpCode Payload
///     OpCode : u8 = 0
///     Payload : u8* = <bytes up to length>
///
/// ReadLastOffsetRequest = OpCode
///     OpCode : u8 = 1
///
/// ReadFromOffsetRequest = OpCode Offset
///     OpCode : u8 = 2
///     Offset : u64
///
/// ReplicateFromRequest = Opcode Offset
///     OpCode : u8 = 3
///     Offset : u64
///
/// Response = Length RequestId Response
///     Length : u32 = length of entire response (including headers)
///     RequestId : u64
///     Response : OffsetResponse | MessagesResponse
///
/// OffsetResponse = OpCode Offset
///     OpCode : u8 = 0
///     Offset : u64
///
/// MessagesResponse = OpCode MessageBuf
///     OpCode : u8 = 1
///     MessageBuf : Message*
///
/// Message = Offset PayloadSize Hash Payload
///     Offset : u64
///     PayloadSize : u32
///     Hash : u64 = seahash of payload
///     Payload : u8* = <bytes up to PayloadSize>
///
#[derive(Default)]
pub struct Protocol;

impl Codec for Protocol {
    /// The type of decoded frames.
    type In = (RequestId, Req);

    /// The type of frames to be encoded.
    type Out = (RequestId, Res);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        // must have at least 13 bytes
        if probably_not!(buf.len() < 13) {
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

        let reqid = {
            let len_and_reqid = buf.drain_to(12);
            LittleEndian::read_u64(&len_and_reqid.as_slice()[4..12])
        };

        // parse by op code
        let op = buf.drain_to(1).as_slice()[0];
        match op {
            0u8 => Ok(Some((reqid, Req::Append(buf)))),
            1u8 => Ok(Some((reqid, Req::LastOffset))),
            2u8 => {
                // parse out the starting offset
                if probably_not!(buf.len() < 8) {
                    Err(io::Error::new(io::ErrorKind::Other, "Offset not specified for read query"))
                } else {
                    let data = buf.as_slice();
                    let starting_off = LittleEndian::read_u64(&data[0..8]);
                    Ok(Some((reqid, Req::Read(starting_off))))
                }
            },
            3u8 => {
                // parse out the offset
                if probably_not!(buf.len() < 8) {
                    Err(io::Error::new(io::ErrorKind::Other, "Offset not specified for replicate query"))
                } else {
                    let data = buf.as_slice();
                    let off = LittleEndian::read_u64(&data[0..8]);
                    Ok(Some((reqid, Req::ReplicateFrom(off))))
                }
            }
            op => {
                error!("Invalid operation op={:X}", op);
                Err(io::Error::new(io::ErrorKind::Other, "Invalid operation"))
            }

        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let reqid = msg.0;
        match msg.1 {
            Res::Offset(off) => {
                let mut wbuf = [0u8; 12];
                LittleEndian::write_u32(&mut wbuf[0..4], 21);
                LittleEndian::write_u64(&mut wbuf[4..12], reqid);
                // extend w/ size and zero as request ID
                buf.extend_from_slice(&wbuf);
                // op code
                buf.push(0u8);
                LittleEndian::write_u64(&mut wbuf[0..8], off.0);
                // add the offset
                buf.extend_from_slice(&wbuf[0..8]);
            }
            Res::Messages(ms) => {
                let buf_start_len = buf.len();

                // fake out the length, we'll update if after the
                // message set is added
                let mut wbuf = [0u8; 12];
                LittleEndian::write_u64(&mut wbuf[4..12], reqid);
                buf.extend_from_slice(&wbuf);

                // op code
                buf.push(1u8);

                buf.extend_from_slice(ms.bytes());

                // now set the real length
                let msg_len = buf.len() - buf_start_len;
                LittleEndian::write_u32(&mut wbuf[0..4], msg_len as u32);

                buf[buf_start_len..buf_start_len + 4].copy_from_slice(&wbuf[0..4]);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_core::io::{Codec, EasyBuf};
    use byteorder::{ByteOrder, LittleEndian};
    use commitlog::{Message, MessageBuf};

    macro_rules! op {
        ($code: expr, $rid: expr, $payload: expr) => ({
            let mut v = Vec::new();
            let p = $payload;

            let mut size = [0u8; 4];
            LittleEndian::write_u32(&mut size, (13 + p.len()) as u32);
            v.extend(&size);

            let mut requestid = [0u8; 8];
            LittleEndian::write_u64(&mut requestid, $rid);
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
        let mut buf = easy_buf_of(op!(0u8, 123456u64, b"foobarbaz"));
        match codec.decode(&mut buf) {
            Ok(Some((123456u64, Req::Append(buf)))) => {
                assert_eq!(b"foobarbaz", buf.as_slice());
            }
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn decode_last_offset_request() {
        let mut codec = Protocol;
        let mut buf = easy_buf_of(op!(1u8, 123456u64, b"...extra ignored params..."));
        match codec.decode(&mut buf) {
            Ok(Some((123456u64, Req::LastOffset))) => {}
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn decode_read_by_offset() {
        let mut codec = Protocol;

        let mut offset = [0u8; 8];
        LittleEndian::write_u64(&mut offset, 12345u64);
        let mut buf = easy_buf_of(op!(2u8, 54321u64, &offset));
        match codec.decode(&mut buf) {
            Ok(Some((54321u64, Req::Read(off)))) => {
                assert_eq!(12345u64, off);
            }
            _ => panic!("Invalid decode"),
        }
    }

    #[test]
    pub fn encode_offset() {
        let mut codec = Protocol;

        let mut vec = Vec::new();
        codec.encode((12345u64, Res::Offset(Offset(9876543210u64))), &mut vec).unwrap();
        assert_eq!(21, vec.len());
        assert_eq!(21, LittleEndian::read_u32(&vec[0..4]));
        assert_eq!(12345u64, LittleEndian::read_u64(&vec[4..12]));
        assert_eq!(0u8, vec[12]);
        assert_eq!(9876543210u64, LittleEndian::read_u64(&vec[13..]));
    }

    #[test]
    pub fn encode_messageset() {
        let mut codec = Protocol;

        let mut msg_set_bytes = Vec::new();
        Message::serialize(&mut msg_set_bytes, 10, b"1234567");
        Message::serialize(&mut msg_set_bytes, 11, b"abc");
        Message::serialize(&mut msg_set_bytes, 12, b"foobarbaz");

        let msg_set = msg_set_bytes.clone();
        let msg_set = MessageBuf::from_bytes(msg_set).unwrap();

        let mut output = Vec::new();
        let extras = b"some_extra_crap";
        output.extend(extras);

        codec.encode((12345u64, Res::Messages(msg_set)), &mut output).unwrap();
        assert_eq!(extras.len() + msg_set_bytes.len() + 13, output.len());

        let msg_slice = &output[extras.len()..];
        let res_size = LittleEndian::read_u32(&msg_slice[0..4]);
        assert_eq!(res_size as usize, msg_slice.len());
        let req_id = LittleEndian::read_u64(&msg_slice[4..12]);
        assert_eq!(12345u64, req_id);

        // op code
        assert_eq!(1u8, msg_slice[12]);

        assert_eq!(msg_set_bytes.as_slice(), &msg_slice[13..]);
    }
}
