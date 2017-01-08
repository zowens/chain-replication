use std::intrinsics::likely;
use std::io::{self, Write};
use tokio_core::io::{Codec, EasyBuf};
use commitlog::{Offset, MessageSet};
use memchr::memchr;

macro_rules! probably {
    ($e: expr) => (
        unsafe {
            likely($e)
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

#[derive(Default)]
pub struct Protocol;

impl Codec for Protocol {
    /// The type of decoded frames.
    type In = Req;

    /// The type of frames to be encoded.
    type Out = Res;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        let pos = memchr(b'\n', buf.as_slice());

        match pos {
            Some(p) => {
                let mut m = buf.drain_to(p + 1);
                // Remove trailing newline character
                m.split_off(p);

                if probably!(m.len() > 0) {
                    {
                        let data = m.as_slice();
                        if data[0] == b'+' {
                            let s = String::from_utf8_lossy(&data[1..]);
                            return str::parse::<u64>(&s)
                                .map(|n| Some(Req::Read(n)))
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));
                        } else if data[0] == b'?' {
                            return Ok(Some(Req::LastOffset));
                        }
                    }

                    Ok(Some(Req::Append(m)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        match msg {
            Res::Offset(off) => write!(buf, "+{}\n", off.0),
            Res::Messages(msgs) => {
                for m in msgs.iter() {
                    write!(buf, "{}: {}\n", m.offset(), String::from_utf8_lossy(m.payload()))?;
                }
                Ok(())
            }
        }
    }
}
