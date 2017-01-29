use std::intrinsics::unlikely;
use tokio_core::io::EasyBuf;
use byteorder::{LittleEndian, ByteOrder};

macro_rules! probably_not {
    ($e: expr) => (
        unsafe {
            unlikely($e)
        }
    )
}

pub type ReqId = u64;
pub type OpCode = u8;

#[inline]
pub fn start_decode(buf: &mut EasyBuf) -> Option<(ReqId, OpCode, EasyBuf)> {
    // must have at least 13 bytes
    if probably_not!(buf.len() < 13) {
        trace!("Not enough characters: {}", buf.len());
        return None;
    }

    // read the length of the message
    let len = {
        let data = buf.as_slice();
        LittleEndian::read_u32(&data[0..4]) as usize
    };

    // ensure we have enough
    if probably_not!(buf.len() < len) {
        return None;
    }

    // drain to the length and request ID (not used yet), then remove the length field
    let mut buf = buf.drain_to(len);

    let reqid = {
        let len_and_reqid = buf.drain_to(12);
        LittleEndian::read_u64(&len_and_reqid.as_slice()[4..12])
    };

    // parse by op code
    let op = buf.drain_to(1).as_slice()[0];

    Some((reqid, op, buf))
}

#[inline]
pub fn encode_header(reqid: ReqId, opcode: OpCode, rest: usize, buf: &mut Vec<u8>) {
    let mut wbuf = [0u8; 13];
    LittleEndian::write_u32(&mut wbuf[0..4], 13 + rest as u32);
    LittleEndian::write_u64(&mut wbuf[4..12], reqid);
    wbuf[12] = opcode;
    buf.extend_from_slice(&wbuf);
}
