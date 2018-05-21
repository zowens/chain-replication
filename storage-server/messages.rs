use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;

use byteorder::{ByteOrder, LittleEndian};
use bytes::BytesMut;
use commitlog::{
    message::{MessageBuf, MessageError, MessageSet, MessageSetMut}, reader::LogSliceReader, Offset,
};
use libc;
use nix;
use nix::errno::Errno;
use pool::*;

pub struct FileSlice {
    file: RawFd,
    offset: u64,
    bytes: usize,
}

impl FileSlice {
    pub fn send(&mut self, socket: RawFd) -> Result<(), io::Error> {
        debug!(
            "Attempting write. Offset={}, bytes={}",
            self.offset, self.bytes
        );

        match self.sendfile(socket) {
            Ok(sent) => {
                debug!(
                    "Sent {} bytes. New offset={}, num_bytes={}",
                    sent, self.offset, self.bytes
                );
                Ok(())
            }
            Err(e) => {
                error!("Err {}", e);
                Err(e)
            }
        }
    }

    #[cfg(any(target_os = "macos"))]
    fn sendfile(&mut self, socket: RawFd) -> Result<usize, io::Error> {
        let off = self.offset as libc::off_t;
        let mut len = self.bytes as libc::off_t;

        let ret = unsafe {
            libc::sendfile(
                self.file,
                socket,
                off,
                &mut len as &mut _,
                ptr::null_mut(),
                0,
            )
        };
        match Errno::result(ret) {
            Ok(_) => {
                let sent = len as usize;
                self.offset += sent as u64;
                self.bytes = self.bytes.saturating_sub(sent);
                Ok(sent)
            }
            Err(nix::Error::Sys(err)) => Err(io::Error::from_raw_os_error(err as i32)),
            Err(_) => unreachable!(),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn sendfile(&mut self, socket: RawFd) -> Result<usize, io::Error> {
        let mut off = self.offset as i64;
        let len = self.bytes as usize;
        let ret = unsafe { libc::sendfile(socket, self.file, &mut off as *mut _, len) };
        match Errno::result(ret) {
            Ok(0) => {
                debug!("Connection closed");
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "Connection closed while sending",
                ))
            }
            Ok(sent) => {
                let sent = sent as usize;
                self.offset += sent as u64;
                self.bytes = self.bytes.saturating_sub(sent);
                Ok(sent)
            }
            Err(nix::Error::Sys(err)) => Err(io::Error::from_raw_os_error(err as i32)),
            Err(_) => unreachable!(),
        }
    }

    pub fn completed(&self) -> bool {
        self.bytes == 0
    }

    pub fn remaining_bytes(&self) -> usize {
        self.bytes
    }

    pub fn file_offset(&self) -> u64 {
        self.offset
    }
}

pub struct FileSliceMessageReader;

impl LogSliceReader for FileSliceMessageReader {
    type Result = Option<FileSlice>;

    fn read_from(
        &mut self,
        file: &File,
        offset: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError> {
        Ok(Some(FileSlice {
            file: file.as_raw_fd(),
            offset: u64::from(offset),
            bytes,
        }))
    }

    fn empty() -> Self::Result {
        None
    }
}

/// Wrapper to reset the buffer
struct BufWrapper(MessageBuf);
impl Reset for BufWrapper {
    fn reset(&mut self) {
        unsafe {
            self.0.unsafe_clear();
        }
    }
}

enum MessagesInner {
    Pooled(Checkout<BufWrapper>),
    Unpooled(MessageBuf),
    Replicated(BytesMut),
}

/// Message buf that will release to the pool once dropped.
pub struct Messages(MessagesInner);

impl Messages {
    /// Creates a buffer that acts like a replicated buffer but is unpooled
    pub fn new_unpooled(buf: MessageBuf) -> Messages {
        Messages(MessagesInner::Unpooled(buf))
    }

    /// Creates a buffer that is from an upstream replica
    pub fn from_replication(bytes: BytesMut) -> Messages {
        Messages(MessagesInner::Replicated(bytes))
    }

    #[inline]
    pub fn push<B: AsRef<[u8]>>(&mut self, client_id: u32, client_req_id: u32, payload: B) {
        let mut meta = [0u8; 8];
        LittleEndian::write_u32(&mut meta[0..4], client_id);
        LittleEndian::write_u32(&mut meta[4..8], client_req_id);
        match self.0 {
            MessagesInner::Pooled(ref mut co) => co.0.push_with_metadata(&meta, payload),
            MessagesInner::Unpooled(ref mut buf) => buf.push_with_metadata(&meta, payload),
            MessagesInner::Replicated(_) => panic!("Cannot append to a replicated message"),
        }
    }

    pub fn next_offset(&self) -> Option<Offset> {
        self.iter().last().map(|m| m.offset() + 1)
    }
}

impl MessageSet for Messages {
    fn bytes(&self) -> &[u8] {
        match self.0 {
            MessagesInner::Pooled(ref co) => co.0.bytes(),
            MessagesInner::Unpooled(ref buf) => buf.bytes(),
            MessagesInner::Replicated(ref buf) => &buf,
        }
    }

    fn len(&self) -> usize {
        match self.0 {
            MessagesInner::Pooled(ref co) => co.0.len(),
            MessagesInner::Unpooled(ref buf) => buf.len(),
            MessagesInner::Replicated(_) => self.iter().count(),
        }
    }
}

impl AsMut<[u8]> for Messages {
    fn as_mut(&mut self) -> &mut [u8] {
        match self.0 {
            MessagesInner::Pooled(ref mut co) => co.0.bytes_mut(),
            MessagesInner::Unpooled(ref mut buf) => buf.bytes_mut(),
            MessagesInner::Replicated(ref mut buf) => buf,
        }
    }
}

impl MessageSetMut for Messages {
    type ByteMut = Messages;
    fn bytes_mut(&mut self) -> &mut Messages {
        self
    }
}

/// Pool of message buffers.
pub struct MessageBufPool {
    buf_bytes: usize,
    pool: Pool<BufWrapper>,
}

impl MessageBufPool {
    /// Creates a new message buf pool.;
    pub fn new(capacity: usize, buf_bytes: usize) -> MessageBufPool {
        MessageBufPool {
            buf_bytes,
            pool: Pool::with_capacity(capacity, 0, move || {
                BufWrapper(MessageBuf::from_bytes(Vec::with_capacity(buf_bytes)).unwrap())
            }),
        }
    }

    /// Gets a new buffer from the pool.
    pub fn take(&mut self) -> Messages {
        match self.pool.checkout() {
            Some(buf) => Messages(MessagesInner::Pooled(buf)),
            None => Messages::new_unpooled(
                MessageBuf::from_bytes(Vec::with_capacity(self.buf_bytes)).unwrap(),
            ),
        }
    }
}
