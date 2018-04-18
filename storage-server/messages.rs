use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;

use byteorder::{ByteOrder, LittleEndian};
use commitlog::message::{MessageBuf, MessageError, MessageSet, MessageSetMut};
use commitlog::reader::LogSliceReader;
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
            offset: offset as u64,
            bytes: bytes,
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
}

/// Message buf that will release to the pool once dropped.
pub struct PooledMessageBuf {
    inner: MessagesInner,
}

impl PooledMessageBuf {
    pub fn new_unpooled(buf: MessageBuf) -> PooledMessageBuf {
        PooledMessageBuf {
            inner: MessagesInner::Unpooled(buf),
        }
    }

    #[inline]
    pub fn push<B: AsRef<[u8]>>(&mut self, client_id: u32, client_req_id: u32, payload: B) {
        let mut meta = [0u8; 8];
        LittleEndian::write_u32(&mut meta[0..4], client_id);
        LittleEndian::write_u32(&mut meta[4..8], client_req_id);
        match self.inner {
            MessagesInner::Pooled(ref mut co) => co.0.push_with_metadata(&meta, payload),
            MessagesInner::Unpooled(ref mut buf) => buf.push_with_metadata(&meta, payload),
        }
    }
}

impl MessageSet for PooledMessageBuf {
    fn bytes(&self) -> &[u8] {
        match self.inner {
            MessagesInner::Pooled(ref co) => co.0.bytes(),
            MessagesInner::Unpooled(ref buf) => buf.bytes(),
        }
    }

    fn len(&self) -> usize {
        match self.inner {
            MessagesInner::Pooled(ref co) => co.0.len(),
            MessagesInner::Unpooled(ref buf) => buf.len(),
        }
    }
}

impl AsMut<[u8]> for PooledMessageBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        match self.inner {
            MessagesInner::Pooled(ref mut co) => co.0.bytes_mut(),
            MessagesInner::Unpooled(ref mut buf) => buf.bytes_mut(),
        }
    }
}

impl MessageSetMut for PooledMessageBuf {
    type ByteMut = PooledMessageBuf;
    fn bytes_mut(&mut self) -> &mut PooledMessageBuf {
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
            buf_bytes: buf_bytes,
            pool: Pool::with_capacity(capacity, 0, move || {
                BufWrapper(MessageBuf::from_bytes(Vec::with_capacity(buf_bytes)).unwrap())
            }),
        }
    }

    /// Gets a new buffer from the pool.
    pub fn take(&mut self) -> PooledMessageBuf {
        match self.pool.checkout() {
            Some(buf) => PooledMessageBuf {
                inner: MessagesInner::Pooled(buf),
            },
            None => PooledMessageBuf::new_unpooled(
                MessageBuf::from_bytes(Vec::with_capacity(self.buf_bytes)).unwrap(),
            ),
        }
    }
}
