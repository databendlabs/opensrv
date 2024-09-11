// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::io::prelude::*;

use std::iter::Enumerate;
use std::marker::PhantomData;
use std::ops::RangeFrom;

use bytes::BytesMut;
use nom::Needed;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

const PACKET_BUFFER_SIZE: usize = 4_096;
const PACKET_LARGE_BUFFER_SIZE: usize = 1_048_576;

fn calc_new_buf_size(last_buf_size: usize) -> usize {
    if last_buf_size >= PACKET_BUFFER_SIZE * 2 {
        // if packet is already too large, use larger buffer to avoid multiple allocation
        PACKET_LARGE_BUFFER_SIZE
    } else {
        std::cmp::max(PACKET_BUFFER_SIZE, last_buf_size * 2)
    }
}

pub struct PacketReader<R> {
    bytes: bytes::Bytes,
    pub r: R,
}

impl<R> PacketReader<R> {
    pub fn new(r: R) -> Self {
        PacketReader {
            bytes: bytes::Bytes::new(),
            r,
        }
    }
}

impl<R: Read> PacketReader<R> {
    #[allow(dead_code)]
    pub fn next(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        loop {
            if !self.bytes.is_empty() {
                // coping `bytes::Bytes` are very cheap, just move the pointer and increase the ref count.
                match packet(self.bytes.clone().into()) {
                    Ok((rest, p)) => {
                        self.bytes = rest.into();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        let err = Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ));
                        return err;
                    }
                }
            }

            // read more buffer
            let last_buffer_size = self.bytes.len();
            // allocate new buffer if old buffer have packets that is still in use.
            // notice if old buffer have no alive packet, it's space will get reused.
            let mut buf = std::mem::take(&mut self.bytes)
                .try_into_mut()
                .unwrap_or_else(|_| BytesMut::with_capacity(calc_new_buf_size(last_buffer_size)));

            let read_cnt = self.r.read(&mut buf)?;
            buf.truncate(read_cnt);
            self.bytes = buf.freeze();

            // for a [TcpStream], returning zero indicates the connection was shut down correctly.
            if read_cnt == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for PacketReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // if our buffer have content, send those immediately
        if !self.bytes.is_empty() {
            buf.put_slice(&self.bytes);
            self.bytes.clear();
            std::task::Poll::Ready(Ok(()))
        } else {
            std::pin::Pin::new(&mut self.r).poll_read(cx, buf)
        }
    }
}

impl<R: AsyncRead + Unpin> PacketReader<R> {
    pub async fn next_async(&mut self) -> io::Result<Option<(u8, Packet<'_>)>> {
        loop {
            if !self.bytes.is_empty() {
                match packet(self.bytes.clone().into()) {
                    Ok((rest, p)) => {
                        self.bytes = rest.into();
                        return Ok(Some(p));
                    }
                    Err(nom::Err::Incomplete(_)) | Err(nom::Err::Error(_)) => {}
                    Err(nom::Err::Failure(ctx)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{:?}", ctx),
                        ));
                    }
                }
            }

            // read more buffer
            let last_buffer_size = self.bytes.len();
            // allocate new buffer if old buffer have packets that is still in use.
            // notice if old buffer have no alive packet, it's space will get reused.
            let mut buf = std::mem::take(&mut self.bytes)
                .try_into_mut()
                .unwrap_or_else(|_| BytesMut::with_capacity(calc_new_buf_size(last_buffer_size)));

            let read_cnt = self.r.read(&mut buf).await?;
            buf.truncate(read_cnt);

            self.bytes = buf.freeze();

            if read_cnt == 0 {
                if self.bytes.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{} unhandled bytes", self.bytes.len()),
                    ));
                }
            }
        }
    }
}

pub fn fullpacket(i: NomBytes) -> nom::IResult<NomBytes, (u8, NomBytes)> {
    let (i, _) = nom::bytes::complete::tag(&[0xff, 0xff, 0xff][..])(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(U24_MAX)(i)?;
    Ok((i, (seq.as_ref()[0], bytes)))
}

pub fn onepacket(i: NomBytes) -> nom::IResult<NomBytes, (u8, NomBytes)> {
    let (i, length) = nom::number::complete::le_u24(i)?;
    let (i, seq) = nom::bytes::complete::take(1u8)(i)?;
    let (i, bytes) = nom::bytes::complete::take(length)(i)?;
    Ok((i, (seq.as_ref()[0], bytes)))
}

/// Bytes wrapper for nom, allowing nom to parse bytes::Bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NomBytes(bytes::Bytes);

impl NomBytes {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<&[u8]> for NomBytes {
    fn from(value: &[u8]) -> Self {
        NomBytes(bytes::Bytes::copy_from_slice(value))
    }
}

impl From<bytes::Bytes> for NomBytes {
    fn from(value: bytes::Bytes) -> Self {
        NomBytes(value)
    }
}

impl From<NomBytes> for bytes::Bytes {
    fn from(value: NomBytes) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for NomBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl nom::InputTake for NomBytes {
    fn take(&self, count: usize) -> Self {
        NomBytes(self.0.slice(0..count))
    }

    fn take_split(&self, count: usize) -> (Self, Self) {
        let mut prefix = self.0.clone();
        let suffix = prefix.split_off(count);
        (NomBytes(prefix), NomBytes(suffix))
    }
}

impl nom::Compare<&[u8]> for NomBytes {
    fn compare(&self, t: &[u8]) -> nom::CompareResult {
        self.0.as_ref().compare(t)
    }

    fn compare_no_case(&self, t: &[u8]) -> nom::CompareResult {
        self.0.as_ref().compare_no_case(t)
    }
}

impl nom::InputLength for NomBytes {
    fn input_len(&self) -> usize {
        self.0.len()
    }
}

impl nom::InputIter for NomBytes {
    type Item = u8;
    type Iter = Enumerate<Self::IterElem>;
    type IterElem = bytes::buf::IntoIter<bytes::Bytes>;

    #[inline]
    fn iter_indices(&self) -> Self::Iter {
        self.iter_elements().enumerate()
    }
    #[inline]
    fn iter_elements(&self) -> Self::IterElem {
        self.0.clone().into_iter()
    }
    #[inline]
    fn position<P>(&self, predicate: P) -> Option<usize>
    where
        P: Fn(Self::Item) -> bool,
    {
        self.0.iter().position(|b| predicate(*b))
    }
    #[inline]
    fn slice_index(&self, count: usize) -> Result<usize, Needed> {
        if self.0.len() >= count {
            Ok(count)
        } else {
            Err(Needed::new(count - self.0.len()))
        }
    }
}

impl nom::Slice<RangeFrom<usize>> for NomBytes {
    fn slice(&self, range: RangeFrom<usize>) -> Self {
        NomBytes(self.0.slice(range))
    }
}

// a simple wrapper around bytes::Bytes to make sure interface stays the same
#[derive(Clone)]
pub struct Packet<'a> {
    bytes: bytes::Bytes,
    _lifetime: PhantomData<&'a ()>, // NOTE: the lifetime can be removed since Bytes mangaes the lifetime by itself
}

impl<'a> Packet<'a> {
    fn from_bytes(bytes: bytes::Bytes) -> Self {
        Packet {
            bytes,
            _lifetime: PhantomData,
        }
    }
}

use crate::U24_MAX;
use std::ops::Deref;

impl<'a> Deref for Packet<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.bytes.as_ref()
    }
}

pub(crate) fn packet<'a>(i: NomBytes) -> nom::IResult<NomBytes, (u8, Packet<'a>)> {
    nom::combinator::map(
        nom::sequence::pair(
            nom::multi::fold_many0(
                fullpacket,
                || (0, None),
                |(seq, pkt): (_, Option<BytesMut>), (nseq, p)| {
                    let pkt = if let Some(mut pkt) = pkt {
                        assert_eq!(nseq, seq + 1);
                        pkt.extend_from_slice(p.as_ref());
                        Some(pkt)
                    } else {
                        // TODO: avoid copy
                        Some(BytesMut::from(p.0))
                    };
                    (nseq, pkt)
                },
            ),
            onepacket,
        ),
        move |(full, last)| {
            let seq = last.0;
            let pkt = if let Some(mut pkt) = full.1 {
                assert_eq!(last.0, full.0 + 1);
                pkt.extend_from_slice(last.1.as_ref());
                pkt.freeze()
            } else {
                // TODO: avoid copy
                last.1 .0
            };
            (seq, Packet::from_bytes(pkt))
        },
    )(i)
}
