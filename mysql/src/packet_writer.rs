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

use byteorder::{ByteOrder, LittleEndian};
use std::io;
use std::io::prelude::*;
use std::io::IoSlice;

use crate::U24_MAX;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The writer of mysql packet.
/// - behaves as a sync writer, while build the packet
///   so that trivial async writes could be avoided
/// - behaves like a async writer, while writing data to the output stream
pub struct PacketWriter<W> {
    packet_builder: PacketBuilder,
    output_stream: W,
}

// exports the internal builder as sync Write
impl<W> Write for PacketWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.packet_builder.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.packet_builder.flush()
    }
}

impl<W> PacketWriter<W> {
    pub fn new(output_stream: W) -> Self {
        Self {
            packet_builder: PacketBuilder::new(),
            output_stream,
        }
    }
    pub fn set_seq(&mut self, seq: u8) {
        self.packet_builder.set_seq(seq)
    }
}

const PACKET_HEADER_SIZE: usize = 4;
impl<W: AsyncWrite + Unpin> PacketWriter<W> {
    /// Build packet(s) and write them to the output stream
    pub async fn end_packet(&mut self) -> io::Result<()> {
        let builder = &mut self.packet_builder;
        if !builder.is_empty() {
            let raw_packet = builder.take_buffer();

            // split the rww buffer at the boundary of size U24_MAX
            let chunks = raw_packet.chunks(U24_MAX);
            let mut header = [0; PACKET_HEADER_SIZE];
            for chunk in chunks {
                // prepare the header
                LittleEndian::write_u24(&mut header, chunk.len() as u32);
                header[3] = builder.seq();
                builder.increase_seq();

                // write out the header and payload.
                //
                // depends on the AsyncWrite provided, this may trigger
                // real system call or not (for example, if AsyncWrite is buffered stream)
                let written = self
                    .output_stream
                    .write_vectored(&[IoSlice::new(&header), IoSlice::new(chunk)])
                    .await?;

                // if write buffer is not drained, fall back to write_all
                if written != PACKET_HEADER_SIZE + chunk.len() {
                    let remaining: Vec<u8> = header
                        .iter()
                        .chain(chunk.iter())
                        .skip(written)
                        .cloned()
                        .collect();
                    self.output_stream.write_all(&remaining).await?
                }
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn flush_all(&mut self) -> io::Result<()> {
        self.output_stream.flush().await
    }
}

// Builder that exports as sync `Write`, so that  trivial scattered async writes
// could be avoided during constructing the packet, especially the writes in mod [writers]
struct PacketBuilder {
    buffer: Vec<u8>,
    seq: u8,
}

impl Write for PacketBuilder {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Here we take them all, and split them into raw packets later in `end_packet` if the size
        // of buffer is larger than max payload size (16MB)
        self.buffer.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl PacketBuilder {
    pub fn new() -> Self {
        PacketBuilder {
            buffer: vec![],
            seq: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    fn set_seq(&mut self, seq: u8) {
        self.seq = seq;
    }

    fn increase_seq(&mut self) {
        self.seq = self.seq.wrapping_add(1);
    }

    fn seq(&self) -> u8 {
        self.seq
    }
}
