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

use std::io::IoSlice;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf, ReadHalf, WriteHalf};
use tokio_rustls::server::TlsStream;
use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};

use crate::commands::ClientHandshake;
use crate::myc::constants::CapabilityFlags;
use crate::packet_reader::PacketReader;
use crate::packet_writer::PacketWriter;
use crate::{AsyncMysqlIntermediary, AsyncMysqlShim, IntermediaryOptions};

pub async fn plain_run_with_options<B, R, W>(
    shim: B,
    writer: W,
    opts: IntermediaryOptions,
    init_params: (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
) -> Result<(), B::Error>
where
    B: AsyncMysqlShim<W> + Send + Sync,
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin,
{
    let (handshake, seq, client_capabilities, reader) = init_params;
    let reader = PacketReader::new(reader);
    let writer = PacketWriter::new(writer);

    let process_use_statement_on_query = opts.process_use_statement_on_query;
    let mut mi = AsyncMysqlIntermediary {
        client_capabilities,
        process_use_statement_on_query,
        shim,
        reader,
        writer,
    };
    mi.init_after_ssl(handshake, seq).await?;
    mi.run().await
}

pub async fn secure_run_with_options<B, R, W>(
    shim: B,
    writer: W,
    opts: IntermediaryOptions,
    tls_config: Arc<ServerConfig>,
    init_params: (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
) -> Result<(), B::Error>
where
    B: AsyncMysqlShim<WriteHalf<TlsStream<Duplex<PacketReader<R>, W>>>> + Send + Sync,
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin,
{
    let (handshake, seq, client_capabilities, reader) = init_params;
    let (reader, writer) = switch_to_tls(tls_config, reader, writer).await?;
    let reader = PacketReader::new(reader);
    let writer = PacketWriter::new(writer);

    let process_use_statement_on_query = opts.process_use_statement_on_query;
    let mut mi = AsyncMysqlIntermediary {
        client_capabilities,
        process_use_statement_on_query,
        shim,
        reader,
        writer,
    };
    mi.init_after_ssl(handshake, seq).await?;
    mi.run().await
}

pub async fn switch_to_tls<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    config: Arc<ServerConfig>,
    reader: R,
    writer: W,
) -> std::io::Result<(
    ReadHalf<TlsStream<Duplex<R, W>>>,
    WriteHalf<TlsStream<Duplex<R, W>>>,
)> {
    let stream = Duplex::new(reader, writer);
    let acceptor = TlsAcceptor::from(config);
    let stream = acceptor.accept(stream).await?;
    let (r, w) = tokio::io::split(stream);
    Ok((r, w))
}

pin_project! {
    #[derive(Clone, Debug)]
    pub struct Duplex<R, W> {
        #[pin]
        reader: R,
        #[pin]
        writer: W,
    }
}

impl<R, W> Duplex<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }
}

impl<R: AsyncRead, W> AsyncRead for Duplex<R, W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        AsyncRead::poll_read(self.project().reader, cx, buf)
    }
}

impl<R, W: AsyncWrite> AsyncWrite for Duplex<R, W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(self.project().writer, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(self.project().writer, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        AsyncWrite::poll_shutdown(self.project().writer, cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        AsyncWrite::poll_write_vectored(self.project().writer, cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        AsyncWrite::is_write_vectored(&self.writer)
    }
}
