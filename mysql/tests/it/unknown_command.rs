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

use async_trait::async_trait;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, InitWriter, ParamParser, QueryResultWriter,
    StatementMetaWriter,
};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CLIENT_PROTOCOL_41: u32 = 1 << 9;
const CLIENT_SECURE_CONNECTION: u32 = 1 << 15;
const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;

struct Shim;

#[async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Shim {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(1, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(Default::default()).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32)
    where
        W: 'async_trait,
    {
    }

    async fn on_query<'a>(
        &'a mut self,
        _query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(Default::default()).await
    }

    async fn on_init<'a>(
        &'a mut self,
        _schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        writer.ok().await
    }
}

async fn start() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, write) = stream.into_split();
        let _ = AsyncMysqlIntermediary::run_on(Shim, read, write).await;
    });
    port
}

async fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await.unwrap();
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await.unwrap();
    (header[3], payload)
}

async fn write_packet(stream: &mut TcpStream, seq: u8, payload: &[u8]) {
    let mut header = (payload.len() as u32).to_le_bytes();
    header[3] = seq;
    stream.write_all(&header).await.unwrap();
    stream.write_all(payload).await.unwrap();
}

fn handshake_response() -> Vec<u8> {
    let caps = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    let auth = [0u8; 20];
    let mut body = Vec::new();
    body.extend_from_slice(&caps.to_le_bytes());
    body.extend_from_slice(&0x00FF_FFFFu32.to_le_bytes());
    body.push(33);
    body.extend_from_slice(&[0u8; 23]);
    body.extend_from_slice(b"u\0");
    body.push(auth.len() as u8);
    body.extend_from_slice(&auth);
    body.extend_from_slice(b"mysql_native_password\0");
    body
}

async fn login(port: u16) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let _greeting = read_packet(&mut stream).await;
    write_packet(&mut stream, 1, &handshake_response()).await;
    let (seq, payload) = read_packet(&mut stream).await;
    assert_eq!(seq, 2);
    assert_eq!(
        payload[0], 0x00,
        "expected OK after handshake, got {payload:?}"
    );
    stream
}

fn errno(payload: &[u8]) -> (u16, String) {
    assert_eq!(payload[0], 0xff, "expected ERR, got {payload:?}");
    let code = u16::from_le_bytes([payload[1], payload[2]]);
    assert_eq!(payload[3], b'#');
    (code, String::from_utf8(payload[4..9].to_vec()).unwrap())
}

#[tokio::test]
async fn unrecognized_commands_return_er_unknown_com_error() {
    let port = start().await;
    let mut stream = login(port).await;
    // COM_PROCESS_KILL, COM_CHANGE_USER, COM_RESET_CONNECTION, then an unknown byte.
    for command in [
        vec![0x0c, 1, 0, 0, 0],
        vec![0x11, b'r', 0],
        vec![0x1f],
        vec![0xee],
    ] {
        write_packet(&mut stream, 0, &command).await;
        let (seq, payload) = read_packet(&mut stream).await;
        assert_eq!(seq, 1);
        assert_eq!(errno(&payload), (1047, "08S01".to_owned()), "{command:?}");
        assert!(payload[9..].starts_with(b"Unknown command"), "{payload:?}");
    }
    // The connection stays usable after an unrecognized command.
    write_packet(&mut stream, 0, &[0x0e]).await;
    let (_, payload) = read_packet(&mut stream).await;
    assert_eq!(payload[0], 0x00, "COM_PING should still be OK");
}
