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

// Note to developers: you can find decent overviews of the protocol at
//
//   https://github.com/cwarden/mysql-proxy/blob/master/doc/protocol.rst
//
// and
//
//   https://mariadb.com/kb/en/library/clientserver-protocol/
//
// Wireshark also does a pretty good job at parsing the MySQL protocol.

use tokio::io::{duplex, AsyncReadExt};

use crate::packet_writer::PacketWriter;
use crate::writers::write_ok_packet;
use crate::{CapabilityFlags, OkResponse};

async fn capture_ok_payload(info: &str, capabilities: CapabilityFlags, header: u8) -> Vec<u8> {
    let (mut client, server) = duplex(1024);
    let mut writer = PacketWriter::new(server);

    let ok_packet = OkResponse {
        header,
        info: info.to_string(),
        ..Default::default()
    };

    write_ok_packet(&mut writer, capabilities, ok_packet)
        .await
        .expect("write_ok_packet succeeds");

    let mut header_buf = [0u8; 4];
    client
        .read_exact(&mut header_buf)
        .await
        .expect("payload header available");
    let payload_len = (header_buf[0] as usize)
        | ((header_buf[1] as usize) << 8)
        | ((header_buf[2] as usize) << 16);
    let mut payload = vec![0u8; payload_len];
    client
        .read_exact(&mut payload)
        .await
        .expect("payload body available");
    payload
}

fn parse_lenenc_int(data: &[u8]) -> (u64, usize) {
    match data[0] {
        0xFC => {
            let len = u16::from_le_bytes([data[1], data[2]]) as u64;
            (len, 3)
        }
        0xFD => {
            let len = (data[1] as u64) | ((data[2] as u64) << 8) | ((data[3] as u64) << 16);
            (len, 4)
        }
        0xFE => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&data[1..9]);
            (u64::from_le_bytes(buf), 9)
        }
        v => (v as u64, 1),
    }
}

fn consume_ok_prefix(payload: &[u8]) -> (usize, u8, u16, u16) {
    let mut idx = 0;
    let header = payload[idx];
    idx += 1;

    let (affected_rows, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(affected_rows, 0);
    idx += consumed;

    let (last_insert_id, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(last_insert_id, 0);
    idx += consumed;

    let status = u16::from_le_bytes([payload[idx], payload[idx + 1]]);
    idx += 2;

    let warnings = u16::from_le_bytes([payload[idx], payload[idx + 1]]);
    idx += 2;

    (idx, header, status, warnings)
}

#[tokio::test]
async fn ok_packet_info_lenenc_when_session_track() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(
        info,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SESSION_TRACK,
        0x00,
    )
    .await;

    let (mut idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(info_len as usize, info.len());
    idx += consumed;

    let encoded = &payload[idx..idx + info.len()];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_lenenc_when_deprecate_eof() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(
        info,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_DEPRECATE_EOF,
        0x00,
    )
    .await;

    let (mut idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(info_len as usize, info.len());
    idx += consumed;

    let encoded = &payload[idx..idx + info.len()];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_lenenc_when_header_fe() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(info, CapabilityFlags::CLIENT_PROTOCOL_41, 0xfe).await;

    let (mut idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0xfe);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(info_len as usize, info.len());
    idx += consumed;

    let encoded = &payload[idx..idx + info.len()];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_plain_when_no_flags() {
    let info = "Read 1 rows, 1.00 B in 0.007 sec.";
    let payload = capture_ok_payload(info, CapabilityFlags::CLIENT_PROTOCOL_41, 0x00).await;

    let (idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let encoded = &payload[idx..];
    assert_eq!(encoded, info.as_bytes());
}

#[tokio::test]
async fn ok_packet_info_extended_lenenc_with_flags() {
    let info = "x".repeat(300);
    let payload = capture_ok_payload(
        &info,
        CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SESSION_TRACK,
        0x00,
    )
    .await;

    let (mut idx, header, status, warnings) = consume_ok_prefix(&payload);
    assert_eq!(header, 0x00);
    assert_eq!(status, 0);
    assert_eq!(warnings, 0);

    let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
    assert_eq!(consumed, 3); // expect 0xFC marker with two-byte length
    assert_eq!(payload[idx], 0xFC);
    assert_eq!(info_len as usize, info.len());
    idx += consumed;

    let encoded = &payload[idx..idx + info.len()];
    assert_eq!(encoded, info.as_bytes());
}
