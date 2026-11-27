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

use std::io::Cursor;

use crate::commands::*;
use crate::myc::constants::{CapabilityFlags, UTF8_GENERAL_CI};
use crate::packet_reader::PacketReader;

#[test]
fn it_parses_handshake() {
    let data = &[
        0x5b, 0x00, 0x00, 0x01, 0x8d, 0xa6, 0xff, 0x09, 0x00, 0x00, 0x00, 0x01, 0x21, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x00, 0x14,
        0xf7, 0xd1, 0x6c, 0xe9, 0x0d, 0x2f, 0x34, 0xb0, 0x2f, 0xd8, 0x1d, 0x18, 0xc7, 0xa4, 0xe8,
        0x98, 0x97, 0x67, 0xeb, 0xad, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6c, 0x74, 0x00, 0x6d, 0x79,
        0x73, 0x71, 0x6c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x70, 0x61, 0x73, 0x73,
        0x77, 0x6f, 0x72, 0x64, 0x00,
    ];

    let r = Cursor::new(&data[..]);
    let mut pr = PacketReader::new(r);
    let (_, p) = pr.next().unwrap().unwrap();
    let (_, handshake) = client_handshake(&p, false).unwrap();
    println!("{handshake:?}");
    assert!(handshake
        .capabilities
        .contains(CapabilityFlags::CLIENT_LONG_PASSWORD));
    assert!(handshake
        .capabilities
        .contains(CapabilityFlags::CLIENT_MULTI_RESULTS));
    assert!(handshake
        .capabilities
        .contains(CapabilityFlags::CLIENT_CONNECT_WITH_DB));
    assert!(handshake
        .capabilities
        .contains(CapabilityFlags::CLIENT_DEPRECATE_EOF));
    assert_eq!(handshake.collation, UTF8_GENERAL_CI);
    assert_eq!(handshake.username, Some(b"default"[..].to_vec()));
    assert_eq!(handshake.maxps, 16777216);
}

#[test]
fn it_parses_request() {
    let data = &[
        0x21, 0x00, 0x00, 0x00, 0x03, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76,
        0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20,
        0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
    ];
    let r = Cursor::new(&data[..]);
    let mut pr = PacketReader::new(r);
    let (_, p) = pr.next().unwrap().unwrap();
    let (_, cmd) = parse(&p).unwrap();
    assert_eq!(
        cmd,
        Command::Query(&b"select @@version_comment limit 1"[..])
    );
}

#[test]
fn it_handles_list_fields() {
    // mysql_list_fields (CommandByte::COM_FIELD_LIST / 0x04) has been deprecated in mysql 5.7 and will be removed
    // in a future version. The mysql command line tool issues one of these commands after
    // switching databases with USE <DB>.
    let data = &[
        0x21, 0x00, 0x00, 0x00, 0x04, 0x73, 0x65, 0x6c, 0x65, 0x63, 0x74, 0x20, 0x40, 0x40, 0x76,
        0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x65, 0x6e, 0x74, 0x20,
        0x6c, 0x69, 0x6d, 0x69, 0x74, 0x20, 0x31,
    ];
    let r = Cursor::new(&data[..]);
    let mut pr = PacketReader::new(r);
    let (_, p) = pr.next().unwrap().unwrap();
    let (_, cmd) = parse(&p).unwrap();
    assert_eq!(
        cmd,
        Command::ListFields(&b"select @@version_comment limit 1"[..])
    );
}

fn handshake41(
    caps: CapabilityFlags,
    user: &str,
    auth: &[u8],
    db: Option<&str>,
    plugin: Option<&str>,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&caps.bits().to_le_bytes());
    body.extend_from_slice(&0x00FF_FFFFu32.to_le_bytes());
    body.push(33);
    body.extend_from_slice(&[0u8; 23]);
    body.extend_from_slice(user.as_bytes());
    body.push(0);
    if caps.contains(CapabilityFlags::CLIENT_SECURE_CONNECTION) {
        body.push(auth.len() as u8);
        body.extend_from_slice(auth);
    } else {
        body.extend_from_slice(auth);
        body.push(0);
    }
    if let Some(db) = db {
        body.extend_from_slice(db.as_bytes());
        body.push(0);
    }
    if let Some(plugin) = plugin {
        body.extend_from_slice(plugin.as_bytes());
        body.push(0);
    }
    body
}

#[test]
fn nul_terminated_native_response_keeps_the_plugin_name() {
    let caps = CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_PLUGIN_AUTH;
    let packet = handshake41(caps, "u", &[0x42; 20], None, Some("mysql_native_password"));
    let (rest, hs) = client_handshake(&packet, false).unwrap();
    assert!(rest.is_empty());
    assert_eq!(hs.auth_response, vec![0x42; 20]);
    assert_eq!(hs.auth_plugin, b"mysql_native_password");
    assert!(hs.db.is_none());
}

#[test]
fn nul_terminated_response_with_database_then_plugin() {
    let caps = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB
        | CapabilityFlags::CLIENT_PLUGIN_AUTH;
    let packet = handshake41(
        caps,
        "u",
        &[0x11; 20],
        Some("appdb"),
        Some("caching_sha2_password"),
    );
    let (rest, hs) = client_handshake(&packet, false).unwrap();
    assert!(rest.is_empty());
    assert_eq!(hs.auth_response, vec![0x11; 20]);
    assert_eq!(hs.db.as_deref(), Some(&b"appdb"[..]));
    assert_eq!(hs.auth_plugin, b"caching_sha2_password");
}
