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

use std::io::{self, Write};

use crate::myc::constants::{CapabilityFlags, StatusFlags};
use crate::myc::io::WriteMysqlExt;
use crate::packet_writer::PacketWriter;
use crate::{Column, ColumnFlags, ColumnType, ErrorKind, OkResponse};
use byteorder::{LittleEndian, WriteBytesExt};
use tokio::io::AsyncWrite;

const BIN_GENERAL_CI: u16 = 0x3f;

fn column_charset(column: &Column) -> u16 {
    use crate::myc::constants::UTF8_GENERAL_CI;

    if column
        .colflags
        .intersects(ColumnFlags::BINARY_FLAG | ColumnFlags::BLOB_FLAG)
        || matches!(
            column.coltype,
            ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_GEOMETRY
        )
    {
        return BIN_GENERAL_CI;
    }

    if matches!(
        column.coltype,
        ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_JSON
    ) {
        UTF8_GENERAL_CI
    } else {
        BIN_GENERAL_CI
    }
}

pub(crate) async fn write_eof_packet<W: AsyncWrite + Unpin>(
    w: &mut PacketWriter<W>,
    s: StatusFlags,
) -> io::Result<()> {
    w.write_all(&[0xFE, 0x00, 0x00])?;
    w.write_u16::<LittleEndian>(s.bits())?;
    w.end_packet().await
}

pub(crate) async fn write_ok_packet<W: AsyncWrite + Unpin>(
    w: &mut PacketWriter<W>,
    client_capabilities: CapabilityFlags,
    ok_packet: OkResponse,
) -> io::Result<()> {
    w.write_u8(ok_packet.header)?; // OK packet type
    w.write_lenenc_int(ok_packet.affected_rows)?;
    w.write_lenenc_int(ok_packet.last_insert_id)?;
    if client_capabilities.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
        w.write_u16::<LittleEndian>(ok_packet.status_flags.bits())?;
        w.write_u16::<LittleEndian>(ok_packet.warnings)?;
    } else if client_capabilities.contains(CapabilityFlags::CLIENT_TRANSACTIONS) {
        w.write_u16::<LittleEndian>(ok_packet.status_flags.bits())?;
    }

    // Only session-tracking clients expect length-encoded info per protocol; otherwise emit raw text.
    let has_session_track = client_capabilities.contains(CapabilityFlags::CLIENT_SESSION_TRACK);
    let send_info = !ok_packet.info.is_empty() || has_session_track;
    if send_info {
        if has_session_track {
            w.write_lenenc_str(ok_packet.info.as_bytes())?;
        } else {
            w.write_all(ok_packet.info.as_bytes())?;
        }
    }

    // Session state info is optional and only sent if flag is set
    if has_session_track
        && ok_packet
            .status_flags
            .contains(StatusFlags::SERVER_SESSION_STATE_CHANGED)
    {
        w.write_lenenc_str(ok_packet.session_state_info.as_bytes())?;
    }
    w.end_packet().await
}

pub async fn write_err<W: AsyncWrite + Unpin>(
    err: ErrorKind,
    msg: &[u8],
    w: &mut PacketWriter<W>,
) -> io::Result<()> {
    w.write_u8(0xFF)?;
    w.write_u16::<LittleEndian>(err as u16)?;
    w.write_u8(b'#')?;
    w.write_all(err.sqlstate())?;
    w.write_all(msg)?;
    w.end_packet().await
}

pub(crate) async fn write_prepare_ok<'a, PI, CI, W>(
    id: u32,
    params: PI,
    columns: CI,
    w: &mut PacketWriter<W>,
    client_capabilities: CapabilityFlags,
) -> io::Result<()>
where
    PI: IntoIterator<Item = &'a Column>,
    CI: IntoIterator<Item = &'a Column>,
    <PI as IntoIterator>::IntoIter: ExactSizeIterator,
    <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    W: AsyncWrite + Unpin,
{
    let pi = params.into_iter();
    let ci = columns.into_iter();

    // first, write out COM_STMT_PREPARE_OK
    w.write_u8(0x00)?;
    w.write_u32::<LittleEndian>(id)?;
    w.write_u16::<LittleEndian>(ci.len() as u16)?;
    w.write_u16::<LittleEndian>(pi.len() as u16)?;
    w.write_u8(0x00)?;
    w.write_u16::<LittleEndian>(0)?; // number of warnings
    w.end_packet().await?;

    if pi.len() > 0 {
        write_column_definitions_41(pi, w, client_capabilities, false).await?;
    }
    if ci.len() > 0 {
        write_column_definitions_41(ci, w, client_capabilities, false).await?;
    }
    Ok(())
}

/// works for Protocol::ColumnDefinition41 is set
/// see: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
pub(crate) async fn write_column_definitions_41<'a, I, W>(
    i: I,
    w: &mut PacketWriter<W>,
    client_capabilities: CapabilityFlags,
    is_com_field_list: bool,
) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    W: AsyncWrite + Unpin,
{
    for c in i {
        w.write_lenenc_str(b"def")?;
        w.write_lenenc_str(b"")?;
        w.write_lenenc_str(c.table.as_bytes())?;
        w.write_lenenc_str(b"")?;
        w.write_lenenc_str(c.column.as_bytes())?;
        w.write_lenenc_str(b"")?;
        w.write_lenenc_int(0xC)?;
        w.write_u16::<LittleEndian>(column_charset(c))?;
        w.write_u32::<LittleEndian>(1024)?;
        w.write_u8(c.coltype as u8)?;
        w.write_u16::<LittleEndian>(c.colflags.bits())?;
        w.write_all(&[0x00])?; // decimals
        w.write_all(&[0x00, 0x00])?; // unused

        if is_com_field_list {
            w.write_all(&[0xfb])?;
        }
        w.end_packet().await?;
    }

    if !client_capabilities.contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        write_eof_packet(w, StatusFlags::empty()).await
    } else {
        Ok(())
    }
}

pub(crate) async fn column_definitions<'a, I, W>(
    i: I,
    w: &mut PacketWriter<W>,
    client_capabilities: CapabilityFlags,
) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
    W: AsyncWrite + Unpin,
{
    let i = i.into_iter();
    w.write_lenenc_int(i.len() as u64)?;
    w.end_packet().await?;
    write_column_definitions_41(i, w, client_capabilities, false).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Column, ColumnFlags, ColumnType};
    use mysql_common::constants::CapabilityFlags;
    use mysql_common::io::ParseBuf;
    use mysql_common::packets::{OkPacket, OkPacketDeserializer, ResultSetTerminator};
    use tokio::io::AsyncReadExt;

    fn parse_lenenc_int(buf: &[u8]) -> (u64, usize) {
        match buf[0] {
            v @ 0x00..=0xfa => (v as u64, 1),
            0xfc => {
                let len = u16::from_le_bytes([buf[1], buf[2]]);
                (len as u64, 3)
            }
            0xfd => {
                let len = (buf[1] as u32) | ((buf[2] as u32) << 8) | ((buf[3] as u32) << 16);
                (len as u64, 4)
            }
            0xfe => {
                let len = u64::from_le_bytes([
                    buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
                ]);
                (len, 9)
            }
            marker => panic!("unexpected length-encoded integer marker: {marker:#x}"),
        }
    }

    async fn extract_payload_with_header(
        info: &str,
        capabilities: CapabilityFlags,
        header: u8,
    ) -> Vec<u8> {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut writer = PacketWriter::new(server);

        let ok_packet = OkResponse {
            header,
            info: info.to_string(),
            ..Default::default()
        };

        write_ok_packet(&mut writer, capabilities, ok_packet)
            .await
            .expect("write_ok_packet succeeds");

        let mut header = [0u8; 4];
        client
            .read_exact(&mut header)
            .await
            .expect("payload header available");
        let payload_len =
            (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        let mut payload = vec![0u8; payload_len];
        client
            .read_exact(&mut payload)
            .await
            .expect("payload body available");
        payload
    }

    async fn extract_payload(info: &str, capabilities: CapabilityFlags) -> Vec<u8> {
        extract_payload_with_header(info, capabilities, 0x00).await
    }

    fn consume_ok_packet_prefix(payload: &[u8]) -> (usize, u16, u16) {
        let mut idx = 0;
        assert_eq!(payload[idx], 0x00);
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

        (idx, status, warnings)
    }

    #[test]
    fn column_charset_defaults_to_utf8() {
        use crate::myc::constants::UTF8_GENERAL_CI;

        let column = Column {
            table: "t".into(),
            column: "c".into(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        };

        assert_eq!(column_charset(&column), UTF8_GENERAL_CI);
    }

    #[test]
    fn column_charset_uses_binary_for_numeric_types() {
        let column = Column {
            table: "t".into(),
            column: "c".into(),
            coltype: ColumnType::MYSQL_TYPE_LONG,
            colflags: ColumnFlags::empty(),
        };

        assert_eq!(column_charset(&column), BIN_GENERAL_CI);
    }

    #[test]
    fn column_charset_uses_binary_when_flagged() {
        let column = Column {
            table: "t".into(),
            column: "c".into(),
            coltype: ColumnType::MYSQL_TYPE_STRING,
            colflags: ColumnFlags::BINARY_FLAG,
        };

        assert_eq!(column_charset(&column), BIN_GENERAL_CI);
    }

    #[test]
    fn column_charset_handles_blob_types() {
        let column = Column {
            table: "t".into(),
            column: "c".into(),
            coltype: ColumnType::MYSQL_TYPE_BLOB,
            colflags: ColumnFlags::empty(),
        };

        assert_eq!(column_charset(&column), BIN_GENERAL_CI);
    }

    #[test]
    fn column_charset_respects_blob_flag() {
        let column = Column {
            table: "t".into(),
            column: "c".into(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::BLOB_FLAG,
        };

        assert_eq!(column_charset(&column), BIN_GENERAL_CI);
    }

    #[tokio::test]
    async fn ok_packet_info_is_length_encoded_with_session_track() {
        let info = "Read 1 rows, 1.00 B in 0.007 sec.";
        let capabilities =
            CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SESSION_TRACK;
        let payload = extract_payload(info, capabilities).await;

        let (mut idx, status, warnings) = consume_ok_packet_prefix(&payload);
        assert_eq!(status, 0);
        assert_eq!(warnings, 0);

        let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
        assert_eq!(info_len as usize, info.len());
        assert_eq!(payload[idx], info.len() as u8);
        idx += consumed;

        let encoded = &payload[idx..idx + info.len()];
        assert_eq!(encoded, info.as_bytes());
    }

    #[tokio::test]
    async fn ok_packet_info_uses_extended_length_prefix_when_needed() {
        let info = "x".repeat(300);
        let capabilities =
            CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SESSION_TRACK;
        let payload = extract_payload(&info, capabilities).await;

        let (mut idx, status, warnings) = consume_ok_packet_prefix(&payload);
        assert_eq!(status, 0);
        assert_eq!(warnings, 0);

        let (info_len, consumed) = parse_lenenc_int(&payload[idx..]);
        assert_eq!(consumed, 3, "expected 0xFC marker with two-byte length");
        assert_eq!(payload[idx], 0xFC);
        assert_eq!(info_len as usize, info.len());
        idx += consumed;

        let encoded = &payload[idx..idx + info.len()];
        assert_eq!(encoded, info.as_bytes());
    }

    #[tokio::test]
    async fn ok_packet_skips_info_for_empty_string_without_session_track() {
        let payload = extract_payload("", CapabilityFlags::CLIENT_PROTOCOL_41).await;

        let (idx, status, warnings) = consume_ok_packet_prefix(&payload);
        assert_eq!(status, 0);
        assert_eq!(warnings, 0);
        assert_eq!(
            idx,
            payload.len(),
            "empty info without CLIENT_SESSION_TRACK should not emit length prefix"
        );
    }

    #[tokio::test]
    async fn ok_packet_info_is_plain_string_without_session_track() {
        let info = "Read 1 rows, 1.00 B in 0.007 sec.";
        let payload = extract_payload(info, CapabilityFlags::CLIENT_PROTOCOL_41).await;

        let (idx, status, warnings) = consume_ok_packet_prefix(&payload);
        assert_eq!(status, 0);
        assert_eq!(warnings, 0);

        let encoded = &payload[idx..];
        assert_eq!(encoded, info.as_bytes());
    }

    #[tokio::test]
    async fn ok_packet_with_deprecate_eof_parses_lenenc_info() {
        let info = "x".repeat(300);
        let capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_DEPRECATE_EOF
            | CapabilityFlags::CLIENT_SESSION_TRACK;
        let payload = extract_payload_with_header(&info, capabilities, 0xfe).await;

        let mut buf = ParseBuf(&payload);
        let ok_packet: OkPacket = buf
            .parse::<OkPacketDeserializer<ResultSetTerminator>>(capabilities)
            .expect("ResultSetTerminator packet to parse")
            .into_inner();
        assert_eq!(ok_packet.info_ref(), Some(info.as_bytes()));
        assert_eq!(ok_packet.status_flags(), StatusFlags::empty());
    }
}
