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

use byteorder::{LittleEndian, WriteBytesExt};

use crate::myc::constants::{CapabilityFlags, StatusFlags};
use crate::myc::io::WriteMysqlExt;
//use crate::packet::PacketWriter;
use crate::packet_writer::PacketWriter;
use crate::{Column, ErrorKind, OkResponse};

const BIN_GENERAL_CI: u16 = 0x3f;

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
        w.write_all(&[0x00, 0x00])?; // no warnings
    } else if client_capabilities.contains(CapabilityFlags::CLIENT_TRANSACTIONS) {
        w.write_u16::<LittleEndian>(ok_packet.status_flags.bits())?;
    }

    if client_capabilities.contains(CapabilityFlags::CLIENT_SESSION_TRACK) {
        w.write_lenenc_str(ok_packet.info.as_bytes())?;
        if ok_packet
            .status_flags
            .contains(StatusFlags::SERVER_SESSION_STATE_CHANGED)
        {
            w.write_lenenc_str(ok_packet.session_state_info.as_bytes())?;
        }
    } else {
        w.write_all(ok_packet.info.as_bytes())?;
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

use tokio::io::AsyncWrite;

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
        w.write_u16::<LittleEndian>(BIN_GENERAL_CI)?;
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
