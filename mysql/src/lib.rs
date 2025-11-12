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

extern crate mysql_common as myc;

use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::iter;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
#[cfg(feature = "tls")]
use tokio_rustls::rustls::ServerConfig;

pub use crate::myc::constants::{CapabilityFlags, ColumnFlags, ColumnType, StatusFlags};
#[cfg(feature = "tls")]
pub use crate::tls::{plain_run_with_options, secure_run_with_options};

mod commands;
mod errorcodes;
mod packet_reader;
mod packet_writer;
mod params;
mod resultset;
#[cfg(feature = "tls")]
mod tls;
mod value;
mod writers;

#[cfg(test)]
mod tests;

// max payload size 2^(24-1)
pub const U24_MAX: usize = 16_777_215;

/// Meta-information abot a single column, used either to describe a prepared statement parameter
/// or an output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// This column's associated table.
    ///
    /// Note that this is *technically* the table's alias.
    pub table: String,
    /// This column's name.
    ///
    /// Note that this is *technically* the column's alias.
    pub column: String,
    /// Column length (in bytes) reported through COLUMN_DEFINITION41.
    /// 0 means "use default".
    pub collen: u32,
    /// This column's type>
    pub coltype: ColumnType,
    /// Any flags associated with this column.
    ///
    /// Of particular interest are `ColumnFlags::UNSIGNED_FLAG` and `ColumnFlags::NOT_NULL_FLAG`.
    pub colflags: ColumnFlags,
}

/// QueryStatusInfo represents the status of a query.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OkResponse {
    /// header
    pub header: u8,
    /// affected rows in update/insert
    pub affected_rows: u64,
    /// insert_id in update/insert
    pub last_insert_id: u64,
    /// StatusFlags associated with this query
    pub status_flags: StatusFlags,
    /// Warnings
    pub warnings: u16,
    /// Extra infomation
    pub info: String,
    /// session state change information
    pub session_state_info: String,
}

pub use crate::errorcodes::ErrorKind;
pub use crate::params::{ParamParser, ParamValue, Params};
pub use crate::resultset::{InitWriter, QueryResultWriter, RowWriter, StatementMetaWriter};
pub use crate::value::{decode::to_naive_datetime, ToMysqlValue, Value, ValueInner};
use crate::{commands::ClientHandshake, packet_reader::PacketReader, packet_writer::PacketWriter};

const SCRAMBLE_SIZE: usize = 20;
const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";

#[async_trait]
/// Implementors of this async-trait can be used to drive a MySQL-compatible database backend.
pub trait AsyncMysqlShim<W: Send> {
    /// The error type produced by operations on this shim.
    ///
    /// Must implement `From<io::Error>` so that transport-level errors can be lifted.
    type Error: From<io::Error>;

    /// Server version
    fn version(&self) -> String {
        // 5.1.10 because that's what Ruby's ActiveRecord requires
        "5.1.10-alpha-msql-proxy".to_string()
    }

    /// Connection id
    fn connect_id(&self) -> u32 {
        u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
    }

    /// get auth plugin
    fn default_auth_plugin(&self) -> &str {
        MYSQL_NATIVE_PASSWORD
    }

    /// get auth plugin
    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &'static str {
        MYSQL_NATIVE_PASSWORD
    }

    /// Default salt(scramble) for auth plugin
    fn salt(&self) -> [u8; SCRAMBLE_SIZE] {
        let bs = ";X,po_k}>o6^Wz!/kM}N".as_bytes();
        let mut scramble: [u8; SCRAMBLE_SIZE] = [0; SCRAMBLE_SIZE];
        for i in 0..SCRAMBLE_SIZE {
            scramble[i] = bs[i];
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }
        scramble
    }

    /// authenticate method for the specified plugin
    async fn authenticate(
        &self,
        _auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        true
    }

    /// Called when the client issues a request to prepare `query` for later execution.
    ///
    /// The provided [`StatementMetaWriter`](struct.StatementMetaWriter.html) should be used to
    /// notify the client of the statement id assigned to the prepared statement, as well as to
    /// give metadata about the types of parameters and returned columns.
    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`.
    /// A response to the query should be given using the provided
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    async fn on_close<'a>(&'a mut self, stmt: u32)
    where
        W: 'async_trait;

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error>;

    /// Called when client switches database.
    async fn on_init<'a>(
        &'a mut self,
        _: &'a str,
        _: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The options which passed to AsyncMysqlIntermediary struct
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntermediaryOptions {
    /// process use statement on the on_query handler
    pub process_use_statement_on_query: bool,
    /// reject connection if dbname not provided
    pub reject_connection_on_dbname_absence: bool,
}

#[derive(Default)]
struct StatementData {
    long_data: HashMap<u16, Vec<u8>>,
    bound_types: Vec<(myc::constants::ColumnType, bool)>,
    params: u16,
}

const AUTH_PLUGIN_DATA_PART_1_LENGTH: usize = 8;

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`AsyncMysqlShim`](trait.AsyncMysqlShim.html).
pub struct AsyncMysqlIntermediary<B, S: AsyncRead + Unpin, W> {
    pub(crate) client_capabilities: CapabilityFlags,
    process_use_statement_on_query: bool,
    reject_connection_on_dbname_absence: bool,
    shim: B,
    reader: packet_reader::PacketReader<S>,
    writer: packet_writer::PacketWriter<W>,
}

/// Configuration used to craft the initial server handshake before a shim is available.
#[derive(Clone, Debug)]
pub struct ServerHandshakeConfig {
    pub version: String,
    pub connection_id: u32,
    pub default_auth_plugin: String,
    pub scramble: [u8; SCRAMBLE_SIZE],
}

impl<B, R, W> AsyncMysqlIntermediary<B, R, W>
where
    W: AsyncWrite + Send + Unpin,
    B: AsyncMysqlShim<W> + Send + Sync,
    R: AsyncRead + Send + Unpin,
{
    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs.
    pub async fn run_on(shim: B, stream: R, output_stream: W) -> Result<(), B::Error> {
        Self::run_with_options(shim, stream, output_stream, &Default::default()).await
    }

    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs, with config options.
    pub async fn run_with_options(
        mut shim: B,
        input_stream: R,
        mut output_stream: W,
        opts: &IntermediaryOptions,
    ) -> Result<(), B::Error> {
        let process_use_statement_on_query = opts.process_use_statement_on_query;
        let reject_connection_on_dbname_absence = opts.reject_connection_on_dbname_absence;
        let (_, (handshake, seq, client_capabilities, input_stream)) =
            AsyncMysqlIntermediary::init_before_ssl(
                &mut shim,
                input_stream,
                &mut output_stream,
                #[cfg(feature = "tls")]
                &None,
            )
            .await?;

        let reader = PacketReader::new(input_stream);
        let writer = PacketWriter::new(output_stream);

        let mut mi = AsyncMysqlIntermediary {
            client_capabilities,
            process_use_statement_on_query,
            reject_connection_on_dbname_absence,
            shim,
            reader,
            writer,
        };
        mi.init_after_ssl(handshake, seq).await?;
        mi.run().await
    }

    pub async fn init_before_ssl(
        shim: &mut B,
        input_stream: R,
        output_stream: &mut W,
        #[cfg(feature = "tls")] tls_conf: &Option<std::sync::Arc<ServerConfig>>,
    ) -> Result<
        (
            bool,
            (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
        ),
        B::Error,
    > {
        let config = ServerHandshakeConfig {
            version: shim.version(),
            connection_id: shim.connect_id(),
            default_auth_plugin: shim.default_auth_plugin().to_string(),
            scramble: shim.salt(),
        };

        AsyncMysqlIntermediary::<B, R, W>::init_before_ssl_with_config(
            &config,
            input_stream,
            output_stream,
            #[cfg(feature = "tls")]
            tls_conf,
        )
        .await
        .map_err(B::Error::from)
    }

    pub async fn init_before_ssl_with_config(
        config: &ServerHandshakeConfig,
        input_stream: R,
        output_stream: &mut W,
        #[cfg(feature = "tls")] tls_conf: &Option<std::sync::Arc<ServerConfig>>,
    ) -> io::Result<
        (
            bool,
            (ClientHandshake, u8, CapabilityFlags, PacketReader<R>),
        ),
    > {
        let mut reader = PacketReader::new(input_stream);
        let mut writer = PacketWriter::new(output_stream);
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
        writer.write_all(&[10])?; // protocol 10

        writer.write_all(config.version.as_bytes())?;
        writer.write_all(&[0x00])?;

        // connection_id (4 bytes)
        writer.write_all(&config.connection_id.to_le_bytes())?;

        let server_capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | CapabilityFlags::CLIENT_CONNECT_WITH_DB
            | CapabilityFlags::CLIENT_SESSION_TRACK
            | CapabilityFlags::CLIENT_DEPRECATE_EOF;

        #[cfg(feature = "tls")]
        let server_capabilities = if tls_conf.is_some() {
            server_capabilities | CapabilityFlags::CLIENT_SSL
        } else {
            server_capabilities
        };

        let server_capabilities_vec = server_capabilities.bits().to_le_bytes();
        let default_auth_plugin = config.default_auth_plugin.as_str();
        let scramble = &config.scramble;

        writer.write_all(&scramble[0..AUTH_PLUGIN_DATA_PART_1_LENGTH])?; // auth-plugin-data-part-1
        writer.write_all(&[0x00])?;

        writer.write_all(&server_capabilities_vec[..2])?; // The lower 2 bytes of the Capabilities Flags, 0x42
                                                          // self.writer.write_all(&[0x00, 0x42])?;
        writer.write_all(&[0x21])?; // UTF8_GENERAL_CI
        writer.write_all(&[0x00, 0x00])?; // status_flags
        writer.write_all(&server_capabilities_vec[2..4])?; // The upper 2 bytes of the Capabilities Flags

        if default_auth_plugin.is_empty() {
            // no plugins
            writer.write_all(&[0x00])?;
        } else {
            writer.write_all(&((scramble.len() + 1) as u8).to_le_bytes())?; // length of the combined auth_plugin_data(scramble), if auth_plugin_data_len is > 0
        }
        writer.write_all(&[0x00; 10][..])?; // 10 bytes filler

        // Part2 of the auth_plugin_data
        // $len=MAX(13, length of auth-plugin-data - 8)
        writer.write_all(&scramble[AUTH_PLUGIN_DATA_PART_1_LENGTH..])?; // 12 bytes
        writer.write_all(&[0x00])?;

        // Plugin name
        writer.write_all(default_auth_plugin.as_bytes())?;
        writer.write_all(&[0x00])?;
        writer.end_packet().await?;
        writer.flush_all().await?;

        let (seq, handshake) = reader.next_async().await?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "peer terminated connection",
            )
        })?;

        let handshake = commands::client_handshake(&handshake, false)
            .map_err(|e| match e {
                nom::Err::Incomplete(_) => io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "client sent incomplete handshake",
                ),
                nom::Err::Failure(nom_error) | nom::Err::Error(nom_error) => {
                    if let nom::error::ErrorKind::Eof = nom_error.code {
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            format!(
                                "client did not complete handshake; got {:?}",
                                nom_error.input
                            ),
                        )
                    } else {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!(
                                "bad client handshake; got {:?} ({:?})",
                                nom_error.input, nom_error.code
                            ),
                        )
                    }
                }
            })?
            .1;

        writer.set_seq(seq + 1);

        #[cfg(not(feature = "tls"))]
        if handshake.capabilities.contains(CapabilityFlags::CLIENT_SSL) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "client requested SSL despite us not advertising support for it",
            )
            .into());
        }

        #[cfg(feature = "tls")]
        if handshake.capabilities.contains(CapabilityFlags::CLIENT_SSL) {
            return Ok((true, (handshake, seq, server_capabilities, reader)));
        }

        Ok((false, (handshake, seq, server_capabilities, reader)))
    }

    pub async fn init_after_ssl(
        &mut self,
        #[cfg(feature = "tls")] mut handshake: ClientHandshake,
        #[cfg(not(feature = "tls"))] handshake: ClientHandshake,
        mut seq: u8,
    ) -> Result<(), B::Error> {
        #[cfg(feature = "tls")]
        if handshake.capabilities.contains(CapabilityFlags::CLIENT_SSL) {
            let (_seq, hs) = self.reader.next_async().await?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "peer terminated connection",
                )
            })?;
            seq = _seq;

            handshake = commands::client_handshake(&hs, true)
                .map_err(|e| match e {
                    nom::Err::Incomplete(_) => io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "client sent incomplete handshake",
                    ),
                    nom::Err::Failure(nom_error) | nom::Err::Error(nom_error) => {
                        if let nom::error::ErrorKind::Eof = nom_error.code {
                            io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!(
                                    "client did not complete handshake; got {:?}",
                                    nom_error.input
                                ),
                            )
                        } else {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "bad client handshake; got {:?} ({:?})",
                                    nom_error.input, nom_error.code
                                ),
                            )
                        }
                    }
                })?
                .1;

            self.writer.set_seq(seq + 1);
        }

        let scramble = self.shim.salt();
        {
            if !handshake
                .capabilities
                .contains(CapabilityFlags::CLIENT_PROTOCOL_41)
            {
                let err = io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Required capability: CLIENT_PROTOCOL_41, please upgrade your MySQL client version",
                );
                return Err(err.into());
            }

            self.client_capabilities = handshake.capabilities;
            let mut auth_response = handshake.auth_response.clone();
            if let Some(username) = &handshake.username {
                let auth_plugin_expect = self.shim.auth_plugin_for_username(username).await;

                // auth switch
                if !auth_plugin_expect.is_empty()
                    && auth_response.is_empty()
                    && handshake.auth_plugin != auth_plugin_expect.as_bytes()
                {
                    self.writer.set_seq(seq + 1);
                    self.writer.write_all(&[0xfe])?;
                    self.writer.write_all(auth_plugin_expect.as_bytes())?;
                    self.writer.write_all(&[0x00])?;
                    self.writer.write_all(&scramble)?;
                    self.writer.write_all(&[0x00])?;

                    self.writer.end_packet().await?;
                    self.writer.flush_all().await?;

                    {
                        let (rseq, auth_response_data) =
                            self.reader.next_async().await?.ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::ConnectionAborted,
                                    "peer terminated connection",
                                )
                            })?;

                        seq = rseq;
                        auth_response = auth_response_data.to_vec();
                    }
                }

                self.writer.set_seq(seq + 1);

                if !self
                    .shim
                    .authenticate(
                        auth_plugin_expect,
                        username,
                        &scramble,
                        auth_response.as_slice(),
                    )
                    .await
                {
                    let err_msg = format!(
                        "Authenticate failed, user: {:?}, auth_plugin: {:?}",
                        String::from_utf8_lossy(username),
                        auth_plugin_expect,
                    );
                    writers::write_err(
                        ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
                        err_msg.as_bytes(),
                        &mut self.writer,
                    )
                    .await?;
                    self.writer.flush_all().await?;
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, err_msg).into());
                }

                let mut needs_default_ok = true;

                if let Some(db_bytes) = handshake.db.as_ref() {
                    if let Ok(db) = std::str::from_utf8(db_bytes) {
                        let w = InitWriter {
                            client_capabilities: self.client_capabilities,
                            writer: &mut self.writer,
                        };
                        self.shim.on_init(db, w).await?;
                        needs_default_ok = false;
                    }
                } else if self.reject_connection_on_dbname_absence {
                    writers::write_err(
                        ErrorKind::ER_DATABASE_NAME,
                        "database required on connection".as_bytes(),
                        &mut self.writer,
                    )
                    .await?;
                    self.writer.flush_all().await?;
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "database name requried: please add db name to the connection",
                    )
                    .into());
                }

                if needs_default_ok {
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        OkResponse::default(),
                    )
                    .await?;
                }
            }

            self.writer.flush_all().await?;
        };

        Ok(())
    }

    async fn run(mut self) -> Result<(), B::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        while let Some((seq, packet)) = self.reader.next_async().await? {
            self.writer.set_seq(seq + 1);
            let res = commands::parse(&packet);
            match res {
                Ok(cmd) => {
                    match cmd.1 {
                        Command::Query(q) => {
                            if q.starts_with(b"SELECT @@") || q.starts_with(b"select @@") {
                                let w = QueryResultWriter::new(
                                    &mut self.writer,
                                    false,
                                    self.client_capabilities,
                                );

                                let var = &q[b"SELECT @@".len()..];
                                let var_with_at = &q[b"SELECT ".len()..];
                                let cols = &[Column {
                                    table: String::new(),
                                    column: String::from_utf8_lossy(var_with_at).to_string(),
                                    collen: 0,
                                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                                }];

                                match var {
                                    b"max_allowed_packet" => {
                                        let mut w = w.start(cols).await?;
                                        w.write_row(iter::once(67108864u32)).await?;
                                        w.finish().await?;
                                    }
                                    _ => {
                                        self.shim
                                            .on_query(
                                                ::std::str::from_utf8(q).map_err(|e| {
                                                    io::Error::new(io::ErrorKind::InvalidData, e)
                                                })?,
                                                w,
                                            )
                                            .await?;
                                    }
                                }
                            } else if !self.process_use_statement_on_query
                                && (q.starts_with(b"USE ") || q.starts_with(b"use "))
                            {
                                let w = InitWriter {
                                    client_capabilities: self.client_capabilities,
                                    writer: &mut self.writer,
                                };
                                let schema = ::std::str::from_utf8(&q[b"USE ".len()..])
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                                let schema = schema.trim().trim_end_matches(';').trim_matches('`');
                                self.shim.on_init(schema, w).await?;
                            } else {
                                let w = QueryResultWriter::new(
                                    &mut self.writer,
                                    false,
                                    self.client_capabilities,
                                );
                                self.shim
                                    .on_query(
                                        ::std::str::from_utf8(q).map_err(|e| {
                                            io::Error::new(io::ErrorKind::InvalidData, e)
                                        })?,
                                        w,
                                    )
                                    .await?;
                            }
                        }
                        Command::Prepare(q) => {
                            let w = StatementMetaWriter {
                                writer: &mut self.writer,
                                stmts: &mut stmts,
                                client_capabilities: self.client_capabilities,
                            };

                            self.shim
                                .on_prepare(
                                    ::std::str::from_utf8(q).map_err(|e| {
                                        io::Error::new(io::ErrorKind::InvalidData, e)
                                    })?,
                                    w,
                                )
                                .await?;
                        }
                        Command::Execute { stmt, params } => {
                            let state = stmts.get_mut(&stmt).ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("asked to execute unknown statement {}", stmt),
                                )
                            })?;
                            {
                                let params = params::ParamParser::new(params, state);
                                let w = QueryResultWriter::new(
                                    &mut self.writer,
                                    true,
                                    self.client_capabilities,
                                );
                                self.shim.on_execute(stmt, params, w).await?;
                            }
                            state.long_data.clear();
                        }
                        Command::SendLongData { stmt, param, data } => {
                            stmts
                                .get_mut(&stmt)
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!(
                                            "got long data packet for unknown statement {}",
                                            stmt
                                        ),
                                    )
                                })?
                                .long_data
                                .entry(param)
                                .or_insert_with(Vec::new)
                                .extend(data);
                        }
                        Command::Close(stmt) => {
                            self.shim.on_close(stmt).await;
                            stmts.remove(&stmt);
                            // NOTE: spec dictates no response from server
                        }
                        Command::ListFields(_) => {
                            // mysql_list_fields (CommandByte::COM_FIELD_LIST / 0x04) has been deprecated in mysql 5.7
                            // and will be removed in a future version.
                            // The mysql command line tool issues one of these commands after switching databases with USE <DB>.
                            // Return a invalid column definitions lead to incorrect mariadb-client behaviour,
                            // see https://github.com/datafuselabs/databend/issues/4439
                            let ok_packet = OkResponse {
                                header: 0xfe,
                                ..Default::default()
                            };
                            writers::write_ok_packet(
                                &mut self.writer,
                                self.client_capabilities,
                                ok_packet,
                            )
                            .await?;
                        }
                        Command::Init(schema) => {
                            let w = InitWriter {
                                client_capabilities: self.client_capabilities,
                                writer: &mut self.writer,
                            };
                            self.shim
                                .on_init(
                                    ::std::str::from_utf8(schema).map_err(|e| {
                                        io::Error::new(io::ErrorKind::InvalidData, e)
                                    })?,
                                    w,
                                )
                                .await?;
                        }
                        Command::Ping => {
                            writers::write_ok_packet(
                                &mut self.writer,
                                self.client_capabilities,
                                OkResponse::default(),
                            )
                            .await?;
                        }
                        Command::Quit => {
                            break;
                        }
                    }
                    self.writer.flush_all().await?;
                }
                Err(_) => {
                    // if parser err, we need also stay the conn,
                    // because we can not support all command.
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        OkResponse::default(),
                    )
                    .await?;
                    self.writer.flush_all().await?;
                }
            }
        }
        Ok(())
    }
}
