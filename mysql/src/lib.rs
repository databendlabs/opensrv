//! Bindings for emulating a MySQL/MariaDB server.
//!
//! When developing new databases or caching layers, it can be immensely useful to test your system
//! using existing applications. However, this often requires significant work modifying
//! applications to use your database over the existing ones. This crate solves that problem by
//! acting as a MySQL server, and delegating operations such as querying and query execution to
//! user-defined logic.
//!
//! To start, implement `MysqlShim` for your backend, and create a `MysqlIntermediary` over an
//! instance of your backend and a connection stream. The appropriate methods will be called on
//! your backend whenever a client issues a `QUERY`, `PREPARE`, or `EXECUTE` command, and you will
//! have a chance to respond appropriately. For example, to write a shim that always responds to
//! all commands with a "no results" reply:
//!
//! ```
//! # use std::io;
//! # use std::net;
//! # use std::thread;
//! use opensrv_mysql::*;
//! use mysql::prelude::*;
//! use mysql::Opts;
//!
//! struct Backend;
//! impl<W: io::Write> MysqlShim<W> for Backend {
//!     type Error = io::Error;
//!
//!     fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
//!         info.reply(42, &[], &[])
//!     }
//!     fn on_execute(
//!         &mut self,
//!         _: u32,
//!         _: ParamParser,
//!         results: QueryResultWriter<W>,
//!     ) -> io::Result<()> {
//!        results.completed(OkResponse::default())
//!     }
//!     fn on_close(&mut self, _: u32) {}
//!
//!     fn on_init(&mut self, _: &str, writer: InitWriter<W>) -> io::Result<()> { Ok(()) }
//!
//!     fn on_query(&mut self, _: &str, results: QueryResultWriter<W>) -> io::Result<()> {
//!         let cols = [
//!             Column {
//!                 table: "foo".to_string(),
//!                 column: "a".to_string(),
//!                 coltype: ColumnType::MYSQL_TYPE_LONGLONG,
//!                 colflags: ColumnFlags::empty(),
//!             },
//!             Column {
//!                 table: "foo".to_string(),
//!                 column: "b".to_string(),
//!                 coltype: ColumnType::MYSQL_TYPE_STRING,
//!                 colflags: ColumnFlags::empty(),
//!             },
//!         ];
//!
//!         let mut rw = results.start(&cols)?;
//!         rw.write_col(42)?;
//!         rw.write_col("b's value")?;
//!         rw.finish()
//!     }
//! }
//!
//! fn main() {
//!     let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
//!     let port = listener.local_addr().unwrap().port();
//!
//!     let jh = thread::spawn(move || {
//!         if let Ok((s, _)) = listener.accept() {
//!             MysqlIntermediary::run_on_tcp(Backend, s).unwrap();
//!         }
//!     });
//!
//!     let mut db = mysql::Conn::new(Opts::from_url(&format!("mysql://127.0.0.1:{}", port)).unwrap()).unwrap();
//!     assert_eq!(db.ping(), true);
//!     assert_eq!(db.query_iter("SELECT a, b FROM foo").unwrap().count(), 1);
//!     drop(db);
//!     jh.join().unwrap();
//! }
//! ```
#![deny(missing_docs)]
#![allow(clippy::from_over_into)]

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
use std::io::prelude::*;
use std::io::Cursor;
use std::iter;
use std::net;

use async_trait::async_trait;
use tokio::io::AsyncRead;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub use crate::myc::constants::{CapabilityFlags, ColumnFlags, ColumnType, StatusFlags};

mod commands;
mod errorcodes;
mod packet;
mod params;
mod resultset;
mod value;
mod writers;

#[cfg(test)]
mod tests;

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
pub use crate::value::{ToMysqlValue, Value, ValueInner};

const SCRAMBLE_SIZE: usize = 20;

/// Implementors of this trait can be used to drive a MySQL-compatible database backend.
pub trait MysqlShim<W: Write> {
    /// The error type produced by operations on this shim.
    ///
    /// Must implement `From<io::Error>` so that transport-level errors can be lifted.
    type Error: From<io::Error>;

    /// Server version
    fn version(&self) -> &str {
        // 5.1.10 because that's what Ruby's ActiveRecord requires
        "5.1.10-alpha-msql-proxy"
    }

    /// Connection id
    fn connect_id(&self) -> u32 {
        u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
    }

    /// get auth plugin
    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    /// get auth plugin
    fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        "mysql_native_password"
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
    fn authenticate(
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
    fn on_prepare(
        &mut self,
        query: &str,
        info: StatementMetaWriter<'_, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client executes a previously prepared statement.
    ///
    /// Any parameters included with the client's command is given in `params`.
    /// A response to the query should be given using the provided
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    fn on_execute(
        &mut self,
        id: u32,
        params: ParamParser<'_>,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), Self::Error>;

    /// Called when the client wishes to deallocate resources associated with a previously prepared
    /// statement.
    fn on_close(&mut self, stmt: u32);

    /// Called when the client issues a query for immediate execution.
    ///
    /// Results should be returned using the given
    /// [`QueryResultWriter`](struct.QueryResultWriter.html).
    fn on_query(
        &mut self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> Result<(), Self::Error>;

    /// Called when client switches database.
    fn on_init(&mut self, _: &str, _: InitWriter<'_, W>) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[async_trait]
/// Implementors of this async-trait can be used to drive a MySQL-compatible database backend.
pub trait AsyncMysqlShim<W: Write + Send> {
    /// The error type produced by operations on this shim.
    ///
    /// Must implement `From<io::Error>` so that transport-level errors can be lifted.
    type Error: From<io::Error>;

    /// Server version
    fn version(&self) -> &str {
        // 5.1.10 because that's what Ruby's ActiveRecord requires
        "5.1.10-alpha-msql-proxy"
    }

    /// Connection id
    fn connect_id(&self) -> u32 {
        u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
    }

    /// get auth plugin
    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    /// get auth plugin
    fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        "mysql_native_password"
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
    fn authenticate(
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

/// The options which passed to MysqlIntermediary struct
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntermediaryOptions {
    /// process use statement on the on_query handler
    pub process_use_statement_on_query: bool,
}

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`MysqlShim`](trait.MysqlShim.html).
pub struct MysqlIntermediary<B, R: Read, W: Write> {
    pub(crate) client_capabilities: CapabilityFlags,
    process_use_statement_on_query: bool,
    shim: B,
    reader: packet::PacketReader<R>,
    writer: packet::PacketWriter<W>,
}

#[derive(Default)]
struct StatementData {
    long_data: HashMap<u16, Vec<u8>>,
    bound_types: Vec<(myc::constants::ColumnType, bool)>,
    params: u16,
}

const AUTH_PLUGIN_DATA_PART_1_LENGTH: usize = 8;

impl<B: MysqlShim<net::TcpStream>> MysqlIntermediary<B, net::TcpStream, net::TcpStream> {
    /// Create a new server over a TCP stream and process client commands until the client
    /// disconnects or an error occurs. See also
    /// [`MysqlIntermediary::run_on`](struct.MysqlIntermediary.html#method.run_on).
    pub fn run_on_tcp(shim: B, stream: net::TcpStream) -> Result<(), B::Error> {
        let w = stream.try_clone()?;
        MysqlIntermediary::run_on(shim, stream, w)
    }

    /// Create a new server over a TCP stream and process client commands until the client
    /// disconnects or an error occurs. See also
    pub fn run_on_tcp_with_options(
        shim: B,
        stream: net::TcpStream,
        opts: &IntermediaryOptions,
    ) -> Result<(), B::Error> {
        let w = stream.try_clone()?;
        MysqlIntermediary::run_with_options(shim, stream, w, opts)
    }
}

impl<B: MysqlShim<S>, S: Read + Write + Clone> MysqlIntermediary<B, S, S> {
    /// Create a new server over a two-way stream and process client commands until the client
    /// disconnects or an error occurs. See also
    /// [`MysqlIntermediary::run_on`](struct.MysqlIntermediary.html#method.run_on).
    pub fn run_on_stream(shim: B, stream: S) -> Result<(), B::Error> {
        MysqlIntermediary::run_on(shim, stream.clone(), stream)
    }

    /// Create a new server over a two-way stream and process client commands until the client
    /// disconnects or an error ocurrs.
    pub fn run_on_stream_with_options(
        shim: B,
        stream: S,
        opts: &IntermediaryOptions,
    ) -> Result<(), B::Error> {
        MysqlIntermediary::run_with_options(shim, stream.clone(), stream, opts)
    }
}

impl<B: MysqlShim<W>, R: Read, W: Write> MysqlIntermediary<B, R, W> {
    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs.
    pub fn run_on(shim: B, reader: R, writer: W) -> Result<(), B::Error> {
        Self::run_with_options(shim, reader, writer, &Default::default())
    }

    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs, with config options.
    pub fn run_with_options(
        shim: B,
        reader: R,
        writer: W,
        opts: &IntermediaryOptions,
    ) -> Result<(), B::Error> {
        let r = packet::PacketReader::new(reader);
        let w = packet::PacketWriter::new(writer);
        let mut mi = MysqlIntermediary {
            client_capabilities: CapabilityFlags::from_bits_truncate(0),
            process_use_statement_on_query: opts.process_use_statement_on_query,
            shim,
            reader: r,
            writer: w,
        };
        mi.init()?;
        mi.run()
    }

    fn init(&mut self) -> Result<(), B::Error> {
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
        self.writer.write_all(&[10])?; // protocol 10

        self.writer.write_all(self.shim.version().as_bytes())?;
        self.writer.write_all(&[0x00])?;

        // connection_id (4 bytes)
        self.writer
            .write_all(&self.shim.connect_id().to_le_bytes())?;

        let server_capabilities = (
            CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_SECURE_CONNECTION
                | CapabilityFlags::CLIENT_PLUGIN_AUTH
                | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
                | CapabilityFlags::CLIENT_CONNECT_WITH_DB
                | CapabilityFlags::CLIENT_DEPRECATE_EOF
            // | CapabilityFlags::CLIENT_SSL
        )
            .bits();

        let server_capabilities = server_capabilities.to_le_bytes();
        let default_auth_plugin = self.shim.default_auth_plugin();
        let scramble = self.shim.salt();

        self.writer
            .write_all(&scramble[0..AUTH_PLUGIN_DATA_PART_1_LENGTH])?; // auth-plugin-data-part-1
        self.writer.write_all(&[0x00])?;

        self.writer.write_all(&server_capabilities[..2])?; // The lower 2 bytes of the Capabilities Flags, 0x42
                                                           // self.writer.write_all(&[0x00, 0x42])?;
        self.writer.write_all(&[0x21])?; // UTF8_GENERAL_CI
        self.writer.write_all(&[0x00, 0x00])?; // status_flags
        self.writer.write_all(&server_capabilities[2..4])?; // The upper 2 bytes of the Capabilities Flags

        if default_auth_plugin.is_empty() {
            // no plugins
            self.writer.write_all(&[0x00])?;
        } else {
            self.writer
                .write_all(&((scramble.len() + 1) as u8).to_le_bytes())?; // length of the combined auth_plugin_data(scramble), if auth_plugin_data_len is > 0
        }
        self.writer.write_all(&[0x00; 10][..])?; // 10 bytes filler

        // Part2 of the auth_plugin_data
        // $len=MAX(13, length of auth-plugin-data - 8)
        self.writer
            .write_all(&scramble[AUTH_PLUGIN_DATA_PART_1_LENGTH..])?; // 12 bytes
        self.writer.write_all(&[0x00])?;

        // Plugin name
        self.writer.write_all(default_auth_plugin.as_bytes())?;
        self.writer.write_all(&[0x00])?;
        self.writer.flush()?;

        {
            let (mut seq, handshake) = self.reader.next()?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "peer terminated connection",
                )
            })?;
            let handshake = commands::client_handshake(&handshake)
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
            let auth_plugin_expect = self.shim.auth_plugin_for_username(&handshake.username);

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

                self.writer.flush()?;
                {
                    let (rseq, auth_response_data) = self.reader.next()?.ok_or_else(|| {
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

            if !self.shim.authenticate(
                auth_plugin_expect,
                &handshake.username,
                &scramble,
                auth_response.as_slice(),
            ) {
                let err_msg = format!(
                    "Authenticate failed, user: {:?}, auth_plugin: {:?}",
                    String::from_utf8_lossy(&handshake.username),
                    auth_plugin_expect,
                );
                writers::write_err(
                    ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
                    err_msg.as_bytes(),
                    &mut self.writer,
                )?;
                self.writer.flush()?;
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, err_msg).into());
            }
        }

        writers::write_ok_packet(
            &mut self.writer,
            self.client_capabilities,
            OkResponse::default(),
        )?;
        self.writer.flush()?;

        Ok(())
    }

    fn run(mut self) -> Result<(), B::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        while let Some((seq, packet)) = self.reader.next()? {
            self.writer.set_seq(seq + 1);
            let cmd = commands::parse(&packet).unwrap().1;

            match cmd {
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
                            coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                            colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                        }];

                        match var {
                            b"max_allowed_packet" => {
                                let mut w = w.start(cols)?;
                                w.write_row(iter::once(67108864u32))?;
                                w.finish()?;
                            }
                            _ => {
                                let mut w = w.start(cols)?;
                                w.write_row(iter::once(0))?;
                                w.finish()?;
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
                        self.shim.on_init(schema, w)?;
                    } else {
                        let w = QueryResultWriter::new(
                            &mut self.writer,
                            false,
                            self.client_capabilities,
                        );
                        self.shim.on_query(
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )?;
                    }
                }
                Command::Prepare(q) => {
                    let w = StatementMetaWriter {
                        writer: &mut self.writer,
                        stmts: &mut stmts,
                        client_capabilities: self.client_capabilities,
                    };

                    self.shim.on_prepare(
                        ::std::str::from_utf8(q)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        w,
                    )?;
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
                        self.shim.on_execute(stmt, params, w)?;
                    }
                    state.long_data.clear();
                }
                Command::SendLongData { stmt, param, data } => {
                    stmts
                        .get_mut(&stmt)
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("got long data packet for unknown statement {}", stmt),
                            )
                        })?
                        .long_data
                        .entry(param)
                        .or_insert_with(Vec::new)
                        .extend(data);
                }
                Command::Close(stmt) => {
                    self.shim.on_close(stmt);
                    stmts.remove(&stmt);
                    // NOTE: spec dictates no response from server
                }
                Command::ListFields(_) => {
                    let cols = &[Column {
                        table: String::new(),
                        column: "not implemented".to_owned(),
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                    }];
                    writers::write_column_definitions_41(
                        cols,
                        &mut self.writer,
                        self.client_capabilities,
                        true,
                    )?;
                    let ok_packet = OkResponse {
                        header: 0xfe,
                        ..Default::default()
                    };
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        ok_packet,
                    )?;
                }
                Command::Init(schema) => {
                    let w = InitWriter {
                        client_capabilities: self.client_capabilities,
                        writer: &mut self.writer,
                    };
                    self.shim.on_init(
                        ::std::str::from_utf8(schema)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        w,
                    )?;
                }
                Command::Ping => {
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        OkResponse::default(),
                    )?;
                }
                Command::Quit => {
                    break;
                }
            }
            self.writer.flush()?;
        }
        Ok(())
    }
}

/// A server that speaks the MySQL/MariaDB protocol, and can delegate client commands to a backend
/// that implements [`MysqlShim`](trait.MysqlShim.html).
pub struct AsyncMysqlIntermediary<B, S: AsyncRead + AsyncWrite + Unpin> {
    pub(crate) client_capabilities: CapabilityFlags,
    process_use_statement_on_query: bool,
    shim: B,
    reader: packet::PacketReader<S>,
    writer: packet::PacketWriter<Cursor<Vec<u8>>>,
}

impl<B: AsyncMysqlShim<Cursor<Vec<u8>>> + Send, S: AsyncRead + AsyncWrite + Unpin>
    AsyncMysqlIntermediary<B, S>
{
    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs.
    pub async fn run_on(shim: B, stream: S) -> Result<(), B::Error> {
        Self::run_with_options(shim, stream, &Default::default()).await
    }

    /// Create a new server over two one-way channels and process client commands until the client
    /// disconnects or an error occurs, with config options.
    pub async fn run_with_options(
        shim: B,
        stream: S,
        opts: &IntermediaryOptions,
    ) -> Result<(), B::Error> {
        let r = packet::PacketReader::new(stream);
        let w = packet::PacketWriter::new(Cursor::new(Vec::new()));
        let mut mi = AsyncMysqlIntermediary {
            client_capabilities: CapabilityFlags::from_bits_truncate(0),
            process_use_statement_on_query: opts.process_use_statement_on_query,
            shim,
            reader: r,
            writer: w,
        };
        mi.init().await?;
        mi.run().await
    }

    async fn init(&mut self) -> Result<(), B::Error> {
        // https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeV10
        self.writer.write_all(&[10])?; // protocol 10

        self.writer.write_all(self.shim.version().as_bytes())?;
        self.writer.write_all(&[0x00])?;

        // connection_id (4 bytes)
        self.writer
            .write_all(&self.shim.connect_id().to_le_bytes())?;

        let server_capabilities = (
            CapabilityFlags::CLIENT_PROTOCOL_41
                | CapabilityFlags::CLIENT_SECURE_CONNECTION
                | CapabilityFlags::CLIENT_PLUGIN_AUTH
                | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
                | CapabilityFlags::CLIENT_CONNECT_WITH_DB
                | CapabilityFlags::CLIENT_DEPRECATE_EOF
            // | CapabilityFlags::CLIENT_SSL
        )
            .bits();

        let server_capabilities = server_capabilities.to_le_bytes();
        let default_auth_plugin = self.shim.default_auth_plugin();
        let scramble = self.shim.salt();

        self.writer
            .write_all(&scramble[0..AUTH_PLUGIN_DATA_PART_1_LENGTH])?; // auth-plugin-data-part-1
        self.writer.write_all(&[0x00])?;

        self.writer.write_all(&server_capabilities[..2])?; // The lower 2 bytes of the Capabilities Flags, 0x42
                                                           // self.writer.write_all(&[0x00, 0x42])?;
        self.writer.write_all(&[0x21])?; // UTF8_GENERAL_CI
        self.writer.write_all(&[0x00, 0x00])?; // status_flags
        self.writer.write_all(&server_capabilities[2..4])?; // The upper 2 bytes of the Capabilities Flags

        if default_auth_plugin.is_empty() {
            // no plugins
            self.writer.write_all(&[0x00])?;
        } else {
            self.writer
                .write_all(&((scramble.len() + 1) as u8).to_le_bytes())?; // length of the combined auth_plugin_data(scramble), if auth_plugin_data_len is > 0
        }
        self.writer.write_all(&[0x00; 10][..])?; // 10 bytes filler

        // Part2 of the auth_plugin_data
        // $len=MAX(13, length of auth-plugin-data - 8)
        self.writer
            .write_all(&scramble[AUTH_PLUGIN_DATA_PART_1_LENGTH..])?; // 12 bytes
        self.writer.write_all(&[0x00])?;

        // Plugin name
        self.writer.write_all(default_auth_plugin.as_bytes())?;
        self.writer.write_all(&[0x00])?;
        self.writer_flush().await?;

        {
            let (mut seq, handshake) = self.reader.next_async().await?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "peer terminated connection",
                )
            })?;
            let handshake = commands::client_handshake(&handshake)
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
            let auth_plugin_expect = self.shim.auth_plugin_for_username(&handshake.username);

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

                self.writer.flush()?;
                let buf = self.writer.w.get_mut();
                self.reader.r.write_all(buf.as_slice()).await?;
                self.reader.r.flush().await?;
                buf.truncate(0);
                self.writer.w.set_position(0);

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

            if !self.shim.authenticate(
                auth_plugin_expect,
                &handshake.username,
                &scramble,
                auth_response.as_slice(),
            ) {
                let err_msg = format!(
                    "Authenticate failed, user: {:?}, auth_plugin: {:?}",
                    String::from_utf8_lossy(&handshake.username),
                    auth_plugin_expect,
                );
                writers::write_err(
                    ErrorKind::ER_ACCESS_DENIED_NO_PASSWORD_ERROR,
                    err_msg.as_bytes(),
                    &mut self.writer,
                )?;
                self.writer_flush().await?;
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, err_msg).into());
            }
        }

        writers::write_ok_packet(
            &mut self.writer,
            self.client_capabilities,
            OkResponse::default(),
        )?;
        self.writer_flush().await?;

        Ok(())
    }

    async fn writer_flush(&mut self) -> Result<(), B::Error> {
        self.writer.flush()?;
        let buf = self.writer.w.get_mut();
        self.reader.r.write_all(buf.as_slice()).await?;
        self.reader.r.flush().await?;
        buf.truncate(0);
        self.writer.w.set_position(0);
        Ok(())
    }

    async fn run(mut self) -> Result<(), B::Error> {
        use crate::commands::Command;

        let mut stmts: HashMap<u32, _> = HashMap::new();
        while let Some((seq, packet)) = self.reader.next_async().await? {
            self.writer.set_seq(seq + 1);
            let cmd = commands::parse(&packet).unwrap().1;

            match cmd {
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
                            coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                            colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                        }];

                        match var {
                            b"max_allowed_packet" => {
                                let mut w = w.start(cols)?;
                                w.write_row(iter::once(67108864u32))?;
                                w.finish()?;
                            }
                            _ => {
                                let mut w = w.start(cols)?;
                                w.write_row(iter::once(0))?;
                                w.finish()?;
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
                                ::std::str::from_utf8(q)
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
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
                            ::std::str::from_utf8(q)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
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
                                format!("got long data packet for unknown statement {}", stmt),
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
                    let cols = &[Column {
                        table: String::new(),
                        column: "not implemented".to_owned(),
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                    }];
                    writers::write_column_definitions_41(
                        cols,
                        &mut self.writer,
                        self.client_capabilities,
                        true,
                    )?;
                    let ok_packet = OkResponse {
                        header: 0xfe,
                        ..Default::default()
                    };
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        ok_packet,
                    )?;
                }
                Command::Init(schema) => {
                    let w = InitWriter {
                        client_capabilities: self.client_capabilities,
                        writer: &mut self.writer,
                    };
                    self.shim
                        .on_init(
                            ::std::str::from_utf8(schema)
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                            w,
                        )
                        .await?;
                }
                Command::Ping => {
                    writers::write_ok_packet(
                        &mut self.writer,
                        self.client_capabilities,
                        OkResponse::default(),
                    )?;
                }
                Command::Quit => {
                    break;
                }
            }
            self.writer_flush().await?;
        }
        Ok(())
    }
}
