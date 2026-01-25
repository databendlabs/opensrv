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

use std::error::Error;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::FutureExt;
use mysql_async::prelude::*;
use mysql_async::Opts;
use mysql_common as myc;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ErrorKind, InitWriter, OkResponse, ParamParser,
    QueryResultWriter, StatementMetaWriter, U24_MAX,
};
use tokio::io::BufWriter;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpListener;

struct TestingShim<Q, P, E> {
    columns: Vec<Column>,
    params: Vec<Column>,
    on_q: Q,
    on_p: P,
    on_e: E,
}

#[async_trait]
impl<Q, P, E> AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for TestingShim<Q, P, E>
where
    for<'s> Q: 'static
        + Send
        + Sync
        + FnMut(
            &'s str,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
    P: 'static + Send + Sync + FnMut(&str) -> u32,
    for<'s> E: 'static
        + Send
        + Sync
        + FnMut(
            u32,
            Vec<opensrv_mysql::ParamValue<'s>>,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
{
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        let id = (self.on_p)(query);
        info.reply(id, &self.params, &self.columns).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        (self.on_e)(id, params.into_iter().collect(), results).await
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        if query.eq_ignore_ascii_case("SELECT @@socket")
            || query.eq_ignore_ascii_case("SELECT @@wait_timeout")
            || query.eq_ignore_ascii_case("SELECT @@max_allowed_packet")
        {
            results.completed(OkResponse::default()).await
        } else if query.eq_ignore_ascii_case("SELECT @@max_allowed_packet,@@wait_timeout,@@socket")
        {
            let cols = [
                Column {
                    table: String::new(),
                    column: "@@max_allowed_packet".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@wait_timeout".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@socket".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut row_writer = results.start(&cols).await?;
            row_writer.write_col(67108864u32)?;
            row_writer.write_col(28800u32)?;
            row_writer.write_col(None::<String>)?;
            row_writer.end_row().await?;
            row_writer.finish().await
        } else {
            (self.on_q)(query, results).await
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        _schema: &'a str,
        writer: InitWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        writer.ok().await
    }
}

impl<Q, P, E> TestingShim<Q, P, E>
where
    for<'s> Q: 'static
        + Send
        + Sync
        + FnMut(
            &'s str,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
    P: 'static + Send + Sync + FnMut(&str) -> u32,
    for<'s> E: 'static
        + Send
        + Sync
        + FnMut(
            u32,
            Vec<opensrv_mysql::ParamValue<'s>>,
            QueryResultWriter<'s, BufWriter<OwnedWriteHalf>>,
        )
            -> Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send + 's>>,
{
    fn new(on_q: Q, on_p: P, on_e: E) -> Self {
        TestingShim {
            columns: Vec::new(),
            params: Vec::new(),
            on_q,
            on_p,
            on_e,
        }
    }

    fn with_params(mut self, p: Vec<Column>) -> Self {
        self.params = p;
        self
    }

    fn with_columns(mut self, c: Vec<Column>) -> Self {
        self.columns = c;
        self
    }

    async fn test<C, F>(self, c: C)
    where
        F: Future<Output = Result<(), Box<dyn Error>>> + 'static + Send,
        C: FnOnce(mysql_async::Conn) -> F + Send + Sync + 'static,
    {
        self.test_with_opts(
            |port| Opts::from_url(&format!("mysql://127.0.0.1:{port}")).unwrap(),
            c,
        )
        .await;
    }

    async fn test_with_opts<C, F, O>(self, opts: O, c: C)
    where
        F: Future<Output = Result<(), Box<dyn Error>>> + 'static + Send,
        C: FnOnce(mysql_async::Conn) -> F + Send + Sync + 'static,
        O: FnOnce(u16) -> Opts + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let opts = opts(port);

        let listen = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();

            let (r, w) = socket.into_split();
            let w = BufWriter::with_capacity(100 * 1024, w);
            AsyncMysqlIntermediary::run_on(self, r, w).await.unwrap();
        });

        let conn = mysql_async::Conn::new(opts).await.unwrap();
        c(conn).await.unwrap();

        let (r1,) = tokio::join!(listen);

        r1.unwrap();
    }
}

#[tokio::test]
async fn it_connects() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|_| async { Ok(()) })
    .await;
}

#[tokio::test]
async fn it_pings() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        db.ping().await.map(|_| ())?;
        Ok(())
    })
    .await;
}

struct InitCountingShim {
    on_init_called: Arc<AtomicBool>,
}

#[async_trait]
impl AsyncMysqlShim<BufWriter<OwnedWriteHalf>> for InitCountingShim {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _query: &'a str,
        _info: StatementMetaWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "prepare not supported in test shim",
        ))
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        _results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "execute not supported in test shim",
        ))
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        if query.eq_ignore_ascii_case("SELECT @@socket")
            || query.eq_ignore_ascii_case("SELECT @@wait_timeout")
            || query.eq_ignore_ascii_case("SELECT @@max_allowed_packet")
        {
            results.completed(OkResponse::default()).await
        } else if query.eq_ignore_ascii_case("SELECT @@max_allowed_packet,@@wait_timeout,@@socket")
        {
            let columns = [
                Column {
                    table: String::new(),
                    column: "@@max_allowed_packet".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@wait_timeout".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                    colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                },
                Column {
                    table: String::new(),
                    column: "@@socket".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut row_writer = results.start(&columns).await?;
            row_writer.write_col(67108864u32)?;
            row_writer.write_col(28800u32)?;
            row_writer.write_col(None::<String>)?;
            row_writer.end_row().await?;
            row_writer.finish().await
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unexpected query: {query}"),
            ))
        }
    }

    async fn on_init<'a>(
        &'a mut self,
        _schema: &'a str,
        writer: InitWriter<'a, BufWriter<OwnedWriteHalf>>,
    ) -> Result<(), Self::Error> {
        self.on_init_called.store(true, Ordering::SeqCst);
        writer.ok().await
    }
}

#[tokio::test]
async fn handshake_with_initial_database_relies_on_backend_ack() {
    let on_init_called = Arc::new(AtomicBool::new(false));
    let shim = InitCountingShim {
        on_init_called: on_init_called.clone(),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();

        let (r, w) = socket.into_split();
        let w = BufWriter::with_capacity(100 * 1024, w);
        AsyncMysqlIntermediary::run_on(shim, r, w).await.unwrap();
    });

    let opts = Opts::from_url(&format!("mysql://127.0.0.1:{port}/initial_db")).unwrap();
    let mut conn = mysql_async::Conn::new(opts).await.unwrap();
    conn.ping().await.unwrap();
    conn.disconnect().await.unwrap();

    server.await.unwrap();

    assert!(
        on_init_called.load(Ordering::SeqCst),
        "backend on_init was not invoked"
    );
}

#[tokio::test]
async fn empty_response() {
    TestingShim::new(
        |_, w| w.completed(OkResponse::default()).boxed(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_rows() {
    TestingShim::new(
        move |_, w| {
            async move {
                let cols = [Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                w.start(&cols[..]).await?.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_columns() {
    TestingShim::new(
        move |_, w| async { w.start(&[]).await?.finish().await }.boxed(),
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn no_columns_but_rows() {
    TestingShim::new(
        move |_, w| {
            async {
                let mut row_writer = w.start(&[]).await?;
                row_writer.write_col(42)?;
                row_writer.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn really_long_query() {
    let long = "CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
    TestingShim::new(
        move |q, w| {
            async move {
                assert_eq!(q, long);
                let mut row_writer = w.start(&[]).await?;
                row_writer.write_col(42).map(|_| ())?;
                row_writer.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(move |mut db| async move {
        db.query_drop(long).await?;
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn error_response() {
    let err = (ErrorKind::ER_NO, "clearly not".to_string());
    let err_clone = (ErrorKind::ER_NO, "clearly not".to_string());
    TestingShim::new(
        move |_, w| {
            let message = err_clone.1.clone();
            let kind = err_clone.0;
            async move { w.error(kind, message.as_bytes()).await }.boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(move |mut db| async move {
        let res: Result<Vec<mysql_async::Row>, _> = db.query("SELECT a, b FROM foo").await;
        match res {
            Ok(_) => panic!(),
            Err(mysql_async::Error::Server(mysql_async::ServerError {
                code,
                message: ref msg,
                ref state,
            })) => {
                assert_eq!(
                    state,
                    &String::from_utf8(err.0.sqlstate().to_vec()).unwrap()
                );
                assert_eq!(code, err.0 as u16);
                assert_eq!(msg, &err.1);
            }
            Err(e) => {
                eprintln!("unexpected {e:?}");
                panic!();
            }
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
// TODO rename this case, row_writer must be used!
async fn empty_on_drop() {
    TestingShim::new(
        move |_, w| {
            async move {
                let cols = [Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let row_writer = w.start(&cols[..]).await?;
                row_writer.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_queries_nulls() {
    TestingShim::new(
        |_, w| {
            async move {
                let cols = &[Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let mut w = w.start(cols).await?;
                w.write_col(None::<i16>)?;
                w.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0][0], mysql_async::Value::NULL);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_queries() {
    TestingShim::new(
        |_, w| {
            async move {
                let cols = &[Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let mut w = w.start(cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_queries_many_rows() {
    TestingShim::new(
        |_, w| {
            async move {
                let cols = &[
                    Column {
                        table: String::new(),
                        column: "a".to_owned(),
                        collen: 0,
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::empty(),
                    },
                    Column {
                        table: String::new(),
                        column: "b".to_owned(),
                        collen: 0,
                        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                        colflags: myc::constants::ColumnFlags::empty(),
                    },
                ];
                let mut w = w.start(cols).await?;
                w.write_col(1024i16)?;
                w.write_col(1025i16)?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let rs: Vec<mysql_async::Row> = db.query("SELECT a, b FROM foo").await?;
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].len(), 2);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[0].get::<i16, _>(1), Some(1025));
        assert_eq!(rs[1].len(), 2);
        assert_eq!(rs[1].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[1].get::<i16, _>(1), Some(1025));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_prepares() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            let cols3 = cols.clone();
            async move {
                assert_eq!(stmt, 41);
                assert_eq!(params.len(), 1);
                // rust-mysql sends all numbers as LONGLONG
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
                );
                assert_eq!(Into::<i8>::into(params[0].value), 42i8);

                let mut w = w.start(&cols3).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a FROM b WHERE c = ?").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, (42i16,)).await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));

        Ok(())
    })
    .await;
}

#[tokio::test]
async fn insert_exec() {
    let params = vec![
        Column {
            table: String::new(),
            column: "username".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "email".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "pw".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "created".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_DATETIME,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "session".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "rss".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "mail".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 1,
        move |_, params, w| {
            async move {
                assert_eq!(params.len(), 7);
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[1].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[2].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[3].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_DATETIME
                );
                assert_eq!(
                    params[4].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[5].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(
                    params[6].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(Into::<&str>::into(params[0].value), "user199");
                assert_eq!(Into::<&str>::into(params[1].value), "user199@example.com");
                assert_eq!(
                    Into::<&str>::into(params[2].value),
                    "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka"
                );
                assert_eq!(
                    Into::<chrono::NaiveDateTime>::into(params[3].value),
                    chrono::NaiveDate::from_ymd_opt(2018, 4, 6)
                        .unwrap()
                        .and_hms_opt(13, 0, 56)
                        .unwrap()
                );
                assert_eq!(Into::<&str>::into(params[4].value), "token199");
                assert_eq!(Into::<&str>::into(params[5].value), "rsstoken199");
                assert_eq!(Into::<&str>::into(params[6].value), "mtok199");

                let info = OkResponse {
                    affected_rows: 42,
                    last_insert_id: 1,
                    ..Default::default()
                };
                w.completed(info).await
            }
            .boxed()
        },
    )
    .with_params(params)
    .test(|mut db| async move {
        let prep = db
            .prep(
                "INSERT INTO `users` \
        (`username`, `email`, `password_digest`, `created_at`, \
        `session_token`, `rss_token`, `mailing_list_token`) \
        VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .await?;

        let _res: Vec<mysql_async::Row> = db
            .exec(
                prep,
                (
                    "user199",
                    "user199@example.com",
                    "$2a$10$Tq3wrGeC0xtgzuxqOlc3v.07VTUvxvwI70kuoVihoO2cE5qj7ooka",
                    mysql_async::Value::Date(2018, 4, 6, 13, 0, 56, 0),
                    "token199",
                    "rsstoken199",
                    "mtok199",
                ),
            )
            .await?;

        assert_eq!(db.affected_rows(), 42);
        assert_eq!(db.last_insert_id(), Some(1));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn send_long() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a FROM b WHERE c = ?");
            41
        },
        move |stmt, params, w| {
            let cols = cols.clone();
            async move {
                assert_eq!(stmt, 41);
                assert_eq!(params.len(), 1);
                // rust-mysql sends all strings as VAR_STRING
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
                );
                assert_eq!(Into::<&[u8]>::into(params[0].value), b"Hello world");

                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a FROM b WHERE c = ?").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, (b"Hello world",)).await?;

        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn it_prepares_many() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();

    TestingShim::new(
        |_, _| unreachable!(),
        |q| {
            assert_eq!(q, "SELECT a, b FROM x");
            41
        },
        move |stmt, params, w| {
            let cols = cols.clone();
            async move {
                assert_eq!(stmt, 41);
                assert_eq!(params.len(), 0);

                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.write_col(1025i16)?;
                w.end_row().await?;
                w.write_row(&[1024i16, 1025i16]).await?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(Vec::new())
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM x").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].len(), 2);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[0].get::<i16, _>(1), Some(1025));
        assert_eq!(rs[1].len(), 2);
        assert_eq!(rs[1].get::<i16, _>(0), Some(1024));
        assert_eq!(rs[1].get::<i16, _>(1), Some(1025));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_empty() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            async move {
                assert!(!params.is_empty());
                w.completed(OkResponse::default()).await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a FROM b WHERE c = ?").await.unwrap();
        let rs: Vec<mysql_async::Row> = db.exec(prep, (42i16,)).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_params() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            let cols = cols.clone();
            async move {
                assert!(params.is_empty());
                let mut w = w.start(&cols).await?;
                w.write_col(1024i16)?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("foo").await.unwrap();
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 1);
        assert_eq!(rs[0].get::<i16, _>(0), Some(1024));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_nulls() {
    let cols = vec![
        Column {
            table: String::new(),
            column: "a".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            let cols = cols.clone();
            async move {
                assert_eq!(params.len(), 2);
                assert!(params[0].value.is_null());
                assert!(!params[1].value.is_null());
                assert_eq!(
                    params[0].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_NULL
                );
                // rust-mysql sends all numbers as LONGLONG :'(
                assert_eq!(
                    params[1].coltype,
                    myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
                );
                assert_eq!(Into::<i8>::into(params[1].value), 42i8);

                let mut w = w.start(&cols).await?;
                w.write_row(vec![None::<i16>, Some(42)]).await?;
                w.finish().await
            }
            .boxed()
        },
    )
    .with_params(params)
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM x WHERE c = ? AND d = ?").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, (mysql_async::Value::NULL, 42)).await?;
        assert_eq!(rs.len(), 1);
        assert_eq!(rs[0].len(), 2);
        assert_eq!(rs[0].get::<Option<i16>, _>(0), Some(None));
        assert_eq!(rs[0].get::<i16, _>(1), Some(42));
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_rows() {
    let cols = vec![Column {
        table: String::new(),
        column: "a".to_owned(),
        collen: 0,
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            let cols = cols.clone();
            async move { w.start(&cols[..]).await?.finish().await }.boxed()
        },
    )
    .with_columns(cols2)
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_cols_but_rows() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| {
            async move {
                let mut row_writer = w.start(&[]).await?;
                row_writer.write_col(42)?;
                row_writer.finish().await
            }
            .boxed()
        },
    )
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn prepared_no_cols() {
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| async move { w.start(&[]).await?.finish().await }.boxed(),
    )
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn large_packet() {
    TestingShim::new(
        move |_, w| {
            async move {
                let cols = vec![Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    collen: 0,
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_BLOB,
                    colflags: myc::constants::ColumnFlags::empty(),
                }];
                let mut row_writer = w.start(&cols).await?;
                let blob_col = vec![0; U24_MAX + 1];
                row_writer.write_row(vec![blob_col.clone()]).await?;
                row_writer.write_row(vec![blob_col]).await?;
                let row_writer = row_writer.finish_one().await?;
                row_writer.no_more_results().await
            }
            .boxed()
        },
        |_| 0,
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let mut result = db.query_iter("SELECT a, b from foo").await?;

        // check row numbers and packet size
        let mut number_rows = 0;
        while let Some(mut row) = result.next().await? {
            number_rows += 1;
            let value: Vec<u8> = row.take(0).unwrap();
            assert_eq!(U24_MAX + 1, value.len());
        }
        assert_eq!(2, number_rows);

        result.drop_result().await?;
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn ok_packet_with_info_when_session_track_disabled() {
    let info = "Query finished in 0.007 sec.".repeat(12);
    TestingShim::new(
        move |query, w| {
            let info = info.clone();
            async move {
                match query.trim() {
                    "SELECT @@max_allowed_packet,@@wait_timeout,@@socket" => {
                        let cols = &[
                            Column {
                                table: String::new(),
                                column: "@@max_allowed_packet".to_owned(),
                                collen: 0,
                                coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                                colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                            },
                            Column {
                                table: String::new(),
                                column: "@@wait_timeout".to_owned(),
                                collen: 0,
                                coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
                                colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
                            },
                            Column {
                                table: String::new(),
                                column: "@@socket".to_owned(),
                                collen: 0,
                                coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                                colflags: myc::constants::ColumnFlags::empty(),
                            },
                        ];

                        let mut row_writer = w.start(cols).await?;
                        row_writer.write_col(67108864u32)?;
                        row_writer.write_col(28800u32)?;
                        row_writer.write_col(None::<String>)?;
                        row_writer.end_row().await?;
                        row_writer.finish().await
                    }
                    "SELECT @@version_comment" => {
                        let cols = &[Column {
                            table: String::new(),
                            column: "@@version_comment".to_owned(),
                            collen: 0,
                            coltype: myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
                            colflags: myc::constants::ColumnFlags::empty(),
                        }];

                        let mut row_writer = w.start(cols).await?;
                        row_writer
                            .write_row(vec!["Databend (test)".to_string()])
                            .await?;
                        let query_writer = row_writer.finish_one_with_info(&info).await?;
                        query_writer.no_more_results().await
                    }
                    other => panic!("unexpected query: {other}"),
                }
            }
            .boxed()
        },
        |_| unreachable!(),
        |_, _, _| unreachable!(),
    )
    .test(|mut db| async move {
        let version: Option<String> = db.query_first("SELECT @@version_comment").await?;
        assert_eq!(version.as_deref(), Some("Databend (test)"));
        Ok(())
    })
    .await;
}
