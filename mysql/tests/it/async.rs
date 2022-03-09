use std::error::Error;
use std::future::Future;
use std::io;
use std::io::Cursor;

use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::Opts;
use mysql_common as myc;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ErrorKind, OkResponse, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};
use tokio::net::TcpListener;

struct TestingShim<Q, P, E> {
    columns: Vec<Column>,
    params: Vec<Column>,
    on_q: Q,
    on_p: P,
    on_e: E,
}

#[async_trait]
impl<Q, P, E> AsyncMysqlShim<Cursor<Vec<u8>>> for TestingShim<Q, P, E>
where
    Q: 'static + Send + Sync + FnMut(&str, QueryResultWriter<Cursor<Vec<u8>>>) -> io::Result<()>,
    P: 'static + Send + Sync + FnMut(&str) -> u32,
    E: 'static
        + Send
        + Sync
        + FnMut(
            u32,
            Vec<opensrv_mysql::ParamValue>,
            QueryResultWriter<Cursor<Vec<u8>>>,
        ) -> io::Result<()>,
{
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, Cursor<Vec<u8>>>,
    ) -> Result<(), Self::Error> {
        let id = (self.on_p)(query);
        info.reply(id, &self.params, &self.columns)
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, Cursor<Vec<u8>>>,
    ) -> Result<(), Self::Error> {
        (self.on_e)(id, params.into_iter().collect(), results)
    }

    async fn on_close<'a>(&'a mut self, _stmt: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, Cursor<Vec<u8>>>,
    ) -> Result<(), Self::Error> {
        if query.eq_ignore_ascii_case("SELECT @@socket")
            || query.eq_ignore_ascii_case("SELECT @@wait_timeout")
        {
            results.completed(OkResponse::default())
        } else {
            (self.on_q)(query, results)
        }
    }
}

impl<Q, P, E> TestingShim<Q, P, E>
where
    Q: 'static + Send + Sync + FnMut(&str, QueryResultWriter<Cursor<Vec<u8>>>) -> io::Result<()>,
    P: 'static + Send + Sync + FnMut(&str) -> u32,
    E: 'static
        + Send
        + Sync
        + FnMut(
            u32,
            Vec<opensrv_mysql::ParamValue>,
            QueryResultWriter<Cursor<Vec<u8>>>,
        ) -> io::Result<()>,
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let listen = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();

            AsyncMysqlIntermediary::run_on(self, socket).await.unwrap();
        });

        let conn =
            mysql_async::Conn::new(Opts::from_url(&format!("mysql://127.0.0.1:{}", port)).unwrap())
                .await
                .unwrap();
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

#[tokio::test]
async fn empty_response() {
    TestingShim::new(
        |_, w| w.completed(OkResponse::default()),
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
    let cols = [Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    TestingShim::new(
        move |_, w| w.start(&cols[..])?.finish(),
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
        move |_, w| w.start(&[])?.finish(),
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
        move |_, w| w.start(&[])?.write_col(42).map(|_| ()),
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
            assert_eq!(q, long);
            w.start(&[])?.write_col(42).map(|_| ())
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
    let err_to_move = err.clone();
    TestingShim::new(
        move |_, w| w.error(err_to_move.0, err_to_move.1.as_bytes()),
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
                eprintln!("unexpected {:?}", e);
                panic!();
            }
        }
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn empty_on_drop() {
    let cols = [Column {
        table: String::new(),
        column: "a".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    TestingShim::new(
        move |_, w| w.start(&cols[..]).map(|_| ()),
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
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut w = w.start(cols)?;
            w.write_col(None::<i16>)?;
            w.finish()
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
            let cols = &[Column {
                table: String::new(),
                column: "a".to_owned(),
                coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                colflags: myc::constants::ColumnFlags::empty(),
            }];
            let mut w = w.start(cols)?;
            w.write_col(1024i16)?;
            w.finish()
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
            let cols = &[
                Column {
                    table: String::new(),
                    column: "a".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
                Column {
                    table: String::new(),
                    column: "b".to_owned(),
                    coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
                    colflags: myc::constants::ColumnFlags::empty(),
                },
            ];
            let mut w = w.start(cols)?;
            w.write_col(1024i16)?;
            w.write_col(1025i16)?;
            w.end_row()?;
            w.write_row(&[1024i16, 1025i16])?;
            w.finish()
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
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
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
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 1);
            // rust-mysql sends all numbers as LONGLONG
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_LONGLONG
            );
            assert_eq!(Into::<i8>::into(params[0].value), 42i8);

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
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
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "email".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "pw".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "created".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_DATETIME,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "session".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "rss".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "mail".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_VARCHAR,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 1,
        move |_, params, w| {
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
                chrono::NaiveDate::from_ymd(2018, 4, 6).and_hms(13, 0, 56)
            );
            assert_eq!(Into::<&str>::into(params[4].value), "token199");
            assert_eq!(Into::<&str>::into(params[5].value), "rsstoken199");
            assert_eq!(Into::<&str>::into(params[6].value), "mtok199");

            let info = OkResponse {
                affected_rows: 42,
                last_insert_id: 1,
                ..Default::default()
            };
            w.completed(info)
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
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
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
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 1);
            // rust-mysql sends all strings as VAR_STRING
            assert_eq!(
                params[0].coltype,
                myc::constants::ColumnType::MYSQL_TYPE_VAR_STRING
            );
            assert_eq!(Into::<&[u8]>::into(params[0].value), b"Hello world");

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
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
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
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
            assert_eq!(stmt, 41);
            assert_eq!(params.len(), 0);

            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.write_col(1025i16)?;
            w.end_row()?;
            w.write_row(&[1024i16, 1025i16])?;
            w.finish()
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
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![Column {
        table: String::new(),
        column: "c".to_owned(),
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert!(!params.is_empty());
            w.completed(OkResponse::default())
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
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    let params = vec![];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
            assert!(params.is_empty());
            let mut w = w.start(&cols)?;
            w.write_col(1024i16)?;
            w.finish()
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
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "b".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];
    let cols2 = cols.clone();
    let params = vec![
        Column {
            table: String::new(),
            column: "c".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
        Column {
            table: String::new(),
            column: "d".to_owned(),
            coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
            colflags: myc::constants::ColumnFlags::empty(),
        },
    ];

    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, params, w| {
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

            let mut w = w.start(&cols)?;
            w.write_row(vec![None::<i16>, Some(42)])?;
            w.finish()
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
        coltype: myc::constants::ColumnType::MYSQL_TYPE_SHORT,
        colflags: myc::constants::ColumnFlags::empty(),
    }];
    let cols2 = cols.clone();
    TestingShim::new(
        |_, _| unreachable!(),
        |_| 0,
        move |_, _, w| w.start(&cols[..])?.finish(),
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
        move |_, _, w| w.start(&[])?.write_col(42).map(|_| ()),
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
        move |_, _, w| w.start(&[])?.finish(),
    )
    .test(|mut db| async move {
        let prep = db.prep("SELECT a, b FROM foo").await?;
        let rs: Vec<mysql_async::Row> = db.exec(prep, ()).await?;
        assert_eq!(rs.len(), 0);
        Ok(())
    })
    .await;
}
