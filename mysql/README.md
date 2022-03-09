# OpenSrv - MySQL

Bindings for emulating a MySQL/MariaDB server.

When developing new databases or caching layers, it can be immensely useful to test your system
using existing applications. However, this often requires significant work modifying
applications to use your database over the existing ones. This crate solves that problem by
acting as a MySQL server, and delegating operations such as querying and query execution to
user-defined logic.

To start, implement `MysqlShim/AsyncMysqlShim` for your backend, and create a `MysqlIntermediary/AsyncMysqlIntermediary` over an
instance of your backend and a connection stream. The appropriate methods will be called on
your backend whenever a client issues a `QUERY`, `PREPARE`, or `EXECUTE` command, and you will
have a chance to respond appropriately. For example, to write a shim that always responds to
all commands with a "no results" reply:

```rust
use std::io;
use std::net;
use std::thread;

use opensrv_mysql::*;

struct Backend;

impl<W: io::Write> MysqlShim<W> for Backend {
    type Error = io::Error;

    fn on_prepare(&mut self, _: &str, info: StatementMetaWriter<W>) -> io::Result<()> {
        info.reply(42, &[], &[])
    }
    fn on_execute(
        &mut self,
        _: u32,
        _: opensrv_mysql::ParamParser,
        results: QueryResultWriter<W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default())
    }
    fn on_close(&mut self, _: u32) {}

    fn on_query(&mut self, sql: &str, results: QueryResultWriter<W>) -> io::Result<()> {
        println!("execute sql {:?}", sql);
        results.start(&[])?.finish()
    }
}

fn main() {
    let mut threads = Vec::new();
    let listener = net::TcpListener::bind("127.0.0.1:3306").unwrap();

    while let Ok((s, _)) = listener.accept() {
        threads.push(thread::spawn(move || {
            MysqlIntermediary::run_on_tcp(Backend, s).unwrap();
        }));
    }

    for t in threads {
        t.join().unwrap();
    }
}
```
