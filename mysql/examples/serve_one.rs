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

//! After running this, you should be able to run:
//!
//! ```console
//! $ echo "SELECT * FROM foo" | mysql -h 127.0.0.1 --table
//! $
//! ```

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
