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

use crate::myc;
use crate::value::Value;
use crate::{Column, ColumnFlags, ColumnType};
use chrono::{self, TimeZone};
use myc::proto::MySerialize;
use std::time;

macro_rules! rt {
    ($name:ident, $t:ty, $v:expr, $ct:expr) => {
        rt!($name, $t, $v, $ct, false);
    };
    ($name:ident, $t:ty, $v:expr, $ct:expr, $sig:expr) => {
        #[test]
        fn $name() {
            let mut data = Vec::new();
            let mut col = Column {
                table: String::new(),
                column: String::new(),
                collen: 0,
                coltype: $ct,
                colflags: ColumnFlags::empty(),
            };

            if !$sig {
                col.colflags.insert(ColumnFlags::UNSIGNED_FLAG);
            }

            let v: $t = $v;
            myc::value::Value::from(v).serialize(&mut data);
            assert_eq!(
                Into::<$t>::into(Value::parse_from(&mut &data[..], $ct, !$sig).unwrap()),
                v
            );
        }
    };
}

rt!(u8_one, u8, 1, ColumnType::MYSQL_TYPE_TINY, false);
rt!(i8_one, i8, 1, ColumnType::MYSQL_TYPE_TINY, true);
rt!(u8_one_short, u8, 1, ColumnType::MYSQL_TYPE_SHORT, false);
rt!(i8_one_short, i8, 1, ColumnType::MYSQL_TYPE_SHORT, true);
rt!(u8_one_long, u8, 1, ColumnType::MYSQL_TYPE_LONG, false);
rt!(i8_one_long, i8, 1, ColumnType::MYSQL_TYPE_LONG, true);
rt!(
    u8_one_longlong,
    u8,
    1,
    ColumnType::MYSQL_TYPE_LONGLONG,
    false
);
rt!(
    i8_one_longlong,
    i8,
    1,
    ColumnType::MYSQL_TYPE_LONGLONG,
    true
);
rt!(u16_one, u16, 1, ColumnType::MYSQL_TYPE_SHORT, false);
rt!(i16_one, i16, 1, ColumnType::MYSQL_TYPE_SHORT, true);
rt!(u16_one_long, u16, 1, ColumnType::MYSQL_TYPE_LONG, false);
rt!(i16_one_long, i16, 1, ColumnType::MYSQL_TYPE_LONG, true);
rt!(
    u16_one_longlong,
    u16,
    1,
    ColumnType::MYSQL_TYPE_LONGLONG,
    false
);
rt!(
    i16_one_longlong,
    i16,
    1,
    ColumnType::MYSQL_TYPE_LONGLONG,
    true
);
rt!(u32_one_long, u32, 1, ColumnType::MYSQL_TYPE_LONG, false);
rt!(i32_one_long, i32, 1, ColumnType::MYSQL_TYPE_LONG, true);
rt!(
    u32_one_longlong,
    u32,
    1,
    ColumnType::MYSQL_TYPE_LONGLONG,
    false
);
rt!(
    i32_one_longlong,
    i32,
    1,
    ColumnType::MYSQL_TYPE_LONGLONG,
    true
);
rt!(u64_one, u64, 1, ColumnType::MYSQL_TYPE_LONGLONG, false);
rt!(i64_one, i64, 1, ColumnType::MYSQL_TYPE_LONGLONG, true);

rt!(f32_one_float, f32, 1.0, ColumnType::MYSQL_TYPE_FLOAT, false);
rt!(f64_one, f64, 1.0, ColumnType::MYSQL_TYPE_DOUBLE, false);

rt!(u8_max, u8, u8::MAX, ColumnType::MYSQL_TYPE_TINY, false);
rt!(i8_max, i8, i8::MAX, ColumnType::MYSQL_TYPE_TINY, true);
rt!(u16_max, u16, u16::MAX, ColumnType::MYSQL_TYPE_SHORT, false);
rt!(i16_max, i16, i16::MAX, ColumnType::MYSQL_TYPE_SHORT, true);
rt!(u32_max, u32, u32::MAX, ColumnType::MYSQL_TYPE_LONG, false);
rt!(i32_max, i32, i32::MAX, ColumnType::MYSQL_TYPE_LONG, true);
rt!(
    u64_max,
    u64,
    u64::MAX,
    ColumnType::MYSQL_TYPE_LONGLONG,
    false
);
rt!(
    i64_max,
    i64,
    i64::MAX,
    ColumnType::MYSQL_TYPE_LONGLONG,
    true
);

rt!(
    time,
    chrono::NaiveDate,
    chrono::Local::now().date_naive(),
    ColumnType::MYSQL_TYPE_DATE
);
rt!(
    datetime,
    chrono::NaiveDateTime,
    chrono::Utc
        .with_ymd_and_hms(1989, 12, 7, 8, 0, 4)
        .unwrap()
        .naive_utc(),
    ColumnType::MYSQL_TYPE_DATETIME
);
rt!(
    dur,
    time::Duration,
    time::Duration::from_secs(1893),
    ColumnType::MYSQL_TYPE_TIME
);
rt!(
    dur_zero,
    time::Duration,
    time::Duration::from_secs(0),
    ColumnType::MYSQL_TYPE_TIME
);
rt!(
    bytes,
    &[u8],
    &[0x42, 0x00, 0x1a],
    ColumnType::MYSQL_TYPE_BLOB
);
rt!(string, &str, "foobar", ColumnType::MYSQL_TYPE_STRING);
