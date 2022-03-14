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

use crate::value::ToMysqlValue;
use crate::{Column, ColumnFlags, ColumnType};
use chrono::{self, TimeZone};
use std::time;

mod roundtrip_text {
    use super::*;

    use myc::{
        io::ParseBuf,
        proto::MyDeserialize,
        value::{convert::FromValue, TextValue, ValueDeserializer},
    };

    macro_rules! rt {
        ($name:ident, $t:ty, $v:expr) => {
            #[test]
            fn $name() {
                let mut data = Vec::new();
                let v: $t = $v;
                v.to_mysql_text(&mut data).unwrap();
                let mut pb = ParseBuf(&data[..]);
                assert_eq!(
                    <$t>::from_value(
                        ValueDeserializer::<TextValue>::deserialize((), &mut pb)
                            .unwrap()
                            .0,
                    ),
                    v
                );
            }
        };
    }

    rt!(u8_one, u8, 1);
    rt!(i8_one, i8, 1);
    rt!(u16_one, u16, 1);
    rt!(i16_one, i16, 1);
    rt!(u32_one, u32, 1);
    rt!(i32_one, i32, 1);
    rt!(u64_one, u64, 1);
    rt!(i64_one, i64, 1);
    rt!(f32_one, f32, 1.0);
    rt!(f64_one, f64, 1.0);

    rt!(u8_max, u8, u8::max_value());
    rt!(i8_max, i8, i8::max_value());
    rt!(u16_max, u16, u16::max_value());
    rt!(i16_max, i16, i16::max_value());
    rt!(u32_max, u32, u32::max_value());
    rt!(i32_max, i32, i32::max_value());
    rt!(u64_max, u64, u64::max_value());
    rt!(i64_max, i64, i64::max_value());

    rt!(opt_none, Option<u8>, None);
    rt!(opt_some, Option<u8>, Some(1));

    rt!(
        time,
        chrono::NaiveDate,
        chrono::Local::today().naive_local()
    );
    rt!(
        datetime,
        chrono::NaiveDateTime,
        chrono::Utc.ymd(1989, 12, 7).and_hms(8, 0, 4).naive_utc()
    );
    rt!(dur, time::Duration, time::Duration::from_secs(1893));
    rt!(dur_micro, time::Duration, time::Duration::new(1893, 5000));
    rt!(dur_zero, time::Duration, time::Duration::from_secs(0));
    rt!(bytes, Vec<u8>, vec![0x42, 0x00, 0x1a]);
    rt!(string, String, "foobar".to_owned());
}

mod roundtrip_bin {
    use super::*;

    use myc::{
        io::ParseBuf,
        proto::MyDeserialize,
        value::{convert::FromValue, BinValue, ValueDeserializer},
    };

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
                    coltype: $ct,
                    colflags: ColumnFlags::empty(),
                };

                if !$sig {
                    col.colflags.insert(ColumnFlags::UNSIGNED_FLAG);
                }

                let v: $t = $v;
                v.to_mysql_bin(&mut data, &col).unwrap();
                let mut pb = ParseBuf(&data[..]);
                assert_eq!(
                    <$t>::from_value(
                        ValueDeserializer::<BinValue>::deserialize(
                            (
                                $ct,
                                if $sig {
                                    ColumnFlags::empty()
                                } else {
                                    ColumnFlags::UNSIGNED_FLAG
                                }
                            ),
                            &mut pb
                        )
                        .unwrap()
                        .0,
                    ),
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

    rt!(f32_one, f32, 1.0, ColumnType::MYSQL_TYPE_FLOAT, false);
    rt!(f64_one, f64, 1.0, ColumnType::MYSQL_TYPE_DOUBLE, false);

    rt!(
        u8_max,
        u8,
        u8::max_value(),
        ColumnType::MYSQL_TYPE_TINY,
        false
    );
    rt!(
        i8_max,
        i8,
        i8::max_value(),
        ColumnType::MYSQL_TYPE_TINY,
        true
    );
    rt!(
        u16_max,
        u16,
        u16::max_value(),
        ColumnType::MYSQL_TYPE_SHORT,
        false
    );
    rt!(
        i16_max,
        i16,
        i16::max_value(),
        ColumnType::MYSQL_TYPE_SHORT,
        true
    );
    rt!(
        u32_max,
        u32,
        u32::max_value(),
        ColumnType::MYSQL_TYPE_LONG,
        false
    );
    rt!(
        i32_max,
        i32,
        i32::max_value(),
        ColumnType::MYSQL_TYPE_LONG,
        true
    );
    rt!(
        u64_max,
        u64,
        u64::max_value(),
        ColumnType::MYSQL_TYPE_LONGLONG,
        false
    );
    rt!(
        i64_max,
        i64,
        i64::max_value(),
        ColumnType::MYSQL_TYPE_LONGLONG,
        true
    );

    rt!(opt_some, Option<u8>, Some(1), ColumnType::MYSQL_TYPE_TINY);

    rt!(
        time,
        chrono::NaiveDate,
        chrono::Local::today().naive_local(),
        ColumnType::MYSQL_TYPE_DATE
    );
    rt!(
        datetime,
        chrono::NaiveDateTime,
        chrono::Utc.ymd(1989, 12, 7).and_hms(8, 0, 4).naive_utc(),
        ColumnType::MYSQL_TYPE_DATETIME
    );
    rt!(
        dur,
        time::Duration,
        time::Duration::from_secs(1893),
        ColumnType::MYSQL_TYPE_TIME
    );
    rt!(
        bytes,
        Vec<u8>,
        vec![0x42, 0x00, 0x1a],
        ColumnType::MYSQL_TYPE_BLOB
    );
    rt!(
        string,
        String,
        "foobar".to_owned(),
        ColumnType::MYSQL_TYPE_STRING
    );
}
