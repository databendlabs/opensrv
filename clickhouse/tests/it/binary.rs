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

use opensrv_clickhouse::binary::put_uvarint;
use opensrv_clickhouse::binary::ReadEx;

#[test]
fn test_read_uvarint() {
    use std::io::Cursor;

    let bytes = [194_u8, 10];
    let mut cursor = Cursor::new(bytes);

    let actual = cursor.read_uvarint().unwrap();

    assert_eq!(actual, 1346)
}

#[test]
fn test_put_uvarint() {
    let expected = [148u8, 145, 6, 0, 0, 0, 0, 0, 0, 0];
    let mut buffer = [0u8; 10];

    let actual = put_uvarint(&mut buffer[..], 100_500);

    assert_eq!(actual, 3);
    assert_eq!(buffer, expected);
}
