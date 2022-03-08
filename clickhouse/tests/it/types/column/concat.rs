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

use std::sync::Arc;

use opensrv_clickhouse::types::column::concat::*;
use opensrv_clickhouse::types::column::*;

#[test]
fn test_build_index() {
    let sizes = vec![2_usize, 3, 4];
    let index = build_index(sizes.iter().cloned());
    assert_eq!(index, vec![0, 2, 5, 9])
}

#[test]
fn test_find_chunk() {
    let index = vec![0_usize, 2, 5, 9];
    assert_eq!(find_chunk(&index, 0), 0);
    assert_eq!(find_chunk(&index, 1), 0);
    assert_eq!(find_chunk(&index, 2), 1);
    assert_eq!(find_chunk(&index, 3), 1);
    assert_eq!(find_chunk(&index, 4), 1);
    assert_eq!(find_chunk(&index, 5), 2);
    assert_eq!(find_chunk(&index, 6), 2);

    assert_eq!(find_chunk(&index, 7), 2);
    assert_eq!(find_chunk(&[0], 7), 0);
}

#[test]
fn test_find_chunk2() {
    let index = vec![0_usize, 0, 5];
    assert_eq!(find_chunk(&index, 0), 1);
    assert_eq!(find_chunk(&index, 1), 1);
    assert_eq!(find_chunk(&index, 2), 1);
    assert_eq!(find_chunk(&index, 3), 1);
    assert_eq!(find_chunk(&index, 4), 1);
    assert_eq!(find_chunk(&index, 5), 0);
}

#[test]
fn test_find_chunk5() {
    let index = vec![
        0_usize, 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57, 60,
        63, 66, 69,
    ];
    for i in 0..69 {
        assert_eq!(find_chunk(&index, i), 1 + i / 3);
    }
}

#[test]
fn test_concat_column() {
    let xs = vec![make_string_column(), make_string_column()];
    let actual = ConcatColumnData::concat(xs);

    assert_eq!(
        actual.at(0).as_str().unwrap(),
        "13298a5f-6a10-4fbe-9644-807f7ebf82cc"
    );
    assert_eq!(
        actual.at(1).as_str().unwrap(),
        "df0e62bb-c0db-4728-a558-821f8e8da38c"
    );
    assert_eq!(
        actual.at(2).as_str().unwrap(),
        "13298a5f-6a10-4fbe-9644-807f7ebf82cc"
    );
    assert_eq!(
        actual.at(3).as_str().unwrap(),
        "df0e62bb-c0db-4728-a558-821f8e8da38c"
    );

    assert_eq!(actual.len(), 4);
}

#[test]
fn test_concat_num_column() {
    let xs = vec![make_num_column(), make_num_column()];
    let actual = ConcatColumnData::concat(xs);

    assert_eq!(u32::from(actual.at(0)), 1_u32);
    assert_eq!(u32::from(actual.at(1)), 2_u32);
    assert_eq!(u32::from(actual.at(2)), 1_u32);
    assert_eq!(u32::from(actual.at(3)), 2_u32);

    assert_eq!(actual.len(), 4);
}

fn make_string_column() -> ArcColumnData {
    let mut data = StringColumnData::with_capacity(1);
    data.append("13298a5f-6a10-4fbe-9644-807f7ebf82cc".to_string());
    data.append("df0e62bb-c0db-4728-a558-821f8e8da38c".to_string());
    Arc::new(data)
}

fn make_num_column() -> ArcColumnData {
    let mut data = VectorColumnData::<u32>::with_capacity(1);
    data.append(1_u32);
    data.append(2_u32);
    Arc::new(data)
}
