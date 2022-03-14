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

use crate::packet::*;

#[test]
fn test_one_ping() {
    assert_eq!(
        onepacket(&[0x01, 0, 0, 0, 0x10]).unwrap().1,
        (0, &[0x10][..])
    );
}

#[test]
fn test_ping() {
    let p = packet(&[0x01, 0, 0, 0, 0x10]).unwrap().1;
    assert_eq!(p.0, 0);
    assert_eq!(&*p.1, &[0x10][..]);
}

#[test]
fn test_long_exact() {
    let mut data = vec![0xff, 0xff, 0xff, 0];
    data.extend(&[0; U24_MAX][..]);
    data.push(0x00);
    data.push(0x00);
    data.push(0x00);
    data.push(1);

    let (rest, p) = packet(&data[..]).unwrap();
    assert!(rest.is_empty());
    assert_eq!(p.0, 1);
    assert_eq!(p.1.len(), U24_MAX);
    assert_eq!(&*p.1, &[0; U24_MAX][..]);
}

#[test]
fn test_long_more() {
    let mut data = vec![0xff, 0xff, 0xff, 0];
    data.extend(&[0; U24_MAX][..]);
    data.push(0x01);
    data.push(0x00);
    data.push(0x00);
    data.push(1);
    data.push(0x10);

    let (rest, p) = packet(&data[..]).unwrap();
    assert!(rest.is_empty());
    assert_eq!(p.0, 1);
    assert_eq!(p.1.len(), U24_MAX + 1);
    assert_eq!(&p.1[..U24_MAX], &[0; U24_MAX][..]);
    assert_eq!(&p.1[U24_MAX..], &[0x10]);
}
