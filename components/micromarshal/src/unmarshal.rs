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

use std::io::Error;
use std::io::Result;

use ethnum::I256;
use ethnum::U256;
use ordered_float::OrderedFloat;

pub trait Unmarshal<T> {
    fn unmarshal(scratch: &[u8]) -> T;
    fn try_unmarshal(scratch: &[u8]) -> Result<T> {
        Ok(Self::unmarshal(scratch))
    }
}

impl Unmarshal<u8> for u8 {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0]
    }
}

impl Unmarshal<u16> for u16 {
    fn unmarshal(scratch: &[u8]) -> Self {
        u16::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<u32> for u32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        u32::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<u64> for u64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        u64::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<u128> for u128 {
    fn unmarshal(scratch: &[u8]) -> Self {
        u128::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<U256> for U256 {
    fn unmarshal(scratch: &[u8]) -> Self {
        U256::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<i8> for i8 {
    fn unmarshal(scratch: &[u8]) -> Self {
        i8::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<i16> for i16 {
    fn unmarshal(scratch: &[u8]) -> Self {
        i16::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<i32> for i32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        i32::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<i64> for i64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        i64::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<i128> for i128 {
    fn unmarshal(scratch: &[u8]) -> Self {
        i128::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<I256> for I256 {
    fn unmarshal(scratch: &[u8]) -> Self {
        I256::from_le_bytes(scratch.try_into().unwrap())
    }
}

impl Unmarshal<f32> for f32 {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u32::from_le_bytes(scratch.try_into().unwrap());
        Self::from_bits(bits)
    }
}

impl Unmarshal<f64> for f64 {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u64::from_le_bytes(scratch.try_into().unwrap());
        Self::from_bits(bits)
    }
}

impl Unmarshal<OrderedFloat<f32>> for OrderedFloat<f32> {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u32::from_le_bytes(scratch.try_into().unwrap());
        f32::from_bits(bits).into()
    }
}

impl Unmarshal<OrderedFloat<f64>> for OrderedFloat<f64> {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u64::from_le_bytes(scratch.try_into().unwrap());
        f64::from_bits(bits).into()
    }
}

impl Unmarshal<bool> for bool {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0] != 0
    }
}

impl Unmarshal<char> for char {
    fn unmarshal(_: &[u8]) -> char {
        unimplemented!()
    }

    fn try_unmarshal(scratch: &[u8]) -> Result<char> {
        let bits = u32::from_le_bytes(scratch.try_into().unwrap());
        match char::from_u32(bits) {
            Some(c) => Ok(c),
            None => Err(Error::other(format!(
                "try unmarshal u32 to char failed: {}",
                bits
            ))),
        }
    }
}
