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

use std::fmt;
use std::mem;
use std::slice;

use crate::io::Marshal;
use crate::io::Unmarshal;
use crate::types::StatBuffer;

#[derive(Clone)]
pub struct List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    data: Vec<T>,
}

impl<T> List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn at(&self, index: usize) -> T {
        self.data[index]
    }

    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    pub fn new() -> List<T> {
        List { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> List<T> {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn resize(&mut self, new_len: usize, value: T) {
        self.data.resize(new_len, value);
    }

    pub(super) unsafe fn set_len(&mut self, new_len: usize) {
        self.data.set_len(new_len);
    }

    pub(super) unsafe fn as_ptr(&self) -> *const T {
        self.data.as_ptr()
    }
}

impl<T> Default for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> fmt::Debug for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.data)
    }
}

impl<T> AsRef<[u8]> for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn as_ref(&self) -> &[u8] {
        let ptr = self.data.as_ptr() as *const u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts(ptr, size) }
    }
}

impl<T> AsMut<[u8]> for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn as_mut(&mut self) -> &mut [u8] {
        let ptr = self.data.as_mut_ptr() as *mut u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts_mut(ptr, size) }
    }
}
