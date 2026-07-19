// Copyright 2021 Datafuse Labs
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

use crate::mvcc::ViewGet;
use crate::mvcc::ViewRange;
use crate::mvcc::ViewSet;
use crate::MapKey;

/// Combined MVCC API for a view that owns its sequence boundary.
///
/// This trait combines point reads, range reads, and writes over one key-value space.
///
/// # Type Parameters
/// - `K`: Key type satisfying [`MapKey`] constraints
pub trait ViewApi<K>
where
    K: MapKey,
    Self: ViewGet<K> + ViewRange<K> + ViewSet<K>,
{
}

impl<K, T> ViewApi<K> for T
where
    K: MapKey,
    T: ViewGet<K> + ViewRange<K> + ViewSet<K>,
{
}
