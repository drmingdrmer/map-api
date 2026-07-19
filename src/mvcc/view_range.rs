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

use std::io;
use std::ops::RangeBounds;

use seq_marked::SeqMarked;

use crate::IOResultStream;
use crate::MapKey;

/// Range read operations for a view that owns its sequence boundary.
#[async_trait::async_trait]
pub trait ViewRange<K>: Send + Sync
where K: MapKey
{
    /// Returns an async stream of key-value pairs within the specified range.
    async fn range<R>(&self, range: R) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static;
}
