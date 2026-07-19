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

//! Point-in-time reads over a single key-value space.

use std::io;
use std::marker::PhantomData;
use std::ops::RangeBounds;

use seq_marked::InternalSeq;

use crate::mvcc::GetAtSeq;
use crate::mvcc::RangeAtSeq;
use crate::IOResultStream;
use crate::MapKey;
use crate::SeqMarked;

/// A fixed sequence boundary over a low-level reader.
#[derive(Clone, Debug, Default)]
pub struct Snapshot<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    snapshot_seq: InternalSeq,
    data: D,
    _phantom: PhantomData<K>,
}

impl<K, D> Snapshot<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    pub fn new(snapshot_seq: InternalSeq, data: D) -> Self {
        Self {
            snapshot_seq,
            data,
            _phantom: PhantomData,
        }
    }

    pub fn snapshot_seq(&self) -> InternalSeq {
        self.snapshot_seq
    }

    pub async fn get(&self, key: K) -> Result<SeqMarked<K::V>, io::Error> {
        self.data.get_at_seq(key, *self.snapshot_seq).await
    }

    pub async fn get_many(&self, keys: Vec<K>) -> Result<Vec<SeqMarked<K::V>>, io::Error> {
        self.data.get_many_at_seq(keys, *self.snapshot_seq).await
    }

    pub async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        self.data.range_at_seq(range, *self.snapshot_seq).await
    }

    pub fn data(&self) -> &D {
        &self.data
    }

    pub fn into_data(self) -> D {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures_util::StreamExt;

    use super::*;

    #[derive(Debug, Clone)]
    struct MockData {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl MockData {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl GetAtSeq<String> for MockData {
        async fn get_at_seq(
            &self,
            _key: String,
            seq: u64,
        ) -> Result<SeqMarked<Vec<u8>>, io::Error> {
            self.calls.lock().unwrap().push(format!("get:{seq}"));
            Ok(SeqMarked::new_not_found())
        }
    }

    #[async_trait::async_trait]
    impl RangeAtSeq<String> for MockData {
        async fn range_at_seq<R>(
            &self,
            _range: R,
            seq: u64,
        ) -> Result<IOResultStream<(String, SeqMarked<Vec<u8>>)>, io::Error>
        where
            R: RangeBounds<String> + Send + Sync + Clone + 'static,
        {
            self.calls.lock().unwrap().push(format!("range:{seq}"));
            Ok(futures::stream::empty().boxed())
        }
    }

    #[tokio::test]
    async fn test_snapshot_binds_sequence() {
        let data = MockData::new();
        let snapshot = Snapshot::new(InternalSeq::new(42), data.clone());

        snapshot.get("k".to_string()).await.unwrap();
        snapshot
            .get_many(vec!["k1".to_string(), "k2".to_string()])
            .await
            .unwrap();
        let _stream = snapshot.range(..).await.unwrap();

        assert_eq!(*data.calls.lock().unwrap(), vec![
            "get:42", "get:42", "get:42", "range:42"
        ]);
    }
}
