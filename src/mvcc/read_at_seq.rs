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

//! Reads at an explicit sequence for a single key-value space.

use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use seq_marked::SeqMarked;

use crate::mvcc::Table;
use crate::IOResultStream;
use crate::MapKey;

/// Gets values visible at a supplied snapshot sequence.
///
/// The implementor does not own a snapshot sequence. Callers provide it for every read.
#[async_trait::async_trait]
pub trait GetAtSeq<K>: Send + Sync
where K: MapKey
{
    async fn get_at_seq(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error>;

    async fn get_many_at_seq(
        &self,
        keys: Vec<K>,
        snapshot_seq: u64,
    ) -> Result<Vec<SeqMarked<K::V>>, io::Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_at_seq(key, snapshot_seq).await?);
        }
        Ok(values)
    }
}

#[async_trait::async_trait]
impl<K> GetAtSeq<K> for Table<K, K::V>
where K: MapKey
{
    async fn get_at_seq(&self, key: K, snapshot_seq: u64) -> Result<SeqMarked<K::V>, io::Error> {
        Ok(self.get(key, snapshot_seq).cloned())
    }
}

/// Ranges over values visible at a supplied snapshot sequence.
///
/// The implementor does not own a snapshot sequence. Callers provide it for every read.
#[async_trait::async_trait]
pub trait RangeAtSeq<K>: Send + Sync
where K: MapKey
{
    async fn range_at_seq<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static;
}

#[async_trait::async_trait]
impl<K> RangeAtSeq<K> for Table<K, K::V>
where K: MapKey
{
    async fn range_at_seq<R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let values = self
            .range(range, snapshot_seq)
            .map(|(key, value)| (key.clone(), value.cloned()))
            .collect::<Vec<_>>();

        Ok(futures::stream::iter(values.into_iter().map(Ok)).boxed())
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(String);

    impl MapKey for TestKey {
        type V = String;
    }

    fn key(value: &str) -> TestKey {
        TestKey(value.to_string())
    }

    fn value(value: &str) -> String {
        value.to_string()
    }

    fn table() -> Table<TestKey, String> {
        let mut table = Table::new();
        table.insert(key("a"), 1, value("a1")).unwrap();
        table.insert(key("b"), 2, value("b2")).unwrap();
        table.insert(key("a"), 3, value("a3")).unwrap();
        table.insert_tombstone(key("c"), 3).unwrap();
        table.insert(key("d"), 5, value("d5")).unwrap();
        table
    }

    #[tokio::test]
    async fn test_get_at_seq_and_get_many_at_seq() {
        let table = table();

        assert_eq!(
            table.get_at_seq(key("a"), 2).await.unwrap(),
            SeqMarked::new_normal(1, value("a1"))
        );
        assert_eq!(
            table.get_at_seq(key("c"), 3).await.unwrap(),
            SeqMarked::new_tombstone(3)
        );

        let values = table
            .get_many_at_seq(vec![key("a"), key("b"), key("c"), key("d")], 3)
            .await
            .unwrap();
        assert_eq!(values, vec![
            SeqMarked::new_normal(3, value("a3")),
            SeqMarked::new_normal(2, value("b2")),
            SeqMarked::new_tombstone(3),
            SeqMarked::new_not_found(),
        ]);
    }

    #[tokio::test]
    async fn test_range_at_seq() {
        let values = table()
            .range_at_seq(key("a")..=key("d"), 3)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(values, vec![
            (key("a"), SeqMarked::new_normal(3, value("a3"))),
            (key("b"), SeqMarked::new_normal(2, value("b2"))),
            (key("c"), SeqMarked::new_tombstone(3)),
        ]);
    }
}
