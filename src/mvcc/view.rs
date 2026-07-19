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

//! A read-write MVCC view over one key-value space.

use std::fmt;
use std::io;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use log::debug;
use seq_marked::InternalSeq;
use seq_marked::SeqMarked;
use stream_more::KMerge;
use stream_more::StreamMore;

use crate::compact::compact_seq_marked_pair;
use crate::mvcc::GetAtSeq;
use crate::mvcc::RangeAtSeq;
use crate::mvcc::Snapshot;
use crate::mvcc::Table;
use crate::mvcc::ViewGet;
use crate::mvcc::ViewRange;
use crate::mvcc::ViewSet;
use crate::util;
use crate::IOResultStream;
use crate::MapKey;

/// A transaction view with a fixed base snapshot and staged changes.
pub struct View<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    /// Whether deleting a key allocates a new sequence.
    pub(crate) increase_seq_for_tombstone: bool,
    pub(crate) changes: Table<K, K::V>,
    pub(crate) last_seq: InternalSeq,
    pub(crate) snapshot: Snapshot<K, D>,
}

impl<K, D> fmt::Debug for View<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K> + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("View")
            .field(
                "increase_seq_for_tombstone",
                &self.increase_seq_for_tombstone,
            )
            .field("changes", &self.changes)
            .field("last_seq", &self.last_seq)
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

impl<K, D> View<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    pub fn new(snapshot: Snapshot<K, D>) -> Self {
        let last_seq = snapshot.snapshot_seq();
        Self {
            increase_seq_for_tombstone: false,
            changes: Table::new(),
            last_seq,
            snapshot,
        }
    }

    pub fn with_tombstone_seq_increment(mut self, enable: bool) -> Self {
        self.increase_seq_for_tombstone = enable;
        self
    }

    pub fn with_initial_seq(mut self, seq: InternalSeq) -> Self {
        self.last_seq = seq;
        self
    }

    pub fn snapshot(&self) -> &Snapshot<K, D> {
        &self.snapshot
    }

    fn current_normal_seq(&self) -> SeqMarked<()> {
        debug!("current_normal_seq: last_seq: {}", self.last_seq);
        SeqMarked::new_normal(*self.last_seq, ())
    }

    fn next_normal_seq(&mut self) -> SeqMarked<()> {
        self.last_seq += 1;
        debug!("next_normal_seq: last_seq become: {}", self.last_seq);
        SeqMarked::new_normal(*self.last_seq, ())
    }

    fn next_tombstone_seq(&mut self) -> SeqMarked<()> {
        if self.increase_seq_for_tombstone {
            self.last_seq += 1;
        }
        debug!("next_tombstone_seq: last_seq become: {}", self.last_seq);
        SeqMarked::new_tombstone(*self.last_seq)
    }

    #[deprecated(since = "0.4.2", note = "use snapshot() instead")]
    pub fn base(&self) -> &Snapshot<K, D> {
        &self.snapshot
    }

    /// Inserting a tombstone does not increase the seq, but instead, it uses the last used seq.
    /// This is for a historical reason: the first version does not increase seq when deleting an item.
    pub fn set(&mut self, key: K, value: Option<K::V>) -> SeqMarked<()> {
        debug!("View::set: key: {:?}, value: {:?}", key, value);

        let seq = if value.is_none() {
            self.next_tombstone_seq()
        } else {
            self.next_normal_seq()
        };

        let seq_num = *seq.internal_seq();

        if let Some(value) = value {
            self.changes.insert(key, seq_num, value).unwrap();
        } else {
            self.changes.insert_tombstone(key, seq_num).unwrap();
        }

        seq
    }

    /// Stage a change at the current sequence instead of allocating a new one.
    pub fn set_without_seq_increment(&mut self, key: K, value: Option<K::V>) -> SeqMarked<()> {
        let Some(value) = value else {
            return self.set(key, None);
        };

        let seq = self.current_normal_seq();
        let seq_num = *seq.internal_seq();
        self.changes.insert(key, seq_num, value).unwrap();

        seq
    }

    pub async fn get(&self, key: K) -> Result<SeqMarked<K::V>, io::Error> {
        let updated = self.changes.get(key.clone(), *self.last_seq).cloned();
        if !updated.is_not_found() {
            return Ok(updated);
        }

        let base = self.snapshot.get(key).await?;
        Ok(compact_seq_marked_pair(updated, base))
    }

    pub async fn get_many(&self, keys: Vec<K>) -> Result<Vec<SeqMarked<K::V>>, io::Error> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    pub async fn fetch_and_set(
        &mut self,
        key: K,
        value: Option<K::V>,
    ) -> Result<(SeqMarked<K::V>, SeqMarked<K::V>), io::Error> {
        self.fetch_and_set_with_seq_increment(key, value, true)
            .await
    }

    /// Fetch and stage a change without allocating a new sequence for normal values.
    pub async fn fetch_and_set_without_seq_increment(
        &mut self,
        key: K,
        value: Option<K::V>,
    ) -> Result<(SeqMarked<K::V>, SeqMarked<K::V>), io::Error> {
        self.fetch_and_set_with_seq_increment(key, value, false)
            .await
    }

    async fn fetch_and_set_with_seq_increment(
        &mut self,
        key: K,
        value: Option<K::V>,
        increase_seq: bool,
    ) -> Result<(SeqMarked<K::V>, SeqMarked<K::V>), io::Error> {
        let old_value = self.get(key.clone()).await?;
        if old_value.is_not_found() && value.is_none() {
            return Ok((old_value, SeqMarked::new_tombstone(0)));
        }

        let order_key = if increase_seq {
            self.set(key, value.clone())
        } else {
            self.set_without_seq_increment(key, value.clone())
        };
        let new_value = match value {
            Some(value) => order_key.map(|_| value),
            None => SeqMarked::new_tombstone(*order_key.internal_seq()),
        };
        Ok((old_value, new_value))
    }

    pub async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        let base = self.snapshot.range(range.clone()).await?;
        let updates = self
            .changes
            .range(range, *self.last_seq)
            .map(|(key, value)| (key.clone(), value.cloned()))
            .collect::<Vec<_>>();
        let updates = futures::stream::iter(updates.into_iter().map(Ok)).boxed();

        let merged = KMerge::by(util::by_key_seq).merge(base).merge(updates);
        Ok(merged.coalesce(util::merge_kv_results).boxed())
    }

    /// Return the low-level reader, final sequence, and staged changes for committing.
    pub fn into_parts(self) -> (D, InternalSeq, Table<K, K::V>) {
        (self.snapshot.into_data(), self.last_seq, self.changes)
    }
}

#[async_trait::async_trait]
impl<K, D> ViewSet<K> for View<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    fn set(&mut self, key: K, value: Option<K::V>) -> SeqMarked<()> {
        View::set(self, key, value)
    }
}

#[async_trait::async_trait]
impl<K, D> ViewGet<K> for View<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    async fn get(&self, key: K) -> Result<SeqMarked<K::V>, io::Error> {
        View::get(self, key).await
    }
}

#[async_trait::async_trait]
impl<K, D> ViewRange<K> for View<K, D>
where
    K: MapKey,
    D: GetAtSeq<K> + RangeAtSeq<K>,
{
    async fn range<R>(&self, range: R) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, io::Error>
    where R: RangeBounds<K> + Send + Sync + Clone + 'static {
        View::range(self, range).await
    }
}

#[cfg(test)]
mod tests {
    use futures_util::TryStreamExt;
    use seq_marked::InternalSeq;
    use seq_marked::SeqMarked;

    use super::*;
    use crate::mvcc::ViewApi;
    use crate::MapKey;

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

    fn view() -> View<TestKey, Table<TestKey, String>> {
        let mut table = Table::new();
        table.insert(key("base"), 1, value("base-value")).unwrap();
        View::new(Snapshot::new(InternalSeq::new(1), table))
    }

    fn assert_view_api<T: ViewApi<TestKey>>(_: &T) {}

    #[test]
    fn test_view_implements_view_api() {
        assert_view_api(&view());
    }

    #[tokio::test]
    async fn test_staged_value_overrides_snapshot() {
        let mut view = view();
        assert_eq!(
            view.get(key("base")).await.unwrap(),
            SeqMarked::new_normal(1, value("base-value"))
        );

        view.set(key("base"), Some(value("updated")));
        assert_eq!(
            view.get(key("base")).await.unwrap(),
            SeqMarked::new_normal(2, value("updated"))
        );
    }

    #[tokio::test]
    async fn test_snapshot_hides_future_values() {
        let mut table = Table::new();
        table.insert(key("base"), 1, value("base-value")).unwrap();
        table
            .insert(key("future"), 2, value("future-value"))
            .unwrap();
        let view = View::new(Snapshot::new(InternalSeq::new(1), table));

        assert_eq!(
            view.get_many(vec![key("base"), key("future")])
                .await
                .unwrap(),
            vec![
                SeqMarked::new_normal(1, value("base-value")),
                SeqMarked::new_not_found(),
            ]
        );
    }

    #[tokio::test]
    async fn test_secondary_change_reuses_sequence() {
        let mut view = view();
        assert_eq!(
            view.set(key("primary"), Some(value("p"))),
            SeqMarked::new_normal(2, ())
        );
        assert_eq!(
            view.set_without_seq_increment(key("secondary"), Some(value("s"))),
            SeqMarked::new_normal(2, ())
        );
        assert_eq!(view.last_seq, InternalSeq::new(2));
        assert_eq!(
            view.get(key("primary")).await.unwrap(),
            SeqMarked::new_normal(2, value("p"))
        );
        assert_eq!(
            view.get(key("secondary")).await.unwrap(),
            SeqMarked::new_normal(2, value("s"))
        );
        assert_eq!(
            view.fetch_and_set_without_seq_increment(key("tertiary"), Some(value("t")))
                .await
                .unwrap(),
            (
                SeqMarked::new_not_found(),
                SeqMarked::new_normal(2, value("t")),
            )
        );
        assert_eq!(view.last_seq, InternalSeq::new(2));
    }

    #[tokio::test]
    async fn test_delete_missing_key_does_not_stage_tombstone() {
        let mut view = view();
        let (old_value, new_value) = view.fetch_and_set(key("missing"), None).await.unwrap();

        assert_eq!(old_value, SeqMarked::new_not_found());
        assert_eq!(new_value, SeqMarked::new_tombstone(0));
        assert!(view.changes.inner.is_empty());
    }

    #[tokio::test]
    async fn test_range_merges_snapshot_and_changes() {
        let mut view = view();
        view.set(key("base"), None);
        view.set(key("new"), Some(value("new-value")));

        let values = view
            .range(..)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![
            (key("base"), SeqMarked::new_tombstone(1)),
            (key("new"), SeqMarked::new_normal(2, value("new-value"))),
        ]);
    }

    #[tokio::test]
    async fn test_tombstone_sequence_configuration() {
        let mut stable_view = view();
        assert_eq!(
            stable_view.set(key("base"), None),
            SeqMarked::new_tombstone(1)
        );
        assert_eq!(stable_view.last_seq, InternalSeq::new(1));

        let mut incrementing_view = view().with_tombstone_seq_increment(true);
        assert_eq!(
            incrementing_view.set(key("base"), None),
            SeqMarked::new_tombstone(2)
        );
        assert_eq!(incrementing_view.last_seq, InternalSeq::new(2));
        assert_eq!(
            incrementing_view.get(key("base")).await.unwrap(),
            SeqMarked::new_tombstone(2)
        );
    }

    #[tokio::test]
    async fn test_get_many_merges_snapshot_and_changes() {
        let mut view = view();
        view.set(key("base"), Some(value("updated")));
        view.set(key("new"), Some(value("new-value")));

        let values = view
            .get_many(vec![key("base"), key("new"), key("missing")])
            .await
            .unwrap();
        assert_eq!(values, vec![
            SeqMarked::new_normal(2, value("updated")),
            SeqMarked::new_normal(3, value("new-value")),
            SeqMarked::new_not_found(),
        ]);
    }

    #[tokio::test]
    async fn test_tombstone_resurrection_preserves_versions() {
        let mut view = view();
        view.set(key("base"), None);
        view.set(key("base"), Some(value("resurrected")));

        assert_eq!(
            view.changes.get(key("base"), 1),
            SeqMarked::new_tombstone(1)
        );
        assert_eq!(
            view.changes.get(key("base"), 2),
            SeqMarked::new_normal(2, &value("resurrected"))
        );
        assert_eq!(
            view.get(key("base")).await.unwrap(),
            SeqMarked::new_normal(2, value("resurrected"))
        );
    }

    #[tokio::test]
    async fn test_range_respects_bounds_and_tombstones() {
        let mut table = Table::new();
        table.insert(key("a"), 1, value("a-value")).unwrap();
        table.insert(key("b"), 2, value("b-value")).unwrap();
        table.insert(key("c"), 3, value("c-value")).unwrap();
        let mut view = View::new(Snapshot::new(InternalSeq::new(3), table));
        view.set(key("b"), None);
        view.set(key("d"), Some(value("d-value")));

        let values = view
            .range(key("b")..=key("d"))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![
            (key("b"), SeqMarked::new_tombstone(3)),
            (key("c"), SeqMarked::new_normal(3, value("c-value"))),
            (key("d"), SeqMarked::new_normal(4, value("d-value"))),
        ]);
    }

    #[tokio::test]
    async fn test_fetch_and_set_transitions() {
        let mut view = view();
        assert_eq!(
            view.fetch_and_set(key("base"), Some(value("updated")))
                .await
                .unwrap(),
            (
                SeqMarked::new_normal(1, value("base-value")),
                SeqMarked::new_normal(2, value("updated")),
            )
        );
        assert_eq!(
            view.fetch_and_set(key("base"), None).await.unwrap(),
            (
                SeqMarked::new_normal(2, value("updated")),
                SeqMarked::new_tombstone(2),
            )
        );
        assert_eq!(
            view.fetch_and_set(key("base"), Some(value("resurrected")))
                .await
                .unwrap(),
            (
                SeqMarked::new_tombstone(2),
                SeqMarked::new_normal(3, value("resurrected")),
            )
        );
    }

    #[tokio::test]
    async fn test_initial_sequence_is_respected() {
        let mut view = view().with_initial_seq(InternalSeq::new(10));
        assert_eq!(
            view.set(key("primary"), Some(value("p"))),
            SeqMarked::new_normal(11, ())
        );
        assert_eq!(
            view.set_without_seq_increment(key("secondary"), Some(value("s"))),
            SeqMarked::new_normal(11, ())
        );
        assert_eq!(view.last_seq, InternalSeq::new(11));
    }

    #[tokio::test]
    async fn test_sequence_ordering() {
        let mut view = view();
        view.set(key("k1"), Some(value("v1")));
        view.set(key("k2"), Some(value("v2")));
        view.set(key("k3"), Some(value("v3")));

        assert_eq!(view.last_seq, InternalSeq::new(4));
        assert_eq!(
            view.changes.get(key("k1"), 4),
            SeqMarked::new_normal(2, &value("v1"))
        );
        assert_eq!(
            view.changes.get(key("k2"), 4),
            SeqMarked::new_normal(3, &value("v2"))
        );
        assert_eq!(
            view.changes.get(key("k3"), 4),
            SeqMarked::new_normal(4, &value("v3"))
        );
    }

    #[tokio::test]
    async fn test_empty_base_view() {
        let mut view = View::new(Snapshot::new(InternalSeq::new(1), Table::new()));
        view.set(key("k1"), Some(value("v1")));
        view.set(key("k2"), None);

        assert_eq!(
            view.get_many(vec![key("k1"), key("k2")]).await.unwrap(),
            vec![
                SeqMarked::new_normal(2, value("v1")),
                SeqMarked::new_tombstone(2),
            ]
        );

        let values = view
            .range(..)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![
            (key("k1"), SeqMarked::new_normal(2, value("v1"))),
            (key("k2"), SeqMarked::new_tombstone(2)),
        ]);
    }

    #[tokio::test]
    async fn test_zero_initial_sequence() {
        let mut view = view().with_initial_seq(InternalSeq::new(0));
        view.set(key("k1"), Some(value("v1")));
        view.set(key("k2"), None);

        assert_eq!(view.last_seq, InternalSeq::new(1));
        assert_eq!(
            view.get_many(vec![key("k1"), key("k2")]).await.unwrap(),
            vec![
                SeqMarked::new_normal(1, value("v1")),
                SeqMarked::new_tombstone(1),
            ]
        );
    }

    #[tokio::test]
    async fn test_max_sequence_boundary() {
        let mut view = view().with_initial_seq(InternalSeq::new(u64::MAX - 2));

        view.set(key("k1"), Some(value("v1")));
        assert_eq!(view.last_seq, InternalSeq::new(u64::MAX - 1));

        view.set(key("k2"), Some(value("v2")));
        assert_eq!(view.last_seq, InternalSeq::new(u64::MAX));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            view.set(key("k3"), Some(value("v3")))
        }));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_updates_same_key() {
        let mut view = view();
        view.set(key("key"), Some(value("v1")));
        view.set(key("key"), Some(value("v2")));
        view.set(key("key"), Some(value("v3")));
        view.set(key("key"), None);
        view.set(key("key"), Some(value("v4")));

        assert_eq!(view.last_seq, InternalSeq::new(5));
        assert_eq!(
            view.changes.get(key("key"), 2),
            SeqMarked::new_normal(2, &value("v1"))
        );
        assert_eq!(
            view.changes.get(key("key"), 3),
            SeqMarked::new_normal(3, &value("v2"))
        );
        assert_eq!(view.changes.get(key("key"), 4), SeqMarked::new_tombstone(4));
        assert_eq!(
            view.get(key("key")).await.unwrap(),
            SeqMarked::new_normal(5, value("v4"))
        );
    }

    #[tokio::test]
    async fn test_empty_key_lists() {
        assert_eq!(
            view().get_many(vec![]).await.unwrap(),
            Vec::<SeqMarked<String>>::new()
        );
    }

    #[tokio::test]
    async fn test_range_from_base_only() {
        let values = view()
            .range(..)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(values, vec![(
            key("base"),
            SeqMarked::new_normal(1, value("base-value"))
        )]);
    }

    #[tokio::test]
    async fn test_range_with_all_tombstones() {
        let mut view = view();
        view.set(key("base"), None);
        view.set(key("k1"), None);
        view.set(key("k2"), None);

        let values = view
            .range(..)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![
            (key("base"), SeqMarked::new_tombstone(1)),
            (key("k1"), SeqMarked::new_tombstone(1)),
            (key("k2"), SeqMarked::new_tombstone(1)),
        ]);
    }

    #[tokio::test]
    async fn test_range_single_key() {
        let values = view()
            .range(key("base")..=key("base"))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(values, vec![(
            key("base"),
            SeqMarked::new_normal(1, value("base-value"))
        )]);
    }

    #[tokio::test]
    async fn test_range_empty_result() {
        let values = view()
            .range(key("x")..=key("x"))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(values, Vec::<(TestKey, SeqMarked<String>)>::new());
    }

    #[test]
    fn test_into_parts_preserves_staged_changes() {
        let mut view = view();
        view.set(key("new"), Some(value("new-value")));

        let (_data, last_seq, changes) = view.into_parts();
        assert_eq!(last_seq, InternalSeq::new(2));
        assert_eq!(
            changes.get(key("new"), 2),
            SeqMarked::new_normal(2, &value("new-value"))
        );
    }
}
