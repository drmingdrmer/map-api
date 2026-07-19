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

//! Multi-Version Concurrency Control (MVCC) for versioned key-value storage.
//!
//! Provides read-committed isolation with staged writes and snapshot-consistent reads.
//!
//! # Architecture
//!
//! - **[`Table`]**: In-memory versioned storage with sequence-based ordering
//! - **[`View`]**: Read-write transaction with staged changes
//! - **[`Snapshot`]**: Read-only point-in-time view with fixed sequence boundary
//! - **[`ViewApi`]**: High-level reads and writes that own their sequence boundary
//! - **[`GetAtSeq`]** and **[`RangeAtSeq`]**: Low-level reads at an explicit sequence boundary
//!
//! # Key Features
//!
//! - **Snapshot Isolation**: Each transaction sees consistent data from start time
//! - **Atomic Commits**: All changes in a transaction commit together or fail together
//! - **Streaming Range Queries**: Memory-efficient iteration over large datasets
//!
//! # Usage
//!
//! ```rust,ignore
//! use crate::mvcc::{Snapshot, Table, View};
//! use seq_marked::InternalSeq;
//!
//! // Create table and transaction view
//! let table = Table::<String, Vec<u8>>::new();
//! let snapshot = Snapshot::new(InternalSeq::new(0), table);
//! let mut view = View::new(snapshot);
//!
//! // Stage changes within transaction
//! view.set("key1".to_string(), Some(b"value1".to_vec()));
//! view.set("key2".to_string(), None); // deletion
//!
//! // Read includes staged changes
//! let current = view.get("key1".to_string()).await?;
//!
//! // Give the low-level store the staged changes to commit atomically.
//! let (_reader, last_seq, changes) = view.into_parts();
//! ```

pub mod coalesce;
pub mod read_at_seq;
pub mod snapshot;
pub mod snapshot_seq;
pub mod table;
pub mod view;
pub mod view_api;
pub mod view_get;
pub mod view_range;
pub mod view_set;
pub use self::read_at_seq::GetAtSeq;
pub use self::read_at_seq::RangeAtSeq;
pub use self::snapshot::Snapshot;
pub use self::snapshot_seq::SnapshotSeq;
pub use self::table::Table;
pub use self::view::View;
pub use self::view_api::ViewApi;
pub use self::view_get::ViewGet;
pub use self::view_range::ViewRange;
pub use self::view_set::ViewSet;
