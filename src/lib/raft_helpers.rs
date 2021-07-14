use slog::{Drain, Logger};
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use raft::eraftpb::ConfState;
use raft::prelude::*;
use raft::storage::MemStorage;

use slog::{info, o};

type ProposeCallback = Box<dyn Fn() + Send>;
pub fn on_ready(raft_group: &mut RawNode<MemStorage>, cbs: &mut HashMap<u8, ProposeCallback>) {
  if !raft_group.has_ready() {
    return;
  }
  let store = raft_group.raft.raft_log.store.clone();

  // Get the `Ready` with `RawNode::ready` interface.
  let mut ready = raft_group.ready();

  let handle_messages = |msgs: Vec<Message>| {
    for _msg in msgs {
      // Send messages to other peers.
    }
  };

  if !ready.messages().is_empty() {
    // Send out the messages come from the node.
    handle_messages(ready.take_messages());
  }

  if !ready.snapshot().is_empty() {
    // This is a snapshot, we need to apply the snapshot at first.
    store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
  }

  let mut _last_apply_index = 0;
  let mut handle_committed_entries = |committed_entries: Vec<Entry>| {
    for entry in committed_entries {
      // Mostly, you need to save the last apply index to resume applying
      // after restart. Here we just ignore this because we use a Memory storage.
      _last_apply_index = entry.index;

      if entry.data.is_empty() {
        // Emtpy entry, when the peer becomes Leader it will send an empty entry.
        continue;
      }

      if entry.get_entry_type() == EntryType::EntryNormal {
        if let Some(cb) = cbs.remove(entry.data.get(0).unwrap()) {
          cb();
        }
      }

      // TODO: handle EntryConfChange
    }
  };
  handle_committed_entries(ready.take_committed_entries());

  if !ready.entries().is_empty() {
    // Append entries to the Raft log.
    store.wl().append(&ready.entries()).unwrap();
  }

  if let Some(hs) = ready.hs() {
    // Raft HardState changed, and we need to persist it.
    store.wl().set_hardstate(hs.clone());
  }

  if !ready.persisted_messages().is_empty() {
    // Send out the persisted messages come from the node.
    handle_messages(ready.take_persisted_messages());
  }

  // Advance the Raft.
  let mut light_rd = raft_group.advance(ready);
  // Update commit index.
  if let Some(commit) = light_rd.commit_index() {
    store.wl().mut_hard_state().set_commit(commit);
  }
  // Send out the messages.
  handle_messages(light_rd.take_messages());
  // Apply all committed entries.
  handle_committed_entries(light_rd.take_committed_entries());
  // Advance the apply index.
  raft_group.advance_apply();
}
