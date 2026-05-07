//! Cutover state machine for `SubscribeBoard` streams.
//!
//! Tracks the replay → live transition for a single board subscription and
//! deduplicates events across phase boundaries.
//!
//! ## Stream lifecycle
//!
//! 1. **Replay phase**: server emits snapshot events with `nats_seq = 0`
//!    (synthetic). Each event carries a stable ULID `event_id`.
//! 2. **Cutover envelope**: server emits `Cutover { last_replay_nats_seq }`.
//!    The caller drives this by calling [`CutoverTracker::cutover_to_live`].
//! 3. **Live phase**: server emits real JetStream events with monotone
//!    `nats_seq`. The server guarantees `nats_seq` is strictly increasing
//!    within a well-behaved stream; [`Outcome::OutOfOrder`] surfaces
//!    violations.
//!
//! ## Dedupe policy
//!
//! The tracker maintains a bounded set of the last 1 024 `event_id`s seen
//! (ring buffer). If an `event_id` arrives that is already in the set, the
//! event is dropped ([`Outcome::Drop`]). This covers:
//!
//! - Snapshot events replayed a second time on reconnect.
//! - Live events whose `nats_seq` falls within the snapshot window
//!   (Pre-mortem 3 / optimistic-mutation + stream-replay drift).
//!
//! ## Note on `observe_live` during Replay phase
//!
//! Calling `observe_live` before `cutover_to_live` returns
//! [`Outcome::OutOfOrder`]. The Stage 4c handler must buffer or discard
//! live-tail messages that arrive during snapshot emission.

use std::collections::{HashSet, VecDeque};

// ── BoundedSet ────────────────────────────────────────────────────────────────

/// Ring-buffer deduplication set with a fixed capacity.
///
/// When `capacity` is hit, the oldest entry is removed from both the queue
/// and the hash set before the new entry is inserted.
struct BoundedSet {
    capacity: usize,
    queue: VecDeque<String>,
    set: HashSet<String>,
}

impl BoundedSet {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            queue: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
        }
    }

    /// Insert `id`. Returns `true` if it was newly inserted, `false` if
    /// it was already present (duplicate). On capacity overflow the oldest
    /// entry is evicted first.
    fn insert(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return false;
        }
        if self.queue.len() >= self.capacity {
            if let Some(oldest) = self.queue.pop_front() {
                self.set.remove(&oldest);
            }
        }
        self.queue.push_back(id.to_string());
        self.set.insert(id.to_string());
        true
    }

    /// Returns `true` if `id` is currently in the set.
    #[cfg(test)]
    fn contains(&self, id: &str) -> bool {
        self.set.contains(id)
    }

    /// Current number of entries.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.queue.len()
    }
}

// ── Phase ─────────────────────────────────────────────────────────────────────

/// Phase of the cutover state machine.
#[derive(Debug, PartialEq, Clone)]
pub enum Phase {
    /// Receiving snapshot replay events (`nats_seq == 0`).
    Replay,
    /// Live JetStream tail. `resume_seq` is the JetStream sequence at which
    /// live events start (= `last_replay_nats_seq + 1`).
    Live { resume_seq: u64 },
}

// ── Outcome ───────────────────────────────────────────────────────────────────

/// Result of observing an event through the cutover tracker.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// First time seeing this `event_id`; emit to client.
    Emit,
    /// Duplicate `event_id`; drop silently.
    Drop,
    /// Live event with `nats_seq` ≤ last seen seq (out-of-order or exact
    /// duplicate in seq space), or `observe_live` called during Replay phase.
    /// Drop and log.
    OutOfOrder,
}

// ── CutoverTracker ────────────────────────────────────────────────────────────

/// Tracks cutover progress for a single `SubscribeBoard` stream.
pub struct CutoverTracker {
    phase: Phase,
    seen_ids: BoundedSet,
    last_live_seq: Option<u64>,
}

impl CutoverTracker {
    /// Capacity of the event-id ring buffer.
    const SEEN_CAPACITY: usize = 1_024;

    /// Construct a new tracker in the [`Phase::Replay`] state.
    pub fn new() -> Self {
        Self {
            phase: Phase::Replay,
            seen_ids: BoundedSet::new(Self::SEEN_CAPACITY),
            last_live_seq: None,
        }
    }

    /// Current phase.
    pub fn phase(&self) -> &Phase {
        &self.phase
    }

    /// Observe a snapshot replay event.
    ///
    /// May be called in any phase — snapshot events can arrive after cutover
    /// during a reconnect/resume path. Dedupe still applies.
    ///
    /// Returns [`Outcome::Emit`] on first-seen, [`Outcome::Drop`] on duplicate.
    pub fn observe_replay(&mut self, event_id: &str) -> Outcome {
        if self.seen_ids.insert(event_id) {
            Outcome::Emit
        } else {
            Outcome::Drop
        }
    }

    /// Transition from Replay to Live.
    ///
    /// `last_replay_nats_seq` is the JetStream sequence the snapshot was taken
    /// at (from the `Cutover` envelope). May be `0` for an empty board.
    /// After this call, `observe_live` will accept events with `nats_seq >`
    /// `last_replay_nats_seq`.
    ///
    /// Calling this more than once on the same tracker is a logic error and
    /// will be a no-op (the second call does not regress the phase).
    pub fn cutover_to_live(&mut self, last_replay_nats_seq: u64) {
        if self.phase == Phase::Replay {
            self.phase = Phase::Live {
                resume_seq: last_replay_nats_seq.saturating_add(1),
            };
            // Seed last_live_seq at the cutover boundary so that the first
            // real live event (last_replay_nats_seq + 1) passes the
            // monotonicity check.
            self.last_live_seq = Some(last_replay_nats_seq);
        }
        // Already live — no-op.
    }

    /// Observe a live JetStream event.
    ///
    /// Returns:
    /// - [`Outcome::OutOfOrder`] if called during [`Phase::Replay`].
    /// - [`Outcome::Drop`] if `event_id` is a duplicate.
    /// - [`Outcome::OutOfOrder`] if `nats_seq` ≤ `last_live_seq` (out-of-order
    ///   or exact sequence duplicate).
    /// - [`Outcome::Emit`] otherwise.
    pub fn observe_live(&mut self, event_id: &str, nats_seq: u64) -> Outcome {
        // Guard: must be in Live phase.
        if self.phase == Phase::Replay {
            return Outcome::OutOfOrder;
        }

        // Guard: seq must be strictly greater than the last seen live seq.
        if let Some(last_seq) = self.last_live_seq {
            if nats_seq <= last_seq {
                return Outcome::OutOfOrder;
            }
        }

        // Dedupe by event_id across phases.
        if !self.seen_ids.insert(event_id) {
            return Outcome::Drop;
        }

        self.last_live_seq = Some(nats_seq);
        Outcome::Emit
    }
}

impl Default for CutoverTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Full happy-path: replay events then live events, each emitted once.
    #[test]
    fn replay_then_live_emits_each_event_once() {
        let mut t = CutoverTracker::new();

        // Replay phase: two synthetic events.
        assert_eq!(t.observe_replay("evt-001"), Outcome::Emit);
        assert_eq!(t.observe_replay("evt-002"), Outcome::Emit);

        // Cutover.
        t.cutover_to_live(42);
        assert_eq!(*t.phase(), Phase::Live { resume_seq: 43 });

        // Live phase: two real events.
        assert_eq!(t.observe_live("evt-003", 43), Outcome::Emit);
        assert_eq!(t.observe_live("evt-004", 44), Outcome::Emit);
    }

    /// Replaying the same event_id during the replay phase is a Drop.
    #[test]
    fn replay_event_with_repeated_id_is_dropped() {
        let mut t = CutoverTracker::new();
        assert_eq!(t.observe_replay("dup"), Outcome::Emit);
        assert_eq!(t.observe_replay("dup"), Outcome::Drop);
    }

    /// A live event whose event_id was already seen during replay is Dropped
    /// (dedupe is cross-phase).
    #[test]
    fn live_event_with_replay_id_is_dropped() {
        let mut t = CutoverTracker::new();
        assert_eq!(t.observe_replay("shared-id"), Outcome::Emit);
        t.cutover_to_live(10);
        // Same event_id arrives again on the live tail.
        assert_eq!(t.observe_live("shared-id", 11), Outcome::Drop);
    }

    /// A live event with nats_seq strictly less than the last seen seq is
    /// OutOfOrder.
    #[test]
    fn live_event_out_of_order_is_outoforder() {
        let mut t = CutoverTracker::new();
        t.cutover_to_live(5);
        assert_eq!(t.observe_live("a", 6), Outcome::Emit);
        assert_eq!(t.observe_live("b", 7), Outcome::Emit);
        // seq 6 again — out of order.
        assert_eq!(t.observe_live("c", 6), Outcome::OutOfOrder);
    }

    /// A live event with nats_seq exactly equal to the last seen live seq is
    /// OutOfOrder (not a normal forward advance).
    #[test]
    fn live_event_seq_equals_last_is_out_of_order() {
        let mut t = CutoverTracker::new();
        t.cutover_to_live(0);
        assert_eq!(t.observe_live("x", 1), Outcome::Emit);
        // Same seq again — not strictly greater.
        assert_eq!(t.observe_live("y", 1), Outcome::OutOfOrder);
    }

    /// After cutover_to_live the phase transitions to Live.
    #[test]
    fn cutover_transitions_phase_replay_to_live() {
        let mut t = CutoverTracker::new();
        assert_eq!(*t.phase(), Phase::Replay);
        t.cutover_to_live(100);
        assert_eq!(*t.phase(), Phase::Live { resume_seq: 101 });
    }

    /// BoundedSet evicts the oldest entry when capacity is exceeded.
    #[test]
    fn bounded_set_evicts_oldest_when_full() {
        let cap = 4;
        let mut bs = BoundedSet::new(cap);

        bs.insert("a");
        bs.insert("b");
        bs.insert("c");
        bs.insert("d");
        assert_eq!(bs.len(), cap);

        // Insert a 5th item — "a" should be evicted.
        bs.insert("e");
        assert_eq!(bs.len(), cap);
        assert!(!bs.contains("a"), "oldest entry should have been evicted");
        assert!(bs.contains("b"));
        assert!(bs.contains("e"));
    }

    /// Inserting the same id multiple times does not consume additional
    /// capacity (the set does not thrash).
    #[test]
    fn bounded_set_does_not_thrash_on_repeats() {
        let cap = 4;
        let mut bs = BoundedSet::new(cap);

        bs.insert("z");
        // Insert "z" two more times.
        bs.insert("z");
        bs.insert("z");

        // Only one slot should be occupied.
        assert_eq!(bs.len(), 1);
    }

    /// An empty board can cut over at seq 0 — this is a valid initial state.
    #[test]
    fn cutover_to_live_with_zero_last_seq_is_valid() {
        let mut t = CutoverTracker::new();
        t.cutover_to_live(0);
        assert_eq!(*t.phase(), Phase::Live { resume_seq: 1 });
        // First live event at seq 1 must succeed.
        assert_eq!(t.observe_live("first", 1), Outcome::Emit);
    }

    /// `observe_live` called during the Replay phase returns OutOfOrder.
    ///
    /// Rationale: live-tail messages that arrive while the snapshot is still
    /// being emitted must not be forwarded to the client; the Stage 4c handler
    /// is expected to buffer them until after `cutover_to_live`.
    #[test]
    fn phase_replay_rejects_observe_live() {
        let mut t = CutoverTracker::new();
        assert_eq!(*t.phase(), Phase::Replay);
        // Attempting to observe a live event before cutover returns OutOfOrder.
        assert_eq!(t.observe_live("premature", 10), Outcome::OutOfOrder);
        // Phase must remain Replay — observe_live must not side-effect phase.
        assert_eq!(*t.phase(), Phase::Replay);
    }

    /// Calling cutover_to_live a second time is a no-op and does not regress
    /// the phase or the resume_seq.
    #[test]
    fn double_cutover_is_no_op() {
        let mut t = CutoverTracker::new();
        t.cutover_to_live(10);
        assert_eq!(*t.phase(), Phase::Live { resume_seq: 11 });
        // Second call with a different seq — must not change anything.
        t.cutover_to_live(999);
        assert_eq!(*t.phase(), Phase::Live { resume_seq: 11 });
    }
}
