//! Hot-path execution tracing for copperdb.
//!
//! Equivalent to Go's `pkg/cypher/executor_hotpath_trace.go` in NornicDB v1.0.40.
//!
//! Records which specialised execution paths were taken during the most recent
//! `Execute` call.  Useful for diagnostics, benchmarking, and test assertions.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Snapshot of hot-path flags for a single query execution.
///
/// Each field is `true` if the corresponding fast path was taken.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HotPathTrace {
    /// The outer executor used an indexed top-k lookup.
    pub outer_index_top_k: bool,
    /// The outer executor fell back to a full scan.
    pub outer_scan_fallback_used: bool,
    /// The fabric batcher applied rows in a vectorised batch.
    pub fabric_batched_apply_rows: bool,
    /// A simple `MATCH … LIMIT n` fast path was used.
    pub simple_match_limit_fast_path: bool,
    /// A compound-query (multi-MATCH) fast path was used.
    pub compound_query_fast_path: bool,
    /// Traversal used a seeded top-k on the start node.
    pub traversal_start_seed_top_k: bool,
    /// Traversal used a seeded top-k on the end node.
    pub traversal_end_seed_top_k: bool,
    /// Traversal seeded its start node from an indexed property IN-list.
    pub traversal_start_seed_property_in: bool,
    /// An UNWIND simple-merge batch path was used.
    pub unwind_simple_merge_batch: bool,
    /// An UNWIND fixed-chain link-batch path was used.
    pub unwind_fixed_chain_link_batch: bool,
    /// An UNWIND multi-MATCH relationship batch path was used.
    pub unwind_multi_match_relationship_batch: bool,
    /// A CALL tail-traversal fast path was used.
    pub call_tail_traversal_fast_path: bool,
    /// MERGE used a schema-based lookup (index scan).
    pub merge_schema_lookup_used: bool,
    /// MERGE fell back to a full scan.
    pub merge_scan_fallback_used: bool,
}

impl HotPathTrace {
    /// Returns `true` if any fast path was taken.
    pub fn any_fast_path(&self) -> bool {
        self.outer_index_top_k
            || self.simple_match_limit_fast_path
            || self.compound_query_fast_path
            || self.traversal_start_seed_top_k
            || self.traversal_end_seed_top_k
            || self.traversal_start_seed_property_in
            || self.unwind_simple_merge_batch
            || self.unwind_fixed_chain_link_batch
            || self.unwind_multi_match_relationship_batch
            || self.call_tail_traversal_fast_path
            || self.merge_schema_lookup_used
    }
}

/// Mutable hot-path trace state, suitable for sharing across threads.
///
/// Internally uses `AtomicBool` fields so marks can be made from any thread
/// without a mutex on the hot path.
#[derive(Debug, Default)]
pub struct HotPathTraceState {
    outer_index_top_k: AtomicBool,
    outer_scan_fallback_used: AtomicBool,
    fabric_batched_apply_rows: AtomicBool,
    simple_match_limit_fast_path: AtomicBool,
    compound_query_fast_path: AtomicBool,
    traversal_start_seed_top_k: AtomicBool,
    traversal_end_seed_top_k: AtomicBool,
    traversal_start_seed_property_in: AtomicBool,
    unwind_simple_merge_batch: AtomicBool,
    unwind_fixed_chain_link_batch: AtomicBool,
    unwind_multi_match_relationship_batch: AtomicBool,
    call_tail_traversal_fast_path: AtomicBool,
    merge_schema_lookup_used: AtomicBool,
    merge_scan_fallback_used: AtomicBool,
}

impl HotPathTraceState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Reset all flags to `false` (call at the start of each query).
    pub fn reset(&self) {
        self.outer_index_top_k.store(false, Ordering::Relaxed);
        self.outer_scan_fallback_used
            .store(false, Ordering::Relaxed);
        self.fabric_batched_apply_rows
            .store(false, Ordering::Relaxed);
        self.simple_match_limit_fast_path
            .store(false, Ordering::Relaxed);
        self.compound_query_fast_path
            .store(false, Ordering::Relaxed);
        self.traversal_start_seed_top_k
            .store(false, Ordering::Relaxed);
        self.traversal_end_seed_top_k
            .store(false, Ordering::Relaxed);
        self.traversal_start_seed_property_in
            .store(false, Ordering::Relaxed);
        self.unwind_simple_merge_batch
            .store(false, Ordering::Relaxed);
        self.unwind_fixed_chain_link_batch
            .store(false, Ordering::Relaxed);
        self.unwind_multi_match_relationship_batch
            .store(false, Ordering::Relaxed);
        self.call_tail_traversal_fast_path
            .store(false, Ordering::Relaxed);
        self.merge_schema_lookup_used
            .store(false, Ordering::Relaxed);
        self.merge_scan_fallback_used
            .store(false, Ordering::Relaxed);
    }

    /// Snapshot the current flags into a `HotPathTrace`.
    pub fn snapshot(&self) -> HotPathTrace {
        HotPathTrace {
            outer_index_top_k: self.outer_index_top_k.load(Ordering::Relaxed),
            outer_scan_fallback_used: self.outer_scan_fallback_used.load(Ordering::Relaxed),
            fabric_batched_apply_rows: self.fabric_batched_apply_rows.load(Ordering::Relaxed),
            simple_match_limit_fast_path: self.simple_match_limit_fast_path.load(Ordering::Relaxed),
            compound_query_fast_path: self.compound_query_fast_path.load(Ordering::Relaxed),
            traversal_start_seed_top_k: self.traversal_start_seed_top_k.load(Ordering::Relaxed),
            traversal_end_seed_top_k: self.traversal_end_seed_top_k.load(Ordering::Relaxed),
            traversal_start_seed_property_in: self
                .traversal_start_seed_property_in
                .load(Ordering::Relaxed),
            unwind_simple_merge_batch: self.unwind_simple_merge_batch.load(Ordering::Relaxed),
            unwind_fixed_chain_link_batch: self
                .unwind_fixed_chain_link_batch
                .load(Ordering::Relaxed),
            unwind_multi_match_relationship_batch: self
                .unwind_multi_match_relationship_batch
                .load(Ordering::Relaxed),
            call_tail_traversal_fast_path: self
                .call_tail_traversal_fast_path
                .load(Ordering::Relaxed),
            merge_schema_lookup_used: self.merge_schema_lookup_used.load(Ordering::Relaxed),
            merge_scan_fallback_used: self.merge_scan_fallback_used.load(Ordering::Relaxed),
        }
    }

    // ── Mark methods (called from executor hot paths) ─────────────────────────

    pub fn mark_outer_index_top_k(&self) {
        self.outer_index_top_k.store(true, Ordering::Relaxed);
    }

    pub fn mark_outer_scan_fallback(&self) {
        self.outer_scan_fallback_used.store(true, Ordering::Relaxed);
    }

    pub fn mark_fabric_batched_apply_rows(&self) {
        self.fabric_batched_apply_rows
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_simple_match_limit_fast_path(&self) {
        self.simple_match_limit_fast_path
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_compound_query_fast_path(&self) {
        self.compound_query_fast_path.store(true, Ordering::Relaxed);
    }

    pub fn mark_traversal_start_seed_top_k(&self) {
        self.traversal_start_seed_top_k
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_traversal_end_seed_top_k(&self) {
        self.traversal_end_seed_top_k.store(true, Ordering::Relaxed);
    }

    pub fn mark_traversal_start_seed_property_in(&self) {
        self.traversal_start_seed_property_in
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_unwind_simple_merge_batch(&self) {
        self.unwind_simple_merge_batch
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_unwind_fixed_chain_link_batch(&self) {
        self.unwind_fixed_chain_link_batch
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_unwind_multi_match_relationship_batch(&self) {
        self.unwind_multi_match_relationship_batch
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_call_tail_traversal_fast_path(&self) {
        self.call_tail_traversal_fast_path
            .store(true, Ordering::Relaxed);
    }

    pub fn mark_merge_schema_lookup(&self) {
        self.merge_schema_lookup_used.store(true, Ordering::Relaxed);
    }

    pub fn mark_merge_scan_fallback(&self) {
        self.merge_scan_fallback_used.store(true, Ordering::Relaxed);
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hot_path_trace_default() {
        let trace = HotPathTrace::default();
        assert!(!trace.any_fast_path());
    }

    #[test]
    fn test_hot_path_trace_state_reset() {
        let state = HotPathTraceState::new();
        state.mark_outer_index_top_k();
        assert!(state.snapshot().outer_index_top_k);
        state.reset();
        assert!(!state.snapshot().outer_index_top_k);
    }

    #[test]
    fn test_hot_path_trace_marks() {
        let state = HotPathTraceState::new();
        state.mark_simple_match_limit_fast_path();
        let snap = state.snapshot();
        assert!(snap.simple_match_limit_fast_path);
        assert!(snap.any_fast_path());
    }

    #[test]
    fn test_hot_path_trace_compound_query() {
        let state = HotPathTraceState::new();
        state.mark_compound_query_fast_path();
        let snap = state.snapshot();
        assert!(snap.compound_query_fast_path);
    }

    #[test]
    fn test_hot_path_trace_merge_paths() {
        let state = HotPathTraceState::new();
        state.mark_merge_schema_lookup();
        let snap = state.snapshot();
        assert!(snap.merge_schema_lookup_used);
        assert!(!snap.merge_scan_fallback_used);

        state.mark_merge_scan_fallback();
        let snap2 = state.snapshot();
        assert!(snap2.merge_scan_fallback_used);
    }

    #[test]
    fn test_hot_path_trace_all_marks() {
        let state = HotPathTraceState::new();
        state.mark_outer_index_top_k();
        state.mark_outer_scan_fallback();
        state.mark_fabric_batched_apply_rows();
        state.mark_simple_match_limit_fast_path();
        state.mark_compound_query_fast_path();
        state.mark_traversal_start_seed_top_k();
        state.mark_traversal_end_seed_top_k();
        state.mark_unwind_simple_merge_batch();
        state.mark_unwind_fixed_chain_link_batch();
        state.mark_call_tail_traversal_fast_path();
        state.mark_merge_schema_lookup();
        state.mark_merge_scan_fallback();

        let snap = state.snapshot();
        assert!(snap.outer_index_top_k);
        assert!(snap.outer_scan_fallback_used);
        assert!(snap.fabric_batched_apply_rows);
        assert!(snap.simple_match_limit_fast_path);
        assert!(snap.compound_query_fast_path);
        assert!(snap.traversal_start_seed_top_k);
        assert!(snap.traversal_end_seed_top_k);
        assert!(snap.unwind_simple_merge_batch);
        assert!(snap.unwind_fixed_chain_link_batch);
        assert!(snap.call_tail_traversal_fast_path);
        assert!(snap.merge_schema_lookup_used);
        assert!(snap.merge_scan_fallback_used);
        assert!(snap.any_fast_path());
    }
}
