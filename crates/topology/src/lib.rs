//! Foundational cluster topology contracts for copperdb.
//!
//! This crate is intentionally dependency-light so storage, search,
//! replication, and fabric can all share the same vocabulary for hyperscaler
//! placement, distributed search fan-out, and high-availability write planning.
//! It does not execute distributed operations yet; it validates and plans them.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TopologyError {
    #[error("missing peer: {0}")]
    MissingPeer(String),
    #[error("missing placement: {0}")]
    MissingPlacement(String),
    #[error("invalid topology: {0}")]
    InvalidTopology(String),
    #[error("no healthy peer for capability {0:?}")]
    NoHealthyPeer(NodeCapability),
}

pub const DEFAULT_SEARCH_HEDGE_AFTER_MICROS: u32 = 5_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalTransactionId {
    pub epoch: u64,
    pub counter: u64,
    pub node_ordinal: u32,
}

impl LogicalTransactionId {
    pub const ZERO: Self = Self {
        epoch: 0,
        counter: 0,
        node_ordinal: 0,
    };

    pub fn new(epoch: u64, counter: u64, node_ordinal: u32) -> Self {
        Self {
            epoch,
            counter,
            node_ordinal,
        }
    }

    pub fn stable_id(&self) -> String {
        format!(
            "{:016x}:{:016x}:{:08x}",
            self.epoch, self.counter, self.node_ordinal
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalTransactionRange {
    pub first: LogicalTransactionId,
    pub last: LogicalTransactionId,
    pub len: u64,
}

#[derive(Debug)]
pub struct DistributedTransactionClock {
    node_ordinal: u32,
    epoch: AtomicU64,
    counter: AtomicU64,
}

impl DistributedTransactionClock {
    pub fn new(node_ordinal: u32) -> Self {
        Self::with_epoch(node_ordinal, 1)
    }

    pub fn with_epoch(node_ordinal: u32, epoch: u64) -> Self {
        Self {
            node_ordinal,
            epoch: AtomicU64::new(epoch.max(1)),
            counter: AtomicU64::new(0),
        }
    }

    pub fn node_ordinal(&self) -> u32 {
        self.node_ordinal
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn issue(&self) -> LogicalTransactionId {
        let counter = self.counter.fetch_add(1, Ordering::AcqRel) + 1;
        LogicalTransactionId::new(self.epoch(), counter, self.node_ordinal)
    }

    pub fn reserve(&self, len: u64) -> LogicalTransactionRange {
        let len = len.max(1);
        let first_counter = self.counter.fetch_add(len, Ordering::AcqRel) + 1;
        LogicalTransactionRange {
            first: LogicalTransactionId::new(self.epoch(), first_counter, self.node_ordinal),
            last: LogicalTransactionId::new(
                self.epoch(),
                first_counter + len - 1,
                self.node_ordinal,
            ),
            len,
        }
    }

    pub fn observe(&self, remote: LogicalTransactionId) -> LogicalTransactionId {
        self.advance_epoch(remote.epoch);
        advance_atomic_at_least(&self.counter, remote.counter);
        self.issue()
    }

    pub fn advance_epoch(&self, epoch: u64) {
        advance_atomic_at_least(&self.epoch, epoch.max(1));
    }
}

fn advance_atomic_at_least(target: &AtomicU64, floor: u64) {
    let mut current = target.load(Ordering::Acquire);
    while current < floor {
        match target.compare_exchange(current, floor, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HyperscalerProvider {
    Aws,
    Azure,
    Gcp,
    Local,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HyperscalerProfile {
    pub profile_id: String,
    pub provider: HyperscalerProvider,
    pub region: String,
    pub zones: Vec<String>,
    pub tier: String,
    pub enabled: bool,
    pub metadata: BTreeMap<String, String>,
}

impl HyperscalerProfile {
    pub fn local(profile_id: impl Into<String>) -> Self {
        Self {
            profile_id: profile_id.into(),
            provider: HyperscalerProvider::Local,
            region: "local".into(),
            zones: vec!["local-a".into()],
            tier: "local".into(),
            enabled: true,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeCapability {
    Http,
    Bolt,
    Storage,
    Search,
    WriteLeader,
    WriteReplica,
    Coordinator,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerHealth {
    Healthy,
    Degraded,
    Draining,
    Unreachable,
}

impl PeerHealth {
    pub fn can_serve_reads(&self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    pub fn can_accept_writes(&self) -> bool {
        matches!(self, Self::Healthy)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshPeer {
    pub node_id: String,
    pub advertise_addr: String,
    pub region: String,
    pub zone: String,
    pub capabilities: BTreeSet<NodeCapability>,
    pub health: PeerHealth,
    pub hyperscaler_profile: Option<String>,
    pub last_heartbeat_unix_ms: i64,
    pub observed_rtt_micros: u32,
    pub inflight_requests: u32,
    pub capacity_weight: u16,
}

impl MeshPeer {
    pub fn new(node_id: impl Into<String>, advertise_addr: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            advertise_addr: advertise_addr.into(),
            region: "local".into(),
            zone: "local-a".into(),
            capabilities: BTreeSet::new(),
            health: PeerHealth::Healthy,
            hyperscaler_profile: None,
            last_heartbeat_unix_ms: 0,
            observed_rtt_micros: 1_000,
            inflight_requests: 0,
            capacity_weight: 1,
        }
    }

    pub fn with_capability(mut self, capability: NodeCapability) -> Self {
        self.capabilities.insert(capability);
        self
    }

    pub fn with_region_zone(mut self, region: impl Into<String>, zone: impl Into<String>) -> Self {
        self.region = region.into();
        self.zone = zone.into();
        self
    }

    pub fn with_hyperscaler_profile(mut self, profile_id: impl Into<String>) -> Self {
        self.hyperscaler_profile = Some(profile_id.into());
        self
    }

    pub fn with_observed_rtt_micros(mut self, observed_rtt_micros: u32) -> Self {
        self.observed_rtt_micros = observed_rtt_micros.max(1);
        self
    }

    pub fn with_load(mut self, inflight_requests: u32, capacity_weight: u16) -> Self {
        self.inflight_requests = inflight_requests;
        self.capacity_weight = capacity_weight.max(1);
        self
    }

    pub fn can_serve(&self, capability: &NodeCapability) -> bool {
        self.capabilities.contains(capability)
            && match capability {
                NodeCapability::WriteLeader => self.health.can_accept_writes(),
                _ => self.health.can_serve_reads(),
            }
    }

    pub fn search_cost(&self, policy: &SearchRoutingPolicy) -> u64 {
        let locality_penalty = if policy.prefer_same_region {
            match &policy.request_region {
                Some(region) if region == &self.region => 0,
                Some(_) => policy.cross_region_penalty_micros as u64,
                None => 0,
            }
        } else {
            0
        };
        let load_penalty =
            (self.inflight_requests as u64 * 1_000) / self.capacity_weight.max(1) as u64;
        self.observed_rtt_micros as u64 + locality_penalty + load_penalty
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementKey {
    pub tenant: String,
    pub database: String,
    pub shard: String,
}

impl PlacementKey {
    pub fn new(
        tenant: impl Into<String>,
        database: impl Into<String>,
        shard: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            database: database.into(),
            shard: shard.into(),
        }
    }

    pub fn default_for_database(database: impl Into<String>) -> Self {
        Self::new("default", database, "primary")
    }

    pub fn stable_id(&self) -> String {
        format!("{}/{}/{}", self.tenant, self.database, self.shard)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacementRecord {
    pub key: PlacementKey,
    pub primary_node: String,
    pub replica_nodes: Vec<String>,
    pub search_nodes: Vec<String>,
    pub hyperscaler_profile: Option<String>,
    pub min_write_replicas: usize,
    pub search_fanout: usize,
}

impl PlacementRecord {
    pub fn standalone(database: impl Into<String>, node_id: impl Into<String>) -> Self {
        let node_id = node_id.into();
        Self {
            key: PlacementKey::default_for_database(database),
            primary_node: node_id.clone(),
            replica_nodes: Vec::new(),
            search_nodes: vec![node_id],
            hyperscaler_profile: None,
            min_write_replicas: 0,
            search_fanout: 1,
        }
    }

    pub fn participant_ids(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        out.insert(self.primary_node.clone());
        out.extend(self.replica_nodes.iter().cloned());
        out.extend(self.search_nodes.iter().cloned());
        out
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistributedWriteMode {
    Standalone,
    LeaderLease,
    Quorum,
    RaftLog,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchRoutingPolicy {
    pub request_region: Option<String>,
    pub max_fanout: usize,
    pub prefer_same_region: bool,
    pub include_degraded: bool,
    pub hedge_after_micros: u32,
    pub cross_region_penalty_micros: u32,
}

impl SearchRoutingPolicy {
    pub fn low_latency(request_region: impl Into<String>, max_fanout: usize) -> Self {
        Self {
            request_region: Some(request_region.into()),
            max_fanout: max_fanout.max(1),
            prefer_same_region: true,
            include_degraded: true,
            hedge_after_micros: DEFAULT_SEARCH_HEDGE_AFTER_MICROS,
            cross_region_penalty_micros: 25_000,
        }
    }

    pub fn global(max_fanout: usize) -> Self {
        Self {
            request_region: None,
            max_fanout: max_fanout.max(1),
            prefer_same_region: false,
            include_degraded: true,
            hedge_after_micros: DEFAULT_SEARCH_HEDGE_AFTER_MICROS,
            cross_region_penalty_micros: 0,
        }
    }
}

impl Default for SearchRoutingPolicy {
    fn default() -> Self {
        Self::global(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedSearchPlan {
    pub placement: PlacementKey,
    pub fanout: Vec<MeshPeer>,
    pub policy: SearchRoutingPolicy,
    pub parallelism: usize,
    pub hedge_after_micros: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedWritePlan {
    pub placement: PlacementKey,
    pub mode: DistributedWriteMode,
    pub leader: MeshPeer,
    pub replicas: Vec<MeshPeer>,
    pub required_acks: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyRegistry {
    peers: BTreeMap<String, MeshPeer>,
    placements: BTreeMap<PlacementKey, PlacementRecord>,
    hyperscaler_profiles: BTreeMap<String, HyperscalerProfile>,
}

impl TopologyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_hyperscaler_profile(
        &mut self,
        profile: HyperscalerProfile,
    ) -> Result<(), TopologyError> {
        if profile.profile_id.trim().is_empty() {
            return Err(TopologyError::InvalidTopology(
                "hyperscaler profile id must not be empty".into(),
            ));
        }
        self.hyperscaler_profiles
            .insert(profile.profile_id.clone(), profile);
        Ok(())
    }

    pub fn register_peer(&mut self, peer: MeshPeer) -> Result<(), TopologyError> {
        if peer.node_id.trim().is_empty() {
            return Err(TopologyError::InvalidTopology(
                "peer node id must not be empty".into(),
            ));
        }
        if let Some(profile) = &peer.hyperscaler_profile {
            if !self.hyperscaler_profiles.contains_key(profile) {
                return Err(TopologyError::MissingPeer(format!(
                    "hyperscaler profile {profile}"
                )));
            }
        }
        self.peers.insert(peer.node_id.clone(), peer);
        Ok(())
    }

    pub fn register_placement(&mut self, placement: PlacementRecord) -> Result<(), TopologyError> {
        for peer_id in placement.participant_ids() {
            if !self.peers.contains_key(&peer_id) {
                return Err(TopologyError::MissingPeer(peer_id));
            }
        }
        if let Some(profile) = &placement.hyperscaler_profile {
            if !self.hyperscaler_profiles.contains_key(profile) {
                return Err(TopologyError::InvalidTopology(format!(
                    "placement references unknown hyperscaler profile {profile}"
                )));
            }
        }
        self.placements.insert(placement.key.clone(), placement);
        Ok(())
    }

    pub fn peer(&self, node_id: &str) -> Option<&MeshPeer> {
        self.peers.get(node_id)
    }

    pub fn placement(&self, key: &PlacementKey) -> Option<&PlacementRecord> {
        self.placements.get(key)
    }

    pub fn peers(&self) -> Vec<&MeshPeer> {
        self.peers.values().collect()
    }

    pub fn placements(&self) -> Vec<&PlacementRecord> {
        self.placements.values().collect()
    }

    pub fn healthy_peers_with(&self, capability: NodeCapability) -> Vec<&MeshPeer> {
        self.peers
            .values()
            .filter(|peer| peer.can_serve(&capability))
            .collect()
    }

    pub fn plan_search(&self, key: &PlacementKey) -> Result<DistributedSearchPlan, TopologyError> {
        let placement = self
            .placements
            .get(key)
            .ok_or_else(|| TopologyError::MissingPlacement(key.stable_id()))?;
        let policy = SearchRoutingPolicy::global(placement.search_fanout);
        self.plan_search_with_policy(key, policy)
    }

    pub fn plan_search_with_policy(
        &self,
        key: &PlacementKey,
        policy: SearchRoutingPolicy,
    ) -> Result<DistributedSearchPlan, TopologyError> {
        let placement = self
            .placements
            .get(key)
            .ok_or_else(|| TopologyError::MissingPlacement(key.stable_id()))?;

        let target_fanout = placement.search_fanout.max(1).min(policy.max_fanout.max(1));
        let mut candidates = Vec::with_capacity(placement.search_nodes.len());
        for node_id in &placement.search_nodes {
            let peer = self
                .peers
                .get(node_id)
                .ok_or_else(|| TopologyError::MissingPeer(node_id.clone()))?;
            let health_ok = if policy.include_degraded {
                peer.health.can_serve_reads()
            } else {
                peer.health == PeerHealth::Healthy
            };
            if peer.capabilities.contains(&NodeCapability::Search) && health_ok {
                candidates.push((peer.search_cost(&policy), peer.node_id.clone(), peer));
            }
        }

        candidates.sort_unstable_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        let fanout = candidates
            .into_iter()
            .take(target_fanout)
            .map(|(_, _, peer)| peer.clone())
            .collect::<Vec<_>>();

        if fanout.is_empty() {
            return Err(TopologyError::NoHealthyPeer(NodeCapability::Search));
        }

        let parallelism = fanout.len();
        let hedge_after_micros = policy.hedge_after_micros;

        Ok(DistributedSearchPlan {
            placement: key.clone(),
            fanout,
            policy,
            parallelism,
            hedge_after_micros,
        })
    }

    pub fn plan_write(
        &self,
        key: &PlacementKey,
        mode: DistributedWriteMode,
    ) -> Result<DistributedWritePlan, TopologyError> {
        let placement = self
            .placements
            .get(key)
            .ok_or_else(|| TopologyError::MissingPlacement(key.stable_id()))?;
        let leader = self
            .peers
            .get(&placement.primary_node)
            .ok_or_else(|| TopologyError::MissingPeer(placement.primary_node.clone()))?;
        if !leader.can_serve(&NodeCapability::WriteLeader) {
            return Err(TopologyError::NoHealthyPeer(NodeCapability::WriteLeader));
        }

        let replicas = placement
            .replica_nodes
            .iter()
            .filter_map(|node_id| self.peers.get(node_id))
            .filter(|peer| peer.can_serve(&NodeCapability::WriteReplica))
            .cloned()
            .collect::<Vec<_>>();

        let required_acks = match mode {
            DistributedWriteMode::Standalone => 1,
            DistributedWriteMode::LeaderLease => 1 + placement.min_write_replicas,
            DistributedWriteMode::Quorum | DistributedWriteMode::RaftLog => {
                let voters = 1 + placement.replica_nodes.len();
                voters / 2 + 1
            }
        };

        if 1 + replicas.len() < required_acks {
            return Err(TopologyError::InvalidTopology(format!(
                "write plan requires {required_acks} acknowledgements but only {} healthy participants are available",
                1 + replicas.len()
            )));
        }

        Ok(DistributedWritePlan {
            placement: key.clone(),
            mode,
            leader: leader.clone(),
            replicas,
            required_acks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, capabilities: &[NodeCapability]) -> MeshPeer {
        let mut peer = MeshPeer::new(id, format!("{id}.mesh.local:9000"));
        for capability in capabilities {
            peer.capabilities.insert(capability.clone());
        }
        peer
    }

    #[test]
    fn search_plan_uses_healthy_search_fanout() {
        let mut registry = TopologyRegistry::new();
        registry
            .register_peer(peer(
                "n1",
                &[
                    NodeCapability::Storage,
                    NodeCapability::Search,
                    NodeCapability::WriteLeader,
                ],
            ))
            .unwrap();
        registry
            .register_peer(peer(
                "n2",
                &[NodeCapability::Search, NodeCapability::WriteReplica],
            ))
            .unwrap();
        registry
            .register_placement(PlacementRecord {
                key: PlacementKey::default_for_database("neo4j"),
                primary_node: "n1".into(),
                replica_nodes: vec!["n2".into()],
                search_nodes: vec!["n1".into(), "n2".into()],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 2,
            })
            .unwrap();

        let plan = registry
            .plan_search(&PlacementKey::default_for_database("neo4j"))
            .unwrap();
        assert_eq!(plan.fanout.len(), 2);
        assert_eq!(plan.fanout[0].node_id, "n1");
        assert_eq!(plan.fanout[1].node_id, "n2");
    }

    #[test]
    fn quorum_write_plan_requires_majority() {
        let mut registry = TopologyRegistry::new();
        registry
            .register_peer(peer("n1", &[NodeCapability::WriteLeader]))
            .unwrap();
        registry
            .register_peer(peer("n2", &[NodeCapability::WriteReplica]))
            .unwrap();
        registry
            .register_peer(peer("n3", &[NodeCapability::WriteReplica]))
            .unwrap();
        registry
            .register_placement(PlacementRecord {
                key: PlacementKey::default_for_database("neo4j"),
                primary_node: "n1".into(),
                replica_nodes: vec!["n2".into(), "n3".into()],
                search_nodes: vec![],
                hyperscaler_profile: None,
                min_write_replicas: 1,
                search_fanout: 1,
            })
            .unwrap();

        let plan = registry
            .plan_write(
                &PlacementKey::default_for_database("neo4j"),
                DistributedWriteMode::Quorum,
            )
            .unwrap();
        assert_eq!(plan.required_acks, 2);
        assert_eq!(plan.replicas.len(), 2);
    }

    #[test]
    fn hyperscaler_profiles_are_validated_before_use() {
        let mut registry = TopologyRegistry::new();
        let mut cloud_peer = peer("n1", &[NodeCapability::Storage]);
        cloud_peer.hyperscaler_profile = Some("aws-prod".into());
        assert!(matches!(
            registry.register_peer(cloud_peer.clone()),
            Err(TopologyError::MissingPeer(_))
        ));

        registry
            .register_hyperscaler_profile(HyperscalerProfile {
                profile_id: "aws-prod".into(),
                provider: HyperscalerProvider::Aws,
                region: "us-east-1".into(),
                zones: vec!["us-east-1a".into(), "us-east-1b".into()],
                tier: "production".into(),
                enabled: true,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        registry.register_peer(cloud_peer).unwrap();
    }

    #[test]
    fn write_plan_rejects_unhealthy_leader() {
        let mut registry = TopologyRegistry::new();
        let mut leader = peer("n1", &[NodeCapability::WriteLeader]);
        leader.health = PeerHealth::Degraded;
        registry.register_peer(leader).unwrap();
        registry
            .register_placement(PlacementRecord::standalone("neo4j", "n1"))
            .unwrap();

        let error = registry
            .plan_write(
                &PlacementKey::default_for_database("neo4j"),
                DistributedWriteMode::Standalone,
            )
            .unwrap_err();
        assert_eq!(
            error,
            TopologyError::NoHealthyPeer(NodeCapability::WriteLeader)
        );
    }

    #[test]
    fn search_policy_prefers_local_low_cost_peers() {
        let mut registry = TopologyRegistry::new();
        registry
            .register_peer(
                peer("remote-fast", &[NodeCapability::Search])
                    .with_region_zone("eu-west-1", "eu-west-1a")
                    .with_observed_rtt_micros(500)
                    .with_load(0, 8),
            )
            .unwrap();
        registry
            .register_peer(
                peer("local-healthy", &[NodeCapability::Search])
                    .with_region_zone("us-east-1", "us-east-1a")
                    .with_observed_rtt_micros(1_500)
                    .with_load(0, 8),
            )
            .unwrap();
        registry
            .register_peer(
                peer("local-busy", &[NodeCapability::Search])
                    .with_region_zone("us-east-1", "us-east-1b")
                    .with_observed_rtt_micros(1_000)
                    .with_load(100, 1),
            )
            .unwrap();
        registry
            .register_placement(PlacementRecord {
                key: PlacementKey::default_for_database("neo4j"),
                primary_node: "local-healthy".into(),
                replica_nodes: vec![],
                search_nodes: vec![
                    "remote-fast".into(),
                    "local-busy".into(),
                    "local-healthy".into(),
                ],
                hyperscaler_profile: None,
                min_write_replicas: 0,
                search_fanout: 3,
            })
            .unwrap();

        let plan = registry
            .plan_search_with_policy(
                &PlacementKey::default_for_database("neo4j"),
                SearchRoutingPolicy::low_latency("us-east-1", 2),
            )
            .unwrap();

        assert_eq!(plan.parallelism, 2);
        assert_eq!(plan.fanout[0].node_id, "local-healthy");
        assert_eq!(plan.fanout[1].node_id, "remote-fast");
        assert_eq!(plan.hedge_after_micros, DEFAULT_SEARCH_HEDGE_AFTER_MICROS);
    }

    #[test]
    fn distributed_transaction_clock_issues_unique_ordered_ids_without_wall_time() {
        let clock = DistributedTransactionClock::with_epoch(7, 42);
        let first = clock.issue();
        let second = clock.issue();

        assert_eq!(first.epoch, 42);
        assert_eq!(first.node_ordinal, 7);
        assert!(first < second);
        assert_eq!(
            first.stable_id(),
            "000000000000002a:0000000000000001:00000007"
        );
    }

    #[test]
    fn distributed_transaction_clock_reserves_ranges_for_batch_writers() {
        let clock = DistributedTransactionClock::new(3);
        let range = clock.reserve(4);
        let next = clock.issue();

        assert_eq!(range.first.counter, 1);
        assert_eq!(range.last.counter, 4);
        assert_eq!(range.len, 4);
        assert_eq!(next.counter, 5);
    }

    #[test]
    fn distributed_transaction_clock_merges_peer_observations() {
        let local = DistributedTransactionClock::with_epoch(1, 2);
        let remote = LogicalTransactionId::new(9, 100, 8);
        let after_observe = local.observe(remote);
        let next = local.issue();

        assert_eq!(after_observe.epoch, 9);
        assert_eq!(after_observe.counter, 101);
        assert_eq!(after_observe.node_ordinal, 1);
        assert_eq!(next.counter, 102);
        assert!(remote < after_observe);
    }
}
