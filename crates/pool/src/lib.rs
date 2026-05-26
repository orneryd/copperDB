//! Allocation-conscious resource pools for query execution scratch objects.
//!
//! Equivalent to NornicDB's `pkg/pool`: reusable rows, nodes, maps, byte
//! buffers, string builders, and small slices. These pools own no durable state;
//! they reduce allocation pressure on hot query paths.

use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::OnceLock;

pub const DEFAULT_MAX_SIZE: usize = 1000;
pub const DEFAULT_ROW_CAPACITY: usize = 64;
pub const DEFAULT_NODE_CAPACITY: usize = 64;
pub const DEFAULT_BYTE_CAPACITY: usize = 1024;
pub const DEFAULT_BUILDER_CAPACITY: usize = 256;
pub const DEFAULT_STRING_SLICE_CAPACITY: usize = 16;
pub const DEFAULT_VALUE_SLICE_CAPACITY: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    pub enabled: bool,
    pub max_size: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: DEFAULT_MAX_SIZE,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PooledNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Default)]
pub struct PooledStringBuilder {
    buffer: Vec<u8>,
}

impl PooledStringBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
        }
    }

    pub fn write_str(&mut self, value: &str) {
        self.buffer.extend_from_slice(value.as_bytes());
    }

    pub fn write_byte(&mut self, value: u8) {
        self.buffer.push(value);
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    pub fn into_string(&self) -> String {
        String::from_utf8_lossy(&self.buffer).into_owned()
    }
}

#[derive(Default)]
struct PoolState {
    config: PoolConfig,
    row_slices: BoundedPool<Vec<Vec<Value>>>,
    node_slices: BoundedPool<Vec<PooledNode>>,
    byte_buffers: BoundedPool<Vec<u8>>,
    string_builders: BoundedPool<PooledStringBuilder>,
    maps: BoundedPool<BTreeMap<String, Value>>,
    string_slices: BoundedPool<Vec<String>>,
    value_slices: BoundedPool<Vec<Value>>,
}

#[derive(Debug)]
struct BoundedPool<T> {
    entries: VecDeque<T>,
}

impl<T> Default for BoundedPool<T> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }
}

impl<T> BoundedPool<T> {
    fn get_or_else(&mut self, factory: impl FnOnce() -> T) -> T {
        self.entries.pop_front().unwrap_or_else(factory)
    }

    fn put(&mut self, value: T, max_size: usize) {
        if self.entries.len() < max_size {
            self.entries.push_back(value);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

static STATE: OnceLock<Mutex<PoolState>> = OnceLock::new();

fn state() -> &'static Mutex<PoolState> {
    STATE.get_or_init(|| Mutex::new(PoolState::default()))
}

pub fn configure(config: PoolConfig) {
    let mut state = state().lock();
    state.config = PoolConfig {
        enabled: config.enabled,
        max_size: config.max_size.max(1),
    };
    state.row_slices.clear();
    state.node_slices.clear();
    state.byte_buffers.clear();
    state.string_builders.clear();
    state.maps.clear();
    state.string_slices.clear();
    state.value_slices.clear();
}

pub fn current_config() -> PoolConfig {
    state().lock().config
}

pub fn is_enabled() -> bool {
    state().lock().config.enabled
}

pub fn get_row_slice() -> Vec<Vec<Value>> {
    let mut state = state().lock();
    if !state.config.enabled {
        return Vec::with_capacity(DEFAULT_ROW_CAPACITY);
    }
    state
        .row_slices
        .get_or_else(|| Vec::with_capacity(DEFAULT_ROW_CAPACITY))
}

pub fn put_row_slice(mut rows: Vec<Vec<Value>>) {
    let mut state = state().lock();
    if !state.config.enabled || rows.capacity() > state.config.max_size {
        return;
    }
    for row in &mut rows {
        row.clear();
    }
    rows.clear();
    let max_size = state.config.max_size;
    state.row_slices.put(rows, max_size);
}

pub fn get_node_slice() -> Vec<PooledNode> {
    let mut state = state().lock();
    if !state.config.enabled {
        return Vec::with_capacity(DEFAULT_NODE_CAPACITY);
    }
    state
        .node_slices
        .get_or_else(|| Vec::with_capacity(DEFAULT_NODE_CAPACITY))
}

pub fn put_node_slice(mut nodes: Vec<PooledNode>) {
    let mut state = state().lock();
    if !state.config.enabled || nodes.capacity() > state.config.max_size {
        return;
    }
    nodes.clear();
    let max_size = state.config.max_size;
    state.node_slices.put(nodes, max_size);
}

pub fn get_string_builder() -> PooledStringBuilder {
    let mut state = state().lock();
    if !state.config.enabled {
        return PooledStringBuilder::with_capacity(DEFAULT_BUILDER_CAPACITY);
    }
    let mut builder = state
        .string_builders
        .get_or_else(|| PooledStringBuilder::with_capacity(DEFAULT_BUILDER_CAPACITY));
    builder.clear();
    builder
}

pub fn put_string_builder(mut builder: PooledStringBuilder) {
    let mut state = state().lock();
    if !state.config.enabled || builder.capacity() > state.config.max_size {
        return;
    }
    builder.clear();
    let max_size = state.config.max_size;
    state.string_builders.put(builder, max_size);
}

pub fn get_byte_buffer() -> Vec<u8> {
    let mut state = state().lock();
    if !state.config.enabled {
        return Vec::with_capacity(DEFAULT_BYTE_CAPACITY);
    }
    let mut buffer = state
        .byte_buffers
        .get_or_else(|| Vec::with_capacity(DEFAULT_BYTE_CAPACITY));
    buffer.clear();
    buffer
}

pub fn put_byte_buffer(mut buffer: Vec<u8>) {
    let mut state = state().lock();
    if !state.config.enabled || buffer.capacity() > state.config.max_size {
        return;
    }
    buffer.clear();
    let max_size = state.config.max_size;
    state.byte_buffers.put(buffer, max_size);
}

pub fn get_map() -> BTreeMap<String, Value> {
    let mut state = state().lock();
    if !state.config.enabled {
        return BTreeMap::new();
    }
    let mut map = state.maps.get_or_else(BTreeMap::new);
    map.clear();
    map
}

pub fn put_map(mut map: BTreeMap<String, Value>) {
    let mut state = state().lock();
    if !state.config.enabled || map.len() > state.config.max_size {
        return;
    }
    map.clear();
    let max_size = state.config.max_size;
    state.maps.put(map, max_size);
}

pub fn get_string_slice() -> Vec<String> {
    let mut state = state().lock();
    if !state.config.enabled {
        return Vec::with_capacity(DEFAULT_STRING_SLICE_CAPACITY);
    }
    state
        .string_slices
        .get_or_else(|| Vec::with_capacity(DEFAULT_STRING_SLICE_CAPACITY))
}

pub fn put_string_slice(mut values: Vec<String>) {
    let mut state = state().lock();
    if !state.config.enabled || values.capacity() > state.config.max_size {
        return;
    }
    values.clear();
    let max_size = state.config.max_size;
    state.string_slices.put(values, max_size);
}

pub fn get_value_slice() -> Vec<Value> {
    let mut state = state().lock();
    if !state.config.enabled {
        return Vec::with_capacity(DEFAULT_VALUE_SLICE_CAPACITY);
    }
    state
        .value_slices
        .get_or_else(|| Vec::with_capacity(DEFAULT_VALUE_SLICE_CAPACITY))
}

pub fn put_value_slice(mut values: Vec<Value>) {
    let mut state = state().lock();
    if !state.config.enabled || values.capacity() > state.config.max_size {
        return;
    }
    values.clear();
    let max_size = state.config.max_size;
    state.value_slices.put(values, max_size);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn reset_pool() {
        configure(PoolConfig::default());
    }

    #[test]
    fn configure_controls_enabled_and_size() {
        configure(PoolConfig {
            enabled: false,
            max_size: 50,
        });

        assert!(!is_enabled());
        assert_eq!(current_config().max_size, 50);

        reset_pool();
    }

    #[test]
    fn row_slice_round_trips_empty_and_cleared() {
        reset_pool();
        let mut rows = get_row_slice();
        assert!(rows.is_empty());
        rows.push(vec![Value::from(1), Value::from("name")]);
        put_row_slice(rows);

        let rows = get_row_slice();
        assert!(rows.is_empty());
        assert!(rows.capacity() >= DEFAULT_ROW_CAPACITY);
        put_row_slice(rows);
    }

    #[test]
    fn node_slice_round_trips_empty_and_cleared() {
        reset_pool();
        let mut nodes = get_node_slice();
        nodes.push(PooledNode {
            id: "n1".to_string(),
            labels: vec!["User".to_string()],
            properties: BTreeMap::new(),
        });
        put_node_slice(nodes);

        let nodes = get_node_slice();
        assert!(nodes.is_empty());
        put_node_slice(nodes);
    }

    #[test]
    fn string_builder_round_trips_reset() {
        reset_pool();
        let mut builder = get_string_builder();
        builder.write_str("hello");
        builder.write_byte(b' ');
        builder.write_str("world");
        assert_eq!(builder.into_string(), "hello world");
        put_string_builder(builder);

        let builder = get_string_builder();
        assert!(builder.is_empty());
        put_string_builder(builder);
    }

    #[test]
    fn byte_buffer_round_trips_reset() {
        reset_pool();
        let mut buffer = get_byte_buffer();
        buffer.extend_from_slice(b"test data");
        put_byte_buffer(buffer);

        let buffer = get_byte_buffer();
        assert!(buffer.is_empty());
        assert!(buffer.capacity() >= DEFAULT_BYTE_CAPACITY);
        put_byte_buffer(buffer);
    }

    #[test]
    fn map_round_trips_cleared() {
        reset_pool();
        let mut map = get_map();
        map.insert("key".to_string(), Value::from("value"));
        put_map(map);

        let map = get_map();
        assert!(map.is_empty());
        put_map(map);
    }

    #[test]
    fn string_slice_round_trips_cleared() {
        reset_pool();
        let mut values = get_string_slice();
        values.push("hello".to_string());
        values.push("world".to_string());
        put_string_slice(values);

        let values = get_string_slice();
        assert!(values.is_empty());
        put_string_slice(values);
    }

    #[test]
    fn value_slice_round_trips_cleared() {
        reset_pool();
        let mut values = get_value_slice();
        values.push(Value::from(1));
        values.push(Value::from("two"));
        put_value_slice(values);

        let values = get_value_slice();
        assert!(values.is_empty());
        put_value_slice(values);
    }

    #[test]
    fn disabled_pool_returns_fresh_objects_and_ignores_puts() {
        configure(PoolConfig {
            enabled: false,
            max_size: DEFAULT_MAX_SIZE,
        });

        let mut buffer = get_byte_buffer();
        buffer.extend_from_slice(b"disabled");
        put_byte_buffer(buffer);

        assert!(get_byte_buffer().is_empty());
        reset_pool();
    }

    #[test]
    fn oversized_objects_are_not_pooled() {
        configure(PoolConfig {
            enabled: true,
            max_size: 3,
        });

        put_string_slice(vec!["x".to_string(); 20]);
        put_value_slice(vec![Value::from(1); 20]);
        put_byte_buffer(vec![0; 20]);
        put_map(BTreeMap::from([
            ("a".to_string(), Value::from(1)),
            ("b".to_string(), Value::from(2)),
            ("c".to_string(), Value::from(3)),
            ("d".to_string(), Value::from(4)),
        ]));

        reset_pool();
    }

    #[test]
    fn concurrent_pool_access_is_safe() {
        reset_pool();
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let mut handles = Vec::new();

        for worker_id in 0..16 {
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for iteration in 0..100 {
                    let mut map = get_map();
                    map.insert("worker".to_string(), Value::from(worker_id));
                    map.insert("iteration".to_string(), Value::from(iteration));
                    put_map(map);

                    let mut builder = get_string_builder();
                    builder.write_str("query");
                    put_string_builder(builder);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("pool worker panicked");
        }
    }
}
