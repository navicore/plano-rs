//!
//! Cache stats
//!

use metrics::{counter, gauge, Counter, Gauge};
use ocra::stats::{CacheCapacityStats, CacheReadStats, CacheStats};
use std::{
    fmt::Debug,
    sync::atomic::{AtomicU64, Ordering},
};
use tracing::warn;

use std::sync::LazyLock;
static TOTAL_READS: LazyLock<Counter> = LazyLock::new(|| counter!("object_store_total_reads"));
static TOTAL_MISSES: LazyLock<Counter> = LazyLock::new(|| counter!("object_store_total_misses"));
static CAPACITY: LazyLock<Gauge> = LazyLock::new(|| gauge!("object_store_capacity"));
static USAGE: LazyLock<Gauge> = LazyLock::new(|| gauge!("object_store_usage"));

#[derive(Debug)]
pub struct AtomicIntCacheStats {
    total_reads: AtomicU64,
    total_misses: AtomicU64,
    max_capacity: AtomicU64,
    capacity_usage: AtomicU64,
}

impl AtomicIntCacheStats {
    pub const fn new() -> Self {
        Self {
            total_misses: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            max_capacity: AtomicU64::new(0),
            capacity_usage: AtomicU64::new(0),
        }
    }
}

impl Default for AtomicIntCacheStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheReadStats for AtomicIntCacheStats {
    fn total_misses(&self) -> u64 {
        self.total_misses.load(Ordering::Acquire)
    }

    fn total_reads(&self) -> u64 {
        self.total_reads.load(Ordering::Acquire)
    }

    fn inc_total_reads(&self) {
        TOTAL_READS.increment(1);
        self.total_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_total_misses(&self) {
        TOTAL_MISSES.increment(1);
        self.total_misses.fetch_add(1, Ordering::Relaxed);
    }
}

#[allow(clippy::cast_precision_loss)]
impl CacheCapacityStats for AtomicIntCacheStats {
    fn max_capacity(&self) -> u64 {
        self.max_capacity.load(Ordering::Acquire)
    }

    fn set_max_capacity(&self, val: u64) {
        CAPACITY.set(val as f64);
        self.max_capacity.store(val, Ordering::Relaxed);
    }

    fn usage(&self) -> u64 {
        self.capacity_usage.load(Ordering::Acquire)
    }

    fn set_usage(&self, val: u64) {
        USAGE.set(val as f64);
        self.capacity_usage.store(val, Ordering::Relaxed);
    }

    fn inc_usage(&self, val: u64) {
        USAGE.increment(val as f64);
        self.capacity_usage.fetch_add(val, Ordering::Relaxed);
    }

    fn sub_usage(&self, val: u64) {
        USAGE.decrement(val as f64);
        let res =
            self.capacity_usage
                .fetch_update(Ordering::Acquire, Ordering::Relaxed, |current| {
                    if current < val {
                        warn!(
                            "cannot decrement cache usage. current val = {:?} and decrement = {:?}",
                            current, val
                        );
                        None
                    } else {
                        Some(current - val)
                    }
                });
        if let Err(e) = res {
            warn!("error setting cache usage: {:?}", e);
        }
    }
}

impl CacheStats for AtomicIntCacheStats {}
