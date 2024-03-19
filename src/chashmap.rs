use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::borrow::Borrow;
use std::collections::hash_map;
use std::hash::Hash;
use std::sync::atomic::{self, AtomicUsize};
use std::{cmp, fmt};

/// The atomic ordering used throughout the code.
const ORDERING: atomic::Ordering = atomic::Ordering::Relaxed;
/// The length-to-capacity factor.
const LENGTH_MULTIPLIER: usize = 4;
/// The maximal load factor's numerator.
const MAX_LOAD_FACTOR_NUM: usize = 100 - 15;
/// The maximal load factor's denominator.
const MAX_LOAD_FACTOR_DENOM: usize = 100;
/// The default initial capacity.
const DEFAULT_INITIAL_CAPACITY: usize = 64;
/// The lowest capacity a table can have.
const MINIMUM_CAPACITY: usize = 8;

#[derive(Clone)]
enum Bucket<K, V> {
    Empty,
    Full(K, V),
    Removed,
}

impl<K, V> Bucket<K, V> {
    /// Is this bucket 'empty'?
    fn is_empty(&self) -> bool {
        if let Bucket::Empty = *self {
            true
        } else {
            false
        }
    }

    /// Is this bucket 'removed'?
    fn is_removed(&self) -> bool {
        if let Bucket::Removed = *self {
            true
        } else {
            false
        }
    }

    /// Is this bucket free?
    ///
    /// "Free" means that it can safely be replace by another bucket — namely that the bucket is
    /// not occupied.
    fn is_free(&self) -> bool {
        match *self {
            // The two replacable bucket types are removed buckets and empty buckets.
            Bucket::Removed | Bucket::Empty => true,
            // KV pairs can't be replaced as they contain data.
            Bucket::Contains(..) => false,
        }
    }

    /// Get the value (if any) of this bucket.
    ///
    /// This gets the value of the KV pair, if any. If the bucket is not a KV pair, `None` is
    /// returned.
    fn value(self) -> Option<V> {
        if let Bucket::Contains(_, val) = self {
            Some(val)
        } else {
            None
        }
    }

    /// Get a reference to the value of the bucket (if any).
    ///
    /// This returns a reference to the value of the bucket, if it is a KV pair. If not, it will
    /// return `None`.
    ///
    /// Rather than `Option`, it returns a `Result`, in order to make it easier to work with the
    /// `owning_ref` crate (`try_new` and `try_map` of `OwningHandle` and `OwningRef`
    /// respectively).
    fn value_ref(&self) -> Result<&V, ()> {
        if let Bucket::Contains(_, ref val) = *self {
            Ok(val)
        } else {
            Err(())
        }
    }

    /// Does the bucket match a given key?
    ///
    /// This returns `true` if the bucket is a KV pair with key `key`. If not, `false` is returned.
    fn key_matches(&self, key: &K) -> bool
    where
        K: PartialEq,
    {
        if let Bucket::Contains(ref candidate_key, _) = *self {
            // Check if the keys matches.
            candidate_key == key
        } else {
            // The bucket isn't a KV pair, so we'll return false, since there is no key to test
            // against.
            false
        }
    }
}

struct Table<K, V> {
    hasher: hash_map::RandomState,
    buckets: Vec<RwLock<Bucket<K, V>>>,
}

impl<K, V> Table<K, V> {
    /// Create a table with a certain number of buckets.
    fn new(buckets: usize) -> Table<K, V> {
        // TODO: For some obscure reason `RwLock` doesn't implement `Clone`.

        // Fill a vector with `buckets` of `Empty` buckets.
        let mut vec = Vec::with_capacity(buckets);
        for _ in 0..buckets {
            vec.push(RwLock::new(Bucket::Empty));
        }

        Table {
            // Generate a hash function.
            hasher: hash_map::RandomState::new(),
            buckets: vec,
        }
    }

    /// Create a table with at least some capacity.
    fn with_capacity(cap: usize) -> Table<K, V> {
        Table::new(cmp::max(MINIMUM_CAPACITY, cap * LENGTH_MULTIPLIER))
    }
}

impl<K: PartialEq + Hash, V> Table<K, V> {
    /// Hash some key through the internal hash function.
    fn hash<T: ?Sized>(&self, key: &T) -> usize
    where
        T: Hash,
    {
        // Build the initial hash function state.
        let mut hasher = self.hash_builder.build_hasher();
        // Hash the key.
        key.hash(&mut hasher);
        // Cast to `usize`. Since the hash function returns `u64`, this cast won't ever cause
        // entropy less than the ouput space.
        hasher.finish() as usize
    }

    /// Scan from the first priority of a key until a match is found.
    ///
    /// This scans from the first priority of `key` (as defined by its hash), until a match is
    /// found (will wrap on end), i.e. `matches` returns `true` with the bucket as argument.
    ///
    /// The read guard from the RW-lock of the bucket is returned.
    fn scan<F, Q: ?Sized>(&self, key: &Q, matches: F) -> RwLockReadGuard<Bucket<K, V>>
    where
        F: Fn(&Bucket<K, V>) -> bool,
        K: Borrow<Q>,
        Q: Hash,
    {
        // Hash the key.
        let hash = self.hash(key);

        // Start at the first priority bucket, and then move upwards, searching for the matching
        // bucket.
        for i in 0..self.buckets.len() {
            // Get the lock of the `i`'th bucket after the first priority bucket (wrap on end).
            let lock = self.buckets[(hash + i) % self.buckets.len()].read();

            // Check if it is a match.
            if matches(&lock) {
                // Yup. Return.
                return lock;
            }
        }
        panic!("`CHashMap` scan failed! No entry found.");
    }

    /// Scan from the first priority of a key until a match is found (mutable guard).
    ///
    /// This is similar to `scan`, but instead of an immutable lock guard, a mutable lock guard is
    /// returned.
    fn scan_mut<F, Q: ?Sized>(&self, key: &Q, matches: F) -> RwLockWriteGuard<Bucket<K, V>>
    where
        F: Fn(&Bucket<K, V>) -> bool,
        K: Borrow<Q>,
        Q: Hash,
    {
        // Hash the key.
        let hash = self.hash(key);

        // Start at the first priority bucket, and then move upwards, searching for the matching
        // bucket.
        for i in 0..self.buckets.len() {
            // Get the lock of the `i`'th bucket after the first priority bucket (wrap on end).
            let lock = self.buckets[(hash + i) % self.buckets.len()].write();

            // Check if it is a match.
            if matches(&lock) {
                // Yup. Return.
                return lock;
            }
        }
        panic!("`CHashMap` scan_mut failed! No entry found.");
    }

    /// Scan from the first priority of a key until a match is found (bypass locks).
    ///
    /// This is similar to `scan_mut`, but it safely bypasses the locks by making use of the
    /// aliasing invariants of `&mut`.
    fn scan_mut_no_lock<F>(&mut self, key: &K, matches: F) -> &mut Bucket<K, V>
    where
        F: Fn(&Bucket<K, V>) -> bool,
    {
        // Hash the key.
        let hash = self.hash(key);
        // TODO: To tame the borrowchecker, we fetch this in advance.
        let len = self.buckets.len();

        // Start at the first priority bucket, and then move upwards, searching for the matching
        // bucket.
        for i in 0..self.buckets.len() {
            // TODO: hacky hacky
            let idx = (hash + i) % len;

            // Get the lock of the `i`'th bucket after the first priority bucket (wrap on end).

            // Check if it is a match.
            if {
                let bucket = self.buckets[idx].get_mut();
                matches(&bucket)
            } {
                // Yup. Return.
                return self.buckets[idx].get_mut();
            }
        }
        panic!("`CHashMap` scan_mut_no_lock failed! No entry found.");
    }

    /// Find a bucket with some key, or a free bucket in same cluster.
    ///
    /// This scans for buckets with key `key`. If one is found, it will be returned. If none are
    /// found, it will return a free bucket in the same cluster.
    fn lookup_or_free(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        // Hash the key.
        let hash = self.hash(key);
        // The encountered free bucket.
        let mut free = None;

        // Start at the first priority bucket, and then move upwards, searching for the matching
        // bucket.
        for i in 0..self.buckets.len() {
            // Get the lock of the `i`'th bucket after the first priority bucket (wrap on end).
            let lock = self.buckets[(hash + i) % self.buckets.len()].write();

            if lock.key_matches(key) {
                // We found a match.
                return lock;
            } else if lock.is_empty() {
                // The cluster is over. Use the encountered free bucket, if any.
                return free.unwrap_or(lock);
            } else if lock.is_removed() && free.is_none() {
                // We found a free bucket, so we can store it to later (if we don't already have
                // one).
                free = Some(lock)
            }
        }

        free.expect("No free buckets found")
    }

    /// Lookup some key.
    ///
    /// This searches some key `key`, and returns a immutable lock guard to its bucket. If the key
    /// couldn't be found, the returned value will be an `Empty` cluster.
    fn lookup<Q: ?Sized>(&self, key: &Q) -> RwLockReadGuard<Bucket<K, V>>
    where
        K: Borrow<Q>,
        Q: PartialEq + Hash,
    {
        self.scan(key, |x| match *x {
            // We'll check that the keys does indeed match, as the chance of hash collisions
            // happening is inevitable
            Bucket::Contains(ref candidate_key, _) if key.eq(candidate_key.borrow()) => true,
            // We reached an empty bucket, meaning that there are no more buckets, not even removed
            // ones, to search.
            Bucket::Empty => true,
            _ => false,
        })
    }

    /// Lookup some key, mutably.
    ///
    /// This is similar to `lookup`, but it returns a mutable guard.
    ///
    /// Replacing at this bucket is safe as the bucket will be in the same cluster of buckets as
    /// the first priority cluster.
    fn lookup_mut<Q: ?Sized>(&self, key: &Q) -> RwLockWriteGuard<Bucket<K, V>>
    where
        K: Borrow<Q>,
        Q: PartialEq + Hash,
    {
        self.scan_mut(key, |x| match *x {
            // We'll check that the keys does indeed match, as the chance of hash collisions
            // happening is inevitable
            Bucket::Contains(ref candidate_key, _) if key.eq(candidate_key.borrow()) => true,
            // We reached an empty bucket, meaning that there are no more buckets, not even removed
            // ones, to search.
            Bucket::Empty => true,
            _ => false,
        })
    }

    /// Find a free bucket in the same cluster as some key.
    ///
    /// This means that the returned lock guard defines a valid, free bucket, where `key` can be
    /// inserted.
    fn find_free(&self, key: &K) -> RwLockWriteGuard<Bucket<K, V>> {
        self.scan_mut(key, |x| x.is_free())
    }

    /// Find a free bucket in the same cluster as some key (bypassing locks).
    ///
    /// This is similar to `find_free`, except that it safely bypasses locks through the aliasing
    /// guarantees of `&mut`.
    fn find_free_no_lock(&mut self, key: &K) -> &mut Bucket<K, V> {
        self.scan_mut_no_lock(key, |x| x.is_free())
    }

    /// Fill the table with data from another table.
    ///
    /// This is used to efficiently copy the data of `table` into `self`.
    ///
    /// # Important
    ///
    /// The table should be empty for this to work correctly/logically.
    fn fill(&mut self, table: Table<K, V>) {
        // Run over all the buckets.
        for i in table.buckets {
            // We'll only transfer the bucket if it is a KV pair.
            if let Bucket::Contains(key, val) = i.into_inner() {
                // Find a bucket where the KV pair can be inserted.
                let mut bucket = self.scan_mut_no_lock(&key, |x| match *x {
                    // Halt on an empty bucket.
                    Bucket::Empty => true,
                    // We'll assume that the rest of the buckets either contains other KV pairs (in
                    // particular, no buckets have been removed in the newly construct table).
                    _ => false,
                });

                // Set the bucket to the KV pair.
                *bucket = Bucket::Contains(key, val);
            }
        }
    }
}

impl<K: Clone, V: Clone> Clone for Table<K, V> {
    fn clone(&self) -> Table<K, V> {
        Table {
            // Since we copy plainly without rehashing etc., it is important that we keep the same
            // hash function.
            hasher: self.hasher.clone(),
            // Lock and clone every bucket individually.
            buckets: self
                .buckets
                .iter()
                .map(|x| RwLock::new(x.read().clone()))
                .collect(),
        }
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for Table<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // create a debug map and fill with entries
        let mut map = f.debug_map();
        // We'll just run over all buckets and output one after one.
        for i in &self.buckets {
            // Acquire the lock.
            let lock = i.read();
            // Check if the bucket actually contains anything.
            if let Bucket::Contains(ref key, ref val) = *lock {
                // add this entry to the map
                map.entry(key, val);
            }
        }
        map.finish()
    }
}

/// An iterator over the entries of some table.
pub struct IntoIter<K, V> {
    /// The inner table.
    table: Table<K, V>,
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        // We own the table, and can thus do what we want with it. We'll simply pop from the
        // buckets until we find a bucket containing data.
        while let Some(bucket) = self.table.buckets.pop() {
            // We can bypass dem ebil locks.
            if let Bucket::Contains(key, val) = bucket.into_inner() {
                // The bucket contained data, so we'll return the pair.
                return Some((key, val));
            }
        }

        // We've exhausted all the buckets, and no more data could be found.
        None
    }
}

impl<K, V> IntoIterator for Table<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> IntoIter<K, V> {
        IntoIter { table: self }
    }
}

/// A concurrent hash map.
///
/// This type defines a concurrent associative array, based on hash tables with linear probing and
/// dynamic resizing.
///
/// The idea is to let each entry hold a multi-reader lock, effectively limiting lock contentions
/// to writing simultaneously on the same entry, and resizing the table.
///
/// It is not an atomic or lockless hash table, since such construction is only useful in very few
/// cases, due to limitations on in-place operations on values.
pub struct CHashMap<K, V> {
    /// The inner table.
    table: RwLock<Table<K, V>>,
    /// The total number of KV pairs in the table.
    ///
    /// This is used to calculate the load factor.
    len: AtomicUsize,
}
