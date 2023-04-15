//! This module implements "PTHash: Revisiting FCH Minimal Perfect Hashing" by
//! Giulio Ermanno Pibiri, Roberto Trani, arXiv:2104.10402, https://arxiv.org/abs/2104.10402.

const std = @import("std");
const Wyhash = std.hash.Wyhash;

const CompactArray = @import("./CompactArray.zig");
const EliasFano = @import("./EliasFano.zig");
const FreeSlotEncoding = EliasFano;

/// The bucketer takes a hash and places it into a bucket in an un-even fashion:
/// Roughly 60% of the keys are mapped to 30% of the buckets. In addition,
/// it's initialize with a `c` parameter which represents the expected number of
/// bits-per-n that is required to encode the pivots that are created by PTHash.
const Bucketer = struct {
    n: usize,
    m: usize,
    p1: usize,
    p2: usize,

    /// Creates a new bucketer for `n` items with a given `c` parameter.
    pub fn init(n: usize, c: usize) Bucketer {
        const m = c * n / (std.math.log2_int(usize, n) + 1);
        const p1 = @floatToInt(usize, 0.6 * @intToFloat(f64, n));
        const p2 = @floatToInt(usize, 0.3 * @intToFloat(f64, m));

        return Bucketer{
            .n = n,
            .m = m,
            .p1 = p1,
            .p2 = p2,
        };
    }

    /// Returns the bucket for a hash.
    pub fn getBucket(self: Bucketer, hash: u64) u64 {
        if (hash % self.n < self.p1) {
            return hash % self.p2;
        } else {
            return self.p2 + (hash % (self.m - self.p2));
        }
    }

    pub fn writeTo(self: *const Bucketer, w: anytype) !void {
        try w.writeIntNative(u64, self.n);
        try w.writeIntNative(u64, self.m);
        try w.writeIntNative(u64, self.p1);
        try w.writeIntNative(u64, self.p2);
    }

    pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Bucketer {
        var r = stream.reader();
        const n = try r.readIntNative(u64);
        const m = try r.readIntNative(u64);
        const p1 = try r.readIntNative(u64);
        const p2 = try r.readIntNative(u64);
        return Bucketer{
            .n = n,
            .m = m,
            .p1 = p1,
            .p2 = p2,
        };
    }
};

/// Information about the hash + bucket for a key. We compute this once and re-use it.
const HashedKey = struct {
    hash: u64,
    bucket: u64,

    fn lessThan(_: void, lhs: HashedKey, rhs: HashedKey) bool {
        if (lhs.bucket == rhs.bucket) return lhs.hash < rhs.hash;
        return lhs.bucket < rhs.bucket;
    }
};

/// The bucket summary contains information about a single bucket for a slice of hashed keys.
/// The slice should be sorted by bucket.
const BucketSummary = struct {
    idx: usize,
    entry_start: usize,
    entry_end: usize,

    fn count(self: BucketSummary) usize {
        return self.entry_end - self.entry_start;
    }

    fn lessThan(_: void, a: BucketSummary, b: BucketSummary) bool {
        const a_count = a.count();
        const b_count = b.count();
        if (a_count == b_count) return a.idx < b.idx;
        return b_count < a_count;
    }
};

pub const Params = struct {
    c: usize,
    alpha: f64 = 1,
};

// Number of different seeds we try before we give up.
const MAX_ATTEMPTS = 1000;

/// A minimal perfect hash function for a given type and a hash function.
pub fn HashFn(
    comptime Key: type,
    comptime hasher: fn (seed: u64, Key: Key) u64,
    comptime Encoding: type,
) type {
    return struct {
        const Self = @This();

        n: usize,
        seed: u64,
        bucketer: Bucketer,
        free_slots: FreeSlotEncoding,
        pivots: Encoding,

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.pivots.deinit(allocator);
            self.free_slots.deinit(allocator);
            self.* = undefined;
        }

        pub fn get(self: *const Self, key: Key) u64 {
            const hash = hasher(self.seed, key);
            const bucket = self.bucketer.getBucket(hash);
            const pivot = self.pivots.get(bucket);
            const bucket_hash = Wyhash.hash(self.seed, std.mem.asBytes(&pivot));
            const full_hash = Wyhash.hash(bucket_hash, std.mem.asBytes(&hash));
            const pos = full_hash % self.bucketer.n;
            if (pos < self.n) {
                return pos;
            } else {
                return self.free_slots.get(pos - self.n);
            }
        }

        pub fn bits(self: *const Self) usize {
            return self.pivots.bits() + self.free_slots.bits();
        }

        pub fn build(
            allocator: std.mem.Allocator,
            keys: []const Key,
            params: Params,
            seed: ?u64,
        ) !Self {
            if (seed) |s| {
                return buildUsingSeed(allocator, keys, params, s);
            } else {
                return buildUsingRandomSeed(allocator, keys, params, MAX_ATTEMPTS);
            }
        }

        pub fn buildUsingRandomSeed(
            allocator: std.mem.Allocator,
            keys: []const Key,
            params: Params,
            max_attempts: usize,
        ) !Self {
            var seed: u64 = undefined;

            var attempts: usize = 0;
            while (attempts < max_attempts) : (attempts += 1) {
                try std.os.getrandom(std.mem.asBytes(&seed));

                return buildUsingSeed(allocator, keys, params, seed) catch |err| switch (err) {
                    error.HashCollision => continue,
                    else => err,
                };
            }

            return error.HashCollision;
        }

        pub fn buildUsingSeed(
            allocator: std.mem.Allocator,
            keys: []const Key,
            params: Params,
            seed: u64,
        ) !Self {
            std.debug.assert(params.alpha <= 1);
            const n_prime = @floatToInt(usize, @intToFloat(f64, keys.len) / params.alpha);
            const bucketer = Bucketer.init(n_prime, params.c);

            // Step 1: Hash all the inputs and figure out which bucket they belong to.

            var entries = try allocator.alloc(HashedKey, keys.len);
            defer allocator.free(entries);

            for (keys, 0..) |key, idx| {
                const hash = hasher(seed, key);
                const bucket = bucketer.getBucket(hash);
                entries[idx] = HashedKey{ .hash = hash, .bucket = bucket };
            }

            std.sort.sort(HashedKey, entries, {}, HashedKey.lessThan);

            // Step 2: Group the entries into buckets ordered by size.

            var bucket_summaries = try std.ArrayList(BucketSummary).initCapacity(allocator, bucketer.m);
            defer bucket_summaries.deinit();

            var bucket_start: usize = 0;
            var bucket_idx: usize = 0;
            var i: usize = 1;
            while (i < entries.len + 1) : (i += 1) {
                const at_boundary = (i == entries.len) or (entries[i - 1].bucket != entries[i].bucket);
                if (at_boundary) {
                    bucket_summaries.appendAssumeCapacity(BucketSummary{
                        .idx = entries[i - 1].bucket,
                        .entry_start = bucket_start,
                        .entry_end = i,
                    });
                    bucket_idx += 1;
                    bucket_start = i;
                } else {
                    if (entries[i - 1].hash == entries[i].hash) return error.HashCollision;
                }
            }

            std.sort.sort(BucketSummary, bucket_summaries.items, {}, BucketSummary.lessThan);

            // Step 3: Determine pivots

            var taken = try std.bit_set.DynamicBitSet.initEmpty(allocator, bucketer.n);
            defer taken.deinit();

            var attempted_taken = try std.bit_set.DynamicBitSet.initEmpty(allocator, bucketer.n);
            defer attempted_taken.deinit();

            var pivots = try allocator.alloc(u64, bucketer.m);
            defer allocator.free(pivots);

            std.mem.set(u64, pivots, 0);

            for (bucket_summaries.items) |b| {
                var pivot: u64 = 0;
                find_pivot: while (true) : (pivot += 1) {
                    // Reset attempted_taken
                    attempted_taken.setRangeValue(.{ .start = 0, .end = attempted_taken.capacity() }, false);

                    for (entries[b.entry_start..b.entry_end]) |entry| {
                        const bucket_hash = Wyhash.hash(seed, std.mem.asBytes(&pivot));
                        const full_hash = Wyhash.hash(bucket_hash, std.mem.asBytes(&entry.hash));
                        const pos = full_hash % bucketer.n;

                        const is_taken_earlier_bucket = taken.isSet(pos);
                        const is_taken_same_bucket = attempted_taken.isSet(pos);

                        if (is_taken_earlier_bucket or is_taken_same_bucket) {
                            continue :find_pivot;
                        }

                        attempted_taken.set(pos);
                    }

                    pivots[b.idx] = pivot;

                    taken.setUnion(attempted_taken);
                    break;
                }
            }

            const encoded_pivots = try Encoding.encode(allocator, pivots);

            var free_slots = try allocator.alloc(u64, bucketer.n - keys.len);
            defer allocator.free(free_slots);

            var iter = taken.iterator(.{ .kind = .unset });

            var prev_free_value: usize = 0;
            var free_idx: usize = 0;
            while (free_idx < free_slots.len) : (free_idx += 1) {
                if (taken.isSet(keys.len + free_idx)) {
                    free_slots[free_idx] = iter.next().?;
                    prev_free_value = free_slots[free_idx];
                } else {
                    // This value can be anything. We keep it incremental.
                    free_slots[free_idx] = prev_free_value;
                }
            }

            const encoded_free_slots = try FreeSlotEncoding.encode(allocator, free_slots);

            return Self{
                .bucketer = bucketer,
                .n = keys.len,
                .free_slots = encoded_free_slots,
                .seed = seed,
                .pivots = encoded_pivots,
            };
        }

        pub fn writeTo(self: *const Self, w: anytype) !void {
            try w.writeIntNative(u64, self.n);
            try w.writeIntNative(u64, self.seed);
            try self.bucketer.writeTo(w);
            try self.free_slots.writeTo(w);
            try self.pivots.writeTo(w);
        }

        pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
            var r = stream.reader();
            const n = try r.readIntNative(u64);
            const seed = try r.readIntNative(u64);
            const bucketer = try Bucketer.readFrom(stream);
            const free_slots = try FreeSlotEncoding.readFrom(stream);
            const pivots = try Encoding.readFrom(stream);
            return Self{
                .n = n,
                .seed = seed,
                .bucketer = bucketer,
                .free_slots = free_slots,
                .pivots = pivots,
            };
        }
    };
}

pub fn AutoHashFn(
    comptime Key: type,
    comptime Encoding: type,
) type {
    const hasher = struct {
        fn hash(seed: u64, key: Key) u64 {
            if (comptime std.meta.trait.hasUniqueRepresentation(Key)) {
                return Wyhash.hash(seed, std.mem.asBytes(&key));
            } else {
                var hasher = Wyhash.init(seed);
                std.hash.autoHash(&hasher, key);
                return hasher.final();
            }
        }
    }.hash;

    return HashFn(Key, hasher, Encoding);
}

pub fn BytesHashFn(comptime Encoding: type) type {
    return HashFn([]const u8, Wyhash.hash, Encoding);
}

const testing = std.testing;

test "basic bucketing" {
    const b = Bucketer.init(100, 7);
    try testing.expectEqual(@as(u64, 0), b.getBucket(0));
}

test "building" {
    var data: [256]u64 = undefined;

    var i: usize = 0;
    while (i < data.len) : (i += 1) {
        data[i] = i * i;
    }

    var h = try AutoHashFn(u64, CompactArray).buildUsingRandomSeed(testing.allocator, &data, .{ .c = 7, .alpha = 0.80 }, 10);
    defer h.deinit(testing.allocator);

    var seen = std.hash_map.AutoHashMap(u64, usize).init(testing.allocator);
    defer seen.deinit();

    for (data, 0..) |val, idx| {
        const out = h.get(val);
        try testing.expect(out < data.len);

        if (try seen.fetchPut(out, idx)) |other_entry| {
            std.debug.print("collision between idx={} and {}\n", .{ other_entry.value, idx });
            return error.TestCollision;
        }
    }
}

test "collision detection" {
    var data: [2]u64 = .{ 5, 5 };
    var h_result = AutoHashFn(u64, CompactArray).buildUsingRandomSeed(testing.allocator, &data, .{ .c = 7 }, 10);
    if (h_result) |*h| h.deinit(testing.allocator) else |_| {}

    try testing.expectError(error.HashCollision, h_result);
}
