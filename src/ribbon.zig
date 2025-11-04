//! This file implements the ideas from "Fast Succinct Retrieval and Approximate Membership using Ribbon".

const std = @import("std");
const builtin = @import("builtin");
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const CompactArray = @import("./CompactArray.zig");
const utils = @import("./utils.zig");

const endian = builtin.cpu.arch.endian();

fn bitParity(num: u64) u64 {
    return @popCount(num) % 2;
}

const RibbonTable = struct {
    const Self = @This();

    n: usize,
    data: CompactArray,

    pub fn init(n: usize, data: CompactArray) Self {
        return Self{
            .n = n,
            .data = data,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.data.deinit(allocator);
        self.* = undefined;
    }

    pub fn lookup(self: Self, i: u64, c: u64) u64 {
        std.debug.assert((c & 1) == 1);

        var i_ = i;
        var c_ = c;
        var result: u64 = 0;

        while (true) {
            result ^= self.data.get(i_);

            c_ >>= 1;
            i_ += 1;
            if (c_ == 0) break;

            const j: u6 = @intCast(@ctz(c_));
            i_ += j;
            c_ >>= j;
        }
        return result;
    }

    pub fn bits(self: *const Self) u64 {
        return self.data.bits();
    }

    pub fn writeTo(self: *const Self, w: anytype) !void {
        try w.writeInt(u64, self.n, endian);
        try self.data.writeTo(w);
    }

    pub fn readFrom(r: *std.Io.Reader) !Self {
        const n = try r.takeInt(u64, endian);
        const data = try CompactArray.readFrom(r);
        return Self{ .n = n, .data = data };
    }
};

pub const RibbonBandingSystem = struct {
    const Self = @This();

    const Array = CompactArray.Mutable;

    n: usize,
    c: Array,
    b: Array,

    pub fn init(allocator: std.mem.Allocator, n: usize, r: u6, w: u6) !Self {
        var c = try Array.init(allocator, w, n);
        errdefer c.deinit(allocator);

        var b = try Array.init(allocator, r, n);
        errdefer b.deinit(allocator);

        return Self{ .n = n, .c = c, .b = b };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.c.deinit(allocator);
        self.b.deinit(allocator);
        self.* = undefined;
    }

    pub fn getBandWidth(self: Self) u6 {
        return self.c.width;
    }

    pub fn getValueSize(self: Self) u6 {
        return self.b.width;
    }

    pub const InsertResult = union(enum) {
        success: usize,
        redunant: void,
        failure: void,
    };

    pub fn insertRow(self: *Self, i: usize, c: u64, b: u64) InsertResult {
        std.debug.assert(b >> self.getValueSize() == 0);
        std.debug.assert(c >> self.getBandWidth() == 0);
        std.debug.assert((c & 1) == 1);

        var i_ = i;
        var c_ = c;
        var b_ = b;

        while (true) {
            if (self.c.get(i_) == 0) {
                self.c.setFromZero(i_, c_);
                self.b.setFromZero(i_, b_);
                return .{ .success = i_ };
            }

            c_ = c_ ^ self.c.get(i_);
            b_ = b_ ^ self.b.get(i_);

            if (c_ == 0) {
                if (b_ == 0) {
                    return .redunant;
                } else {
                    return .failure;
                }
            }

            const j: u6 = @intCast(@ctz(c_));
            c_ >>= j;
            i_ += j;
        }
    }

    pub fn clearRow(self: *Self, i: usize) void {
        self.c.setToZero(i);
        self.b.setToZero(i);
    }

    pub fn build(self: Self, allocator: std.mem.Allocator) !RibbonTable {
        const r = self.getValueSize();

        var data = try CompactArray.Mutable.init(allocator, r, self.n);
        errdefer data.deinit(allocator);

        var state = try allocator.alloc(u64, r);
        defer allocator.free(state);
        @memset(state, 0);

        // This logic is taken from https://github.com/lorenzhs/BuRR/blob/1c62832ad7d6eab5b337f386955868c3ce9a54ea/backsubst.hpp#L46
        // and I honestly don't quite understand how it works.

        var i = self.n;
        while (i > 0) {
            i -= 1;

            const c = self.c.get(i);
            const b = self.b.get(i);
            var resultRow: u64 = 0;

            var j: u6 = 0;
            while (j < r) : (j += 1) {
                var tmp = state[j] << 1;
                const bit = bitParity(tmp & c) ^ ((b >> j) & 1);
                tmp |= bit;
                state[j] = tmp;
                resultRow |= (bit << j);
            }

            data.setFromZero(i, resultRow);
        }

        return RibbonTable.init(self.n, data.finalize());
    }
};

const BumpedLayer = struct {
    bucket_size: usize,
    upper_threshold: usize,
    lower_threshold: usize,
    thresholds: CompactArray,
    table: RibbonTable,

    pub fn deinit(self: *BumpedLayer, allocator: std.mem.Allocator) void {
        self.table.deinit(allocator);
        self.thresholds.deinit(allocator);
    }

    pub fn lookup(self: BumpedLayer, i: u64, c: u64) ?u64 {
        if (self.isBumped(i)) {
            return null;
        } else {
            return self.table.lookup(i, c);
        }
    }

    fn isBumped(self: BumpedLayer, i: u64) bool {
        const bucket_idx = i / self.bucket_size;
        const bucket_offset = i % self.bucket_size;
        const threshold = self.thresholds.get(bucket_idx);
        const threshold_values = [4]usize{ 0, self.lower_threshold, self.upper_threshold, self.bucket_size };
        return bucket_offset < threshold_values[threshold];
    }

    pub fn bits(self: BumpedLayer) usize {
        return self.table.bits() + self.thresholds.bits();
    }

    pub fn writeTo(self: *const BumpedLayer, w: anytype) !void {
        try w.writeInt(u64, self.bucket_size, endian);
        try w.writeInt(u64, self.upper_threshold, endian);
        try w.writeInt(u64, self.lower_threshold, endian);
        try self.thresholds.writeTo(w);
        try self.table.writeTo(w);
    }

    pub fn readFrom(r: *std.Io.Reader) !BumpedLayer {
        const bucket_size = try r.takeInt(u64, endian);
        const upper_threshold = try r.takeInt(u64, endian);
        const lower_threshold = try r.takeInt(u64, endian);
        const thresholds = try CompactArray.readFrom(r);
        const table = try RibbonTable.readFrom(r);

        return BumpedLayer{
            .bucket_size = bucket_size,
            .upper_threshold = upper_threshold,
            .lower_threshold = lower_threshold,
            .thresholds = thresholds,
            .table = table,
        };
    }
};

const BumpedLayerBuilder = struct {
    const Self = @This();

    const Input = struct {
        hash1: u64,
        hash2: u64,
        hash_result: HashResult,
        value: u64,
    };

    m: usize,
    eps: f64,
    opts: BuildOptions,
    input: std.ArrayListUnmanaged(Input),

    fn tableSizeFromEps(n: usize, eps: f64, w: u6) usize {
        const target: usize = @intFromFloat(@as(f64, @floatFromInt(n)) * (eps + 1));
        return @max(target, @as(usize, @intCast(w)) + 1);
    }

    pub fn init(allocator: std.mem.Allocator, n: usize, eps: f64, opts: BuildOptions) error{OutOfMemory}!Self {
        const input = try std.ArrayListUnmanaged(Input).initCapacity(allocator, n);

        return Self{
            .m = tableSizeFromEps(n, eps, opts.w),
            .eps = eps,
            .opts = opts,
            .input = input,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.input.deinit(allocator);
        self.* = undefined;
    }

    pub fn insert(self: *Self, hash1: u64, hash2: u64, value: u64) void {
        self.input.appendAssumeCapacity(
            Input{
                .hash1 = hash1,
                .hash2 = hash2,
                .hash_result = splitHash(hash1, hash2, self.m, self.opts.w),
                .value = value,
            },
        );
    }

    pub fn build(self: *Self, allocator: std.mem.Allocator) error{ OutOfMemory, HashCollision }!BumpedLayer {
        const w64 = @as(u64, self.opts.w);
        const bucket_size = (w64 * w64) / (4 * std.math.log2_int_ceil(u64, w64));
        const n = self.input.items.len;

        const lessThan = struct {
            fn lessThan(_: void, left: Input, right: Input) bool {
                return left.hash_result.i < right.hash_result.i;
            }
        }.lessThan;

        std.mem.sort(Input, self.input.items, {}, lessThan);

        var system = try RibbonBandingSystem.init(allocator, self.m, self.opts.r, self.opts.w);
        defer system.deinit(allocator);

        var inserted = try std.ArrayListUnmanaged(?usize).initCapacity(allocator, bucket_size);
        defer inserted.deinit(allocator);

        var thresholds = try CompactArray.Mutable.init(allocator, 2, std.math.divCeil(usize, self.m, bucket_size) catch unreachable);
        errdefer thresholds.deinit(allocator);

        const lower_threshold = bucket_size / 7;
        const upper_threshold = bucket_size / 4;
        std.debug.assert(lower_threshold < upper_threshold);
        std.debug.assert(upper_threshold < bucket_size);

        const threshold_values = [4]usize{ 0, lower_threshold, upper_threshold, bucket_size };

        const inputs = self.input.items;

        var i: usize = 0;
        var bucket_start: usize = 0;
        var bucket_idx: usize = 0;
        var bump_count: usize = 0;

        while (i < n) {
            var j = i;

            // Find the end position of this bucket:
            while (j < n) {
                if (inputs[j].hash_result.i >= bucket_start + bucket_size) break;
                j += 1;
            }

            inserted.clearRetainingCapacity();

            var bump_offset: usize = 0;

            // Now iterate backwards again and insert them:
            var k: usize = j;
            while (k > i) {
                k -= 1;
                const input = inputs[k];
                switch (system.insertRow(input.hash_result.i, input.hash_result.c, input.value)) {
                    .success => |idx| {
                        try inserted.append(allocator, idx);
                    },
                    .redunant => {
                        try inserted.append(allocator, null);
                    },
                    .failure => {
                        bump_offset = input.hash_result.i - bucket_start + 1;
                        k += 1;
                        break;
                    },
                }
            }

            // Next determine the actual threshold to use:
            var threshold: usize = undefined;
            for (threshold_values, 0..) |threshold_value, idx| {
                if (threshold_value >= bump_offset) {
                    threshold = idx;
                    break;
                }
            }

            const threshold_value = threshold_values[threshold];

            thresholds.setFromZero(bucket_idx, threshold);

            // And now undo all the inserted ones which have an offset outside the threshold:
            while (k < j) : (k += 1) {
                const input = inputs[k];
                if (input.hash_result.i - bucket_start >= threshold_value) break;
                if (inserted.pop().?) |idx| {
                    system.clearRow(idx);
                }
            }

            bump_count += k - i;

            // Prepare for the next bucket:
            i = j;
            bucket_start += bucket_size;
            bucket_idx += 1;
        }

        var table = try system.build(allocator);
        errdefer table.deinit(allocator);

        // Prepare for the next layer

        var next_inputs = try std.ArrayListUnmanaged(Input).initCapacity(allocator, bump_count);
        errdefer next_inputs.deinit(allocator);

        var layer = BumpedLayer{
            .table = table,
            .bucket_size = bucket_size,
            .upper_threshold = upper_threshold,
            .lower_threshold = lower_threshold,
            .thresholds = thresholds.finalize(),
        };

        self.m = tableSizeFromEps(bump_count, self.eps, self.opts.w);

        for (inputs) |input| {
            if (layer.isBumped(input.hash_result.i)) {
                next_inputs.appendAssumeCapacity(Input{
                    .hash1 = input.hash1,
                    .hash2 = input.hash2,
                    .hash_result = splitHash(input.hash1, input.hash2, self.m, self.opts.w),
                    .value = input.value,
                });
            }
        }

        std.debug.assert(next_inputs.items.len == bump_count);

        self.input.deinit(allocator);
        self.input = next_inputs;

        return layer;
    }

    pub fn buildFallbackTable(self: *BumpedLayerBuilder, allocator: std.mem.Allocator) !RibbonTable {
        const n = self.input.items.len;
        const step = @max(n / 10, 1);
        var m: usize = @max(n, @as(usize, @intCast(self.opts.w)) + 1);

        var i: usize = 0;
        loop: while (i < 50) : (i += 1) {
            var system = try RibbonBandingSystem.init(allocator, m, self.opts.r, self.opts.w);
            defer system.deinit(allocator);

            for (self.input.items) |input| {
                const h = splitHash(input.hash1, input.hash2, m, self.opts.w);
                const insert_result = system.insertRow(h.i, h.c, input.value);
                switch (insert_result) {
                    .failure => {
                        m += step;
                        continue :loop;
                    },
                    else => {},
                }
            }

            return try system.build(allocator);
        }

        return error.HashCollision;
    }
};

const HashResult = struct {
    i: u64,
    c: u64,
};

fn splitHash(hash1: u64, hash2: u64, n: usize, w: u6) HashResult {
    const i = hash1 % (n - w);
    const c_mask = ((@as(u64, 1) << w) - 1);
    const c = (hash2 & c_mask) | 1;
    return .{ .i = i, .c = c };
}

pub const BuildOptions = struct {
    r: u6,
    w: u6,
    seed: u64 = 100,
};

pub fn Ribbon(
    comptime Key: type,
    comptime hasher: fn (seed: u64, Key: Key) u64,
) type {
    return struct {
        const Self = @This();

        fn hashKey(seed: u64, key: Key, n: usize, w: u6) HashResult {
            return splitHash(hasher(seed, key), hasher(seed + 1, key), n, w);
        }

        w: u6,
        seed: u64,
        table: RibbonTable,

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.table.deinit(allocator);
            self.* = undefined;
        }

        pub fn lookup(self: *const Self, key: Key) u64 {
            const h = hashKey(self.seed, key, self.table.n, self.w);
            return self.table.lookup(h.i, h.c);
        }

        pub fn bits(self: *const Self) u64 {
            return self.table.bits();
        }

        pub fn writeTo(self: *const Self, w: anytype) !void {
            try w.writeIntNative(u64, self.w);
            try w.writeIntNative(u64, self.seed);
            try self.table.writeTo(w);
        }

        pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
            var r = stream.reader();
            const w = try r.readIntNative(u64);
            const seed = try r.readIntNative(u64);
            const table = try RibbonTable.readFrom(stream);
            return Self{
                .w = @intCast(w),
                .seed = seed,
                .table = table,
            };
        }

        /// IncrementalBuilder builds the Ribbon table incrementally:
        /// It uses a fixed `n` and tries to construct a table as it inserts entries.
        /// If it's not possible to build a table for a given entry it will fail.
        pub const IncrementalBuilder = struct {
            n: usize,
            seed: u64,
            system: RibbonBandingSystem,

            pub fn init(allocator: std.mem.Allocator, n: usize, opts: BuildOptions) error{OutOfMemory}!IncrementalBuilder {
                const system = try RibbonBandingSystem.init(allocator, n, opts.r, opts.w);

                return IncrementalBuilder{
                    .n = n,
                    .seed = opts.seed,
                    .system = system,
                };
            }

            pub fn deinit(self: *IncrementalBuilder, allocator: std.mem.Allocator) void {
                self.system.deinit(allocator);
                self.* = undefined;
            }

            pub fn insert(self: *IncrementalBuilder, key: Key, value: u64) error{HashCollision}!void {
                const h = hashKey(self.seed, key, self.n, self.system.getBandWidth());
                switch (self.system.insertRow(h.i, h.c, value)) {
                    .failure => return error.HashCollision,
                    else => {},
                }
            }

            pub fn build(self: IncrementalBuilder, allocator: std.mem.Allocator) error{OutOfMemory}!Self {
                const table = try self.system.build(allocator);

                return Self{
                    .w = self.system.getBandWidth(),
                    .seed = self.seed,
                    .table = table,
                };
            }
        };

        pub const IterativeBuilder = struct {
            const Input = struct {
                hash1: u64,
                hash2: u64,
                value: u64,
            };

            n: usize,
            seed: u64,
            input: std.ArrayListUnmanaged(Input),

            pub fn init(allocator: std.mem.Allocator, n: usize, seed: u64) error{OutOfMemory}!IterativeBuilder {
                const input = try std.ArrayListUnmanaged(Input).initCapacity(allocator, n);

                return IterativeBuilder{
                    .n = n,
                    .seed = seed,
                    .input = input,
                };
            }

            pub fn deinit(self: *IterativeBuilder, allocator: std.mem.Allocator) void {
                self.input.deinit(allocator);
                self.* = undefined;
            }

            pub fn insert(self: *IterativeBuilder, key: Key, value: u64) void {
                self.input.appendAssumeCapacity(
                    Input{
                        .hash1 = hasher(self.seed, key),
                        .hash2 = hasher(self.seed + 1, key),
                        .value = value,
                    },
                );
            }

            pub fn insertWithAllocator(self: *IterativeBuilder, allocator: std.mem.Allocator, key: Key, value: u64) error{OutOfMemory}!void {
                try self.input.append(
                    allocator,
                    Input{
                        .hash1 = hasher(self.seed, key),
                        .hash2 = hasher(self.seed + 1, key),
                        .value = value,
                    },
                );
            }

            pub fn build(self: IterativeBuilder, allocator: std.mem.Allocator, opts: BuildOptions) error{ OutOfMemory, HashCollision }!Self {
                std.debug.assert(self.seed == opts.seed);

                const n = self.input.items.len;
                const step = @max(n / 10, 1);
                var m: usize = n;

                var i: usize = 0;
                loop: while (i < 50) : (i += 1) {
                    var system = try RibbonBandingSystem.init(allocator, m, opts.r, opts.w);
                    defer system.deinit(allocator);

                    for (self.input.items) |input| {
                        const h = splitHash(input.hash1, input.hash2, m, opts.w);
                        const insert_result = system.insertRow(h.i, h.c, input.value);
                        switch (insert_result) {
                            .failure => {
                                m += step;
                                continue :loop;
                            },
                            else => {},
                        }
                    }

                    const table = try system.build(allocator);

                    return Self{
                        .w = opts.w,
                        .seed = opts.seed,
                        .table = table,
                    };
                }

                return error.HashCollision;
            }
        };

        fn BoundedArray(comptime T: type, n: comptime_int) type {
            return struct {
                const Arr = @This();

                len: usize,
                data: [n]T,

                fn init() Arr {
                    return .{ .len = 0, .data = undefined };
                }

                fn capacity(_: *Arr) usize {
                    return n;
                }

                fn constSlice(self: *const Arr) []const T {
                    return self.data[0..self.len];
                }

                fn slice(self: *Arr) []T {
                    return self.data[0..self.len];
                }

                fn appendAssumeCapacity(self: *Arr, item: T) void {
                    self.data[self.len] = item;
                    self.len += 1;
                }
            };
        }

        pub const Bumped = struct {
            const Layers = BoundedArray(BumpedLayer, 4);

            w: u6,
            seed: u64,
            layers: Layers,
            fallback_table: RibbonTable,

            pub fn deinit(self: *Bumped, allocator: std.mem.Allocator) void {
                for (self.layers.slice()) |*layer| {
                    layer.deinit(allocator);
                }
                self.fallback_table.deinit(allocator);
                self.* = undefined;
            }

            pub fn lookup(self: *const Bumped, key: Key) u64 {
                const hash1 = hasher(self.seed, key);
                const hash2 = hasher(self.seed + 1, key);
                for (self.layers.constSlice()) |layer| {
                    const h = splitHash(hash1, hash2, layer.table.n, self.w);
                    if (layer.lookup(h.i, h.c)) |result| {
                        return result;
                    }
                }
                const h = splitHash(hash1, hash2, self.fallback_table.n, self.w);
                return self.fallback_table.lookup(h.i, h.c);
            }

            pub fn bits(self: Bumped) usize {
                var result = self.fallback_table.bits();
                for (self.layers.constSlice()) |layer| {
                    result += layer.bits();
                }
                return result;
            }

            pub fn writeTo(self: *const Bumped, w: *std.Io.Writer) !void {
                try w.writeInt(u64, self.w, endian);
                try w.writeInt(u64, self.seed, endian);
                try w.writeInt(u64, self.layers.len, endian);
                for (self.layers.constSlice()) |layer| {
                    try layer.writeTo(w);
                }
                try self.fallback_table.writeTo(w);
            }

            pub fn readFrom(r: *std.Io.Reader) !Bumped {
                const w = try r.takeInt(u64, endian);
                const seed = try r.takeInt(u64, endian);
                const layers_len = try r.takeInt(u64, endian);
                var layers = Layers.init();
                for (0..layers_len) |_| {
                    layers.appendAssumeCapacity(try BumpedLayer.readFrom(r));
                }
                const fallback_table = try RibbonTable.readFrom(r);
                return Bumped{
                    .w = @intCast(w),
                    .seed = seed,
                    .layers = layers,
                    .fallback_table = fallback_table,
                };
            }
        };

        pub const BumpedBuilder = struct {
            layer_builder: BumpedLayerBuilder,

            pub fn init(allocator: std.mem.Allocator, n: usize, eps: f64, opts: BuildOptions) error{OutOfMemory}!BumpedBuilder {
                var layer_builder = try BumpedLayerBuilder.init(allocator, n, eps, opts);
                errdefer layer_builder.deinit(allocator);

                return BumpedBuilder{ .layer_builder = layer_builder };
            }

            pub fn deinit(self: *BumpedBuilder, allocator: std.mem.Allocator) void {
                self.layer_builder.deinit(allocator);
                self.* = undefined;
            }

            pub fn insert(self: *BumpedBuilder, key: Key, value: u64) void {
                const hash1 = hasher(self.layer_builder.opts.seed, key);
                const hash2 = hasher(self.layer_builder.opts.seed + 1, key);
                self.layer_builder.insert(hash1, hash2, value);
            }

            pub fn build(self: *BumpedBuilder, allocator: std.mem.Allocator) error{ OutOfMemory, HashCollision }!Bumped {
                var layers = Bumped.Layers.init();
                errdefer {
                    for (layers.slice()) |*layer| {
                        layer.deinit(allocator);
                    }
                }

                while (layers.len < layers.capacity()) {
                    if (layers.len > 1 and self.layer_builder.input.items.len < 2048) {
                        // Other bother with the lower level if we have enough items.
                        break;
                    }

                    var layer = try self.layer_builder.build(allocator);
                    errdefer layer.deinit(allocator);

                    layers.appendAssumeCapacity(layer);
                }

                var fallback_table = try self.layer_builder.buildFallbackTable(allocator);
                errdefer fallback_table.deinit(allocator);

                return Bumped{
                    .w = self.layer_builder.opts.w,
                    .seed = self.layer_builder.opts.seed,
                    .layers = layers,
                    .fallback_table = fallback_table,
                };
            }
        };
    };
}

pub fn RibbonAutoHash(comptime Key: type) type {
    return Ribbon(Key, utils.autoHash(Key));
}

const testing = std.testing;
const Wyhash = std.hash.Wyhash;
const TestErrorSet = error{ OutOfMemory, HashCollision, TestExpectedEqual };

fn testRibbon(t: anytype) TestErrorSet!void {
    const settings = @TypeOf(t.*).settings;
    const valueSize = 8;
    settings.setValueSize(t, valueSize);
    settings.setBandWidth(t, 32);
    settings.setSeed(t, 100);
    try t.init();

    const seed = 0x0194f614c15227ba;

    {
        // Insert random data:
        var prng = std.Random.DefaultPrng.init(seed);
        const r = prng.random();

        for (0..t.n) |idx| {
            const value = r.uintLessThan(u64, @as(u64, 1) << valueSize);
            try t.insert(idx, value);
        }
    }

    try t.build();

    {
        // Look it up again:
        var prng = std.Random.DefaultPrng.init(seed);
        const r = prng.random();

        for (0..t.n) |idx| {
            const value = r.uintLessThan(u64, @as(u64, 1) << valueSize);
            try testing.expectEqual(value, t.lookup(idx));
        }
    }
}

const RibbonU64 = RibbonAutoHash(u64);

fn RibbonSettings(comptime Self: type) type {
    return struct {
        fn setValueSize(self: *Self, r: u6) void {
            self.r = r;
        }

        fn setBandWidth(self: *Self, w: u6) void {
            self.w = w;
        }

        fn setSeed(self: *Self, seed: u64) void {
            self.seed = seed;
        }

        fn options(self: *const Self) BuildOptions {
            return .{
                .r = self.r.?,
                .w = self.w.?,
                .seed = self.seed.?,
            };
        }
    };
}

const RibbonIncrementalTest = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    n: usize,

    r: ?u6 = null,
    w: ?u6 = null,
    seed: ?u64 = null,
    builder: ?RibbonU64.IncrementalBuilder = null,
    table: ?RibbonU64 = null,

    const settings = RibbonSettings(Self);

    fn deinit(self: *Self) void {
        if (self.builder) |*b| b.deinit(self.allocator);
        if (self.table) |*t| t.deinit(self.allocator);
    }

    fn init(self: *Self) !void {
        self.builder = try RibbonU64.IncrementalBuilder.init(self.allocator, self.n * 2, settings.options(self));
    }

    fn insert(self: *Self, key: u64, value: u64) !void {
        try self.builder.?.insert(key, value);
    }

    fn build(self: *Self) !void {
        self.table = try self.builder.?.build(self.allocator);
    }

    fn lookup(self: *Self, key: u64) u64 {
        return self.table.?.lookup(key);
    }
};

const RibbonIterativeTest = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    n: usize,

    r: ?u6 = null,
    w: ?u6 = null,
    seed: ?u64 = null,

    builder: ?RibbonU64.IterativeBuilder = null,
    table: ?RibbonU64 = null,

    const settings = RibbonSettings(Self);

    fn deinit(self: *Self) void {
        if (self.builder) |*b| b.deinit(self.allocator);
        if (self.table) |*t| t.deinit(self.allocator);
    }

    fn init(self: *Self) !void {
        self.builder = try RibbonU64.IterativeBuilder.init(self.allocator, self.n, self.seed.?);
    }

    fn insert(self: *Self, key: u64, value: u64) !void {
        self.builder.?.insert(key, value);
    }

    fn build(self: *Self) !void {
        self.table = try self.builder.?.build(self.allocator, settings.options(self));
    }

    fn lookup(self: *Self, key: u64) u64 {
        return self.table.?.lookup(key);
    }
};

const BumpedRibbonTest = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    n: usize,

    r: ?u6 = null,
    w: ?u6 = null,
    seed: ?u64 = null,

    builder: ?RibbonU64.BumpedBuilder = null,
    table: ?RibbonU64.Bumped = null,

    const settings = RibbonSettings(Self);

    fn deinit(self: *Self) void {
        if (self.builder) |*b| b.deinit(self.allocator);
        if (self.table) |*t| t.deinit(self.allocator);
    }

    fn init(self: *Self) !void {
        self.builder = try RibbonU64.BumpedBuilder.init(self.allocator, self.n, 0, settings.options(self));
    }

    fn insert(self: *Self, key: u64, value: u64) !void {
        self.builder.?.insert(key, value);
    }

    fn build(self: *Self) !void {
        self.table = try self.builder.?.build(self.allocator);
    }

    fn lookup(self: *Self, key: u64) u64 {
        return self.table.?.lookup(key);
    }
};

fn testRibbonIncremental(allocator: std.mem.Allocator) TestErrorSet!void {
    var t = RibbonIncrementalTest{ .allocator = allocator, .n = 100 };
    defer t.deinit();
    try testRibbon(&t);
}

fn testRibbonIterative(allocator: std.mem.Allocator) TestErrorSet!void {
    var t = RibbonIterativeTest{ .allocator = allocator, .n = 100 };
    defer t.deinit();
    try testRibbon(&t);
}

fn testBumpedRibbon(allocator: std.mem.Allocator) TestErrorSet!void {
    var t = BumpedRibbonTest{ .allocator = allocator, .n = 100 };
    defer t.deinit();
    try testRibbon(&t);
}

test "ribbon incremental" {
    try utils.testFailingAllocator(testRibbonIncremental);
}

test "ribbon iterative" {
    try utils.testFailingAllocator(testRibbonIterative);
}

test "bumped ribbon" {
    try utils.testFailingAllocator(testBumpedRibbon);
}
