//! This file implements the ideas from "Fast Succinct Retrieval and Approximate Membership using Ribbon".

const std = @import("std");
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const CompactArray = @import("./CompactArray.zig");
const utils = @import("./utils.zig");

fn bitParity(num: u64) u64 {
    return @popCount(num) % 2;
}

const RibbonTable = struct {
    const Self = @This();

    data: CompactArray,

    pub fn init(allocator: std.mem.Allocator, r: u6, n: usize) !Self {
        return Self{
            .data = try CompactArray.init(allocator, r, n),
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

            const j = @intCast(u6, @ctz(c_));
            i_ += j;
            c_ >>= j;
        }
        return result;
    }

    pub fn bits(self: *const Self) u64 {
        return self.data.bits();
    }

    pub fn writeTo(self: *const Self, w: anytype) !void {
        try self.data.writeTo(w);
    }

    pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
        var data = try CompactArray.readFrom(stream);
        return Self{ .data = data };
    }
};

pub const RibbonBandingSystem = struct {
    const Self = @This();

    n: usize,
    c: CompactArray,
    b: CompactArray,

    pub fn init(allocator: std.mem.Allocator, n: usize, r: u6, w: u6) !Self {
        var c = try CompactArray.init(allocator, w, n);
        errdefer c.deinit(allocator);

        var b = try CompactArray.init(allocator, r, n);
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

            const j = @intCast(u6, @ctz(c_));
            c_ >>= j;
            i_ += j;
        }
    }

    pub fn build(self: Self, allocator: std.mem.Allocator) !RibbonTable {
        const r = self.getValueSize();

        var table = try RibbonTable.init(allocator, r, self.n);
        errdefer table.deinit(allocator);

        var state = try allocator.alloc(u64, r);
        defer allocator.free(state);
        std.mem.set(u64, state, 0);

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

            table.data.setFromZero(i, resultRow);
        }

        return table;
    }
};

const HashResult = struct {
    i: u64,
    c: u64,
};

const BuildOptions = struct {
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

        fn splitHash(hash1: u64, hash2: u64, n: usize, w: u6) HashResult {
            var i = hash1 % (n - w);
            const c_mask = ((@as(u64, 1) << w) - 1);
            var c = (hash2 & c_mask) | 1;
            return .{ .i = i, .c = c };
        }

        n: usize,
        w: u6,
        seed: u64,
        table: RibbonTable,

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.table.deinit(allocator);
            self.* = undefined;
        }

        pub fn lookup(self: *const Self, key: Key) u64 {
            const h = hashKey(self.seed, key, self.n, self.w);
            return self.table.lookup(h.i, h.c);
        }

        pub fn bits(self: *const Self) u64 {
            return self.table.bits();
        }

        pub fn writeTo(self: *const Self, w: anytype) !void {
            try w.writeIntNative(u64, self.n);
            try w.writeIntNative(u64, self.w);
            try w.writeIntNative(u64, self.seed);
            try self.table.writeTo(w);
        }

        pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
            var r = stream.reader();
            const n = try r.readIntNative(u64);
            const w = try r.readIntNative(u64);
            const seed = try r.readIntNative(u64);
            const table = try RibbonTable.readFrom(stream);
            return Self{
                .n = n,
                .w = @intCast(u6, w),
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
                var system = try RibbonBandingSystem.init(allocator, n, opts.r, opts.w);

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
                var table = try self.system.build(allocator);

                return Self{
                    .n = self.n,
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
                var input = try std.ArrayListUnmanaged(Input).initCapacity(allocator, n);

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

                    var table = try system.build(allocator);

                    return Self{
                        .n = m,
                        .w = opts.w,
                        .seed = opts.seed,
                        .table = table,
                    };
                }

                return error.HashCollision;
            }
        };
    };
}

pub fn RibbonAutoHash(comptime Key: type) type {
    return Ribbon(Key, utils.autoHash(Key));
}

const testing = std.testing;
const Wyhash = std.hash.Wyhash;

fn testRibbon(t: anytype) !void {
    const valueSize = 8;
    t.setValueSize(valueSize);
    t.setBandWidth(32);
    t.setSeed(100);
    try t.init();

    const seed = 0x0194f614c15227ba;

    {
        // Insert random data:
        var prng = std.rand.DefaultPrng.init(seed);
        const r = prng.random();

        for (0..t.n) |idx| {
            const value = r.uintLessThan(u64, @as(u64, 1) << valueSize);
            try t.insert(idx, value);
        }
    }

    try t.build();

    {
        // Look it up again:
        var prng = std.rand.DefaultPrng.init(seed);
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

        fn options(self: Self) BuildOptions {
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

    usingnamespace RibbonSettings(Self);

    fn deinit(self: *Self) void {
        if (self.builder) |*b| b.deinit(self.allocator);
        if (self.table) |*t| t.deinit(self.allocator);
    }

    fn init(self: *Self) !void {
        self.builder = try RibbonU64.IncrementalBuilder.init(self.allocator, self.n * 2, self.options());
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

    usingnamespace RibbonSettings(Self);

    fn deinit(self: *Self) void {
        if (self.builder) |*b| b.deinit(self.allocator);
        if (self.table) |*t| t.deinit(self.allocator);
    }

    fn init(self: *Self) !void {
        self.builder = try RibbonU64.IterativeBuilder.init(self.allocator, self.n, self.options().seed);
    }

    fn insert(self: *Self, key: u64, value: u64) !void {
        self.builder.?.insert(key, value);
    }

    fn build(self: *Self) !void {
        self.table = try self.builder.?.build(self.allocator, self.options());
    }

    fn lookup(self: *Self, key: u64) u64 {
        return self.table.?.lookup(key);
    }
};

fn testRibbonIncremental(allocator: std.mem.Allocator) !void {
    var t = RibbonIncrementalTest{ .allocator = allocator, .n = 100 };
    defer t.deinit();
    try testRibbon(&t);
}

fn testRibbonIterative(allocator: std.mem.Allocator) !void {
    var t = RibbonIterativeTest{ .allocator = allocator, .n = 100 };
    defer t.deinit();
    try testRibbon(&t);
}

test "ribbon incremental" {
    try utils.testFailingAllocator(testRibbonIncremental);
}

test "ribbon iterative" {
    try utils.testFailingAllocator(testRibbonIterative);
}
