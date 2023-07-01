//! Implements the "darray" data structure which provides constant-time
//! select(i) operation for _dense_ bit sets. Roughly half of the items
//! should be set for this to be practical.
//!
//! See "Practical Entropy-Compressed Rank/Select Dictionary" by Daisuke Okanohara and Kunihiko Sadakane.
//!
//! The code is heavily based on https://github.com/jermp/pthash/blob/master/include/encoders/darray.hpp.

const std = @import("std");
const utils = @import("./utils.zig");

const BitSet = std.bit_set.DynamicBitSet;

pub fn DArray(comptime val: bool) type {
    return struct {
        const Self = @This();

        const block_size: usize = 1024;
        const subblock_size: usize = 32;
        const max_in_block_distance: usize = 1 << 16;

        const BlockPosition = packed struct {
            is_overflow: bool,
            pos: u63,
        };

        block_inventory: []BlockPosition,
        subblock_inventory: []u16,
        overflow_positions: []u64,

        pub fn init(allocator: std.mem.Allocator, bit_set: std.bit_set.DynamicBitSetUnmanaged) !Self {
            var cur_block_positions = std.ArrayListUnmanaged(u63){};
            defer cur_block_positions.deinit(allocator);

            var block_inventory = std.ArrayListUnmanaged(BlockPosition){};
            defer block_inventory.deinit(allocator);

            var subblock_inventory = std.ArrayListUnmanaged(u16){};
            defer subblock_inventory.deinit(allocator);

            var overflow_positions = std.ArrayListUnmanaged(u64){};
            defer overflow_positions.deinit(allocator);

            try cur_block_positions.ensureTotalCapacity(allocator, block_size);

            var iter = bit_set.iterator(.{ .kind = if (val) .set else .unset });
            while (iter.next()) |pos| {
                cur_block_positions.appendAssumeCapacity(@intCast(pos));
                if (cur_block_positions.items.len == block_size) {
                    try flushCurBlock(allocator, &cur_block_positions, &block_inventory, &subblock_inventory, &overflow_positions);
                }
            }

            if (cur_block_positions.items.len > 0) {
                try flushCurBlock(allocator, &cur_block_positions, &block_inventory, &subblock_inventory, &overflow_positions);
            }

            return Self{
                .block_inventory = try block_inventory.toOwnedSlice(allocator),
                .subblock_inventory = try subblock_inventory.toOwnedSlice(allocator),
                .overflow_positions = try overflow_positions.toOwnedSlice(allocator),
            };
        }

        // Reads a word, flipping all bits if we're in select0-mode.
        fn readWord(bit_set: std.bit_set.DynamicBitSetUnmanaged, idx: usize) u64 {
            var word = bit_set.masks[idx];
            if (!val) {
                word = ~word;
            }
            return word;
        }

        fn flushCurBlock(
            allocator: std.mem.Allocator,
            cur_block_positions: *std.ArrayListUnmanaged(u63),
            block_inventory: *std.ArrayListUnmanaged(BlockPosition),
            subblock_inventory: *std.ArrayListUnmanaged(u16),
            overflow_positions: *std.ArrayListUnmanaged(u64),
        ) !void {
            var fst = cur_block_positions.items[0];
            var lst = cur_block_positions.items[cur_block_positions.items.len - 1];
            if (lst - fst < max_in_block_distance) {
                try block_inventory.append(allocator, BlockPosition{ .is_overflow = false, .pos = fst });
                var i: usize = 0;
                while (i < cur_block_positions.items.len) : (i += subblock_size) {
                    try subblock_inventory.append(allocator, @intCast(cur_block_positions.items[i] - fst));
                }
            } else {
                var overflow_pos = overflow_positions.items.len;
                try block_inventory.append(allocator, BlockPosition{ .is_overflow = true, .pos = @intCast(overflow_pos) });
                for (cur_block_positions.items) |pos| {
                    try overflow_positions.append(allocator, pos);
                }
                var i: usize = 0;
                while (i < cur_block_positions.items.len) : (i += subblock_size) {
                    // This value isn't used, but we need to fill up the subblock.
                    try subblock_inventory.append(allocator, 0);
                }
            }
            cur_block_positions.clearRetainingCapacity();
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            allocator.free(self.block_inventory);
            allocator.free(self.subblock_inventory);
            allocator.free(self.overflow_positions);
            self.* = undefined;
        }

        /// Returns the position of the `idx`-nth set bit in the bit set.
        pub fn select(self: *const Self, bit_set: std.bit_set.DynamicBitSetUnmanaged, idx: usize) usize {
            const block = idx / block_size;
            const block_pos = self.block_inventory[block];

            if (block_pos.is_overflow) {
                return self.overflow_positions[block_pos.pos + (idx % block_size)];
            }

            const subblock = idx / subblock_size;
            const start_pos = block_pos.pos + self.subblock_inventory[subblock];
            var reminder = idx % subblock_size;
            if (reminder == 0) return start_pos;

            // Note: These assume the BitSet uses u64.
            var word_idx = start_pos >> 6;
            const word_shift: u6 = @intCast(start_pos & 63);

            var word = readWord(bit_set, word_idx);
            word &= @as(u64, @bitCast(@as(i64, -1))) << word_shift;

            while (true) {
                var popcount = @popCount(word);
                if (reminder < popcount) break;
                reminder -= popcount;
                word_idx += 1;
                word = readWord(bit_set, word_idx);
            }

            // TODO: this is probably not the best select_in_word algorithm

            var word_pos: usize = 0;

            while (true) {
                if (word & 1 == 1) {
                    if (reminder == 0) break;
                    reminder -= 1;
                }
                word_pos += 1;
                word >>= 1;
            }

            return (word_idx << 6) + word_pos;
        }

        pub fn bits(self: *const Self) u64 {
            return utils.bitSizeOfSlice(self.block_inventory) +
                utils.bitSizeOfSlice(self.subblock_inventory) +
                utils.bitSizeOfSlice(self.overflow_positions);
        }

        pub fn writeTo(self: *const Self, w: anytype) !void {
            try utils.writeSlice(w, self.block_inventory);
            try utils.writeSlice(w, self.subblock_inventory);
            try utils.writeSlice(w, self.overflow_positions);
        }

        pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
            var block_inventory = try utils.readSlice(stream, BlockPosition);
            var subblock_inventory = try utils.readSlice(stream, u16);
            var overflow_positions = try utils.readSlice(stream, u64);
            return Self{
                .block_inventory = @constCast(block_inventory),
                .subblock_inventory = @constCast(subblock_inventory),
                .overflow_positions = @constCast(overflow_positions),
            };
        }
    };
}

/// Provides select_0 support.
pub const DArray1 = DArray(true);

/// Provides select_0 support.
pub const DArray0 = DArray(false);

const testing = std.testing;

fn testBitSet(
    bit_set: *std.DynamicBitSet,
    positions: []usize,
) !void {
    var darr1 = try DArray1.init(testing.allocator, bit_set.unmanaged);
    defer darr1.deinit(testing.allocator);

    for (positions, 0..) |pos, idx| {
        try testing.expectEqual(pos, darr1.select(bit_set.unmanaged, idx));
    }

    // Now flip it and test select0(i):
    bit_set.toggleAll();

    var darr0 = try DArray0.init(testing.allocator, bit_set.unmanaged);
    defer darr0.deinit(testing.allocator);

    for (positions, 0..) |pos, idx| {
        try testing.expectEqual(pos, darr0.select(bit_set.unmanaged, idx));
    }
}

test "dense" {
    const seed = 0x0194f614c15227ba;
    var prng = std.rand.DefaultPrng.init(seed);
    const r = prng.random();

    var result = std.ArrayList(usize).init(testing.allocator);
    defer result.deinit();

    const n = 10000;

    var bit_set = try std.DynamicBitSet.initEmpty(testing.allocator, n);
    defer bit_set.deinit();

    var idx: usize = 0;
    while (idx < n) : (idx += 1) {
        if (r.boolean()) {
            try result.append(idx);
            bit_set.set(idx);
        }
    }

    try testBitSet(&bit_set, result.items);
}

test "sparse" {
    const seed = 0x0194f614c15227ba;
    var prng = std.rand.DefaultPrng.init(seed);
    const r = prng.random();

    var result = std.ArrayList(usize).init(testing.allocator);
    defer result.deinit();

    const n = 100000;

    var bit_set = try std.DynamicBitSet.initEmpty(testing.allocator, n);
    defer bit_set.deinit();

    var idx: usize = 0;
    while (idx < n) : (idx += 1) {
        if (r.uintLessThan(u64, 100) == 0) {
            try result.append(idx);
            bit_set.set(idx);
        }
    }

    try testBitSet(&bit_set, result.items);
}
