//! EliasFano stores 64-bit _increasing_ numbers in a compact manner.

const std = @import("std");
const DArray1 = @import("./darray.zig").DArray1;
const CompactArray = @import("./CompactArray.zig");
const utils = @import("./utils.zig");
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;

const Self = @This();

high_bits: DynamicBitSetUnmanaged,
high_bits_select: DArray1,
low_bits: CompactArray,

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.high_bits.deinit(allocator);
    self.high_bits_select.deinit(allocator);
    self.low_bits.deinit(allocator);
    self.* = undefined;
}

pub fn encode(allocator: std.mem.Allocator, data: []const u64) !Self {
    const n = data.len;
    const u = data[data.len - 1];

    const l = if (u > data.len) std.math.log2_int(u64, u / data.len) + 1 else 0;
    const l_mask = (@as(u64, 1) << l) - 1;
    const max_h = u >> l;

    // We need to store `2^h-1` zeroes and `n` ones.
    var high_bits = try DynamicBitSetUnmanaged.initEmpty(allocator, max_h + n);

    var low_bits = try CompactArray.Mutable.init(allocator, l, data.len);

    for (data, 0..) |num, idx| {
        if (l > 0) {
            low_bits.setFromZero(idx, num & l_mask);
        }
        high_bits.set((num >> l) + idx);
    }

    return Self{
        .high_bits = high_bits,
        .high_bits_select = try DArray1.init(allocator, high_bits),
        .low_bits = low_bits.finalize(),
    };
}

pub fn get(self: *const Self, idx: usize) u64 {
    const h_bits = self.high_bits_select.select(self.high_bits, idx) - idx;
    const l = self.low_bits.width;
    if (l == 0) return h_bits;

    const l_bits = self.low_bits.get(idx);
    return (h_bits << l) | l_bits;
}

pub fn bits(self: *const Self) u64 {
    // We're poking into the internals of DynamicBitSet here...
    const masks = self.high_bits.masks;
    const len = (masks - 1)[0];
    return self.low_bits.bits() + self.high_bits_select.bits() + len * @bitSizeOf(usize);
}

pub fn bitsWithoutConstantAccess(self: *const Self) u64 {
    const masks = self.high_bits.masks;
    const len = (masks - 1)[0];
    return self.low_bits.bits() + len * @bitSizeOf(usize);
}

pub fn writeTo(self: *const Self, w: anytype) !void {
    const masks = self.high_bits.masks;
    const len = (masks - 1)[0];
    try utils.writeSlice(w, (masks - 1)[0..len]);
    try self.high_bits_select.writeTo(w);
    try self.low_bits.writeTo(w);
}

pub fn readFrom(r: *std.Io.Reader) !Self {
    const mask_arr = try utils.readSlice(r, usize);
    const high_bits = DynamicBitSetUnmanaged{ .masks = @constCast(mask_arr.ptr) + 1 };
    const high_bits_select = try DArray1.readFrom(r);
    const low_bits = try CompactArray.readFrom(r);
    return Self{
        .high_bits = high_bits,
        .high_bits_select = high_bits_select,
        .low_bits = low_bits,
    };
}

const testing = std.testing;

test "encode" {
    const seed = 0x0194f614c15227ba;
    var prng = std.Random.DefaultPrng.init(seed);
    const r = prng.random();

    const n = 100000;

    var result = try std.ArrayList(u64).initCapacity(testing.allocator, n);
    defer result.deinit(testing.allocator);

    var i: usize = 0;
    var prev: u64 = 0;
    while (i < n) : (i += 1) {
        const num = prev + r.uintLessThan(u64, 50);
        result.appendAssumeCapacity(num);
        prev = num;
    }

    var ef = try encode(testing.allocator, result.items);
    defer ef.deinit(testing.allocator);

    // Check that it matches
    for (result.items, 0..) |num, idx| {
        try testing.expectEqual(num, ef.get(idx));
    }
}
