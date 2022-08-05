/// CompactArray stores a list of n-bit integers packed tightly.
const std = @import("std");

const Self = @This();

const Int = u64;
const IntLog2 = std.math.Log2Int(Int);

data: []Int,
width: IntLog2,

/// Creates a new array that can store `n` values of `width` bits each.
pub fn init(allocator: std.mem.Allocator, width: IntLog2, n: usize) !Self {
    const m = std.math.divCeil(usize, width * n, @bitSizeOf(Int)) catch unreachable;

    const data = try allocator.alloc(Int, m);
    std.mem.set(Int, data, 0);

    return Self{
        .data = data,
        .width = width,
    };
}

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    allocator.free(self.data);
    self.* = undefined;
}

fn getMask(self: *const Self) u64 {
    return (@as(Int, 1) << self.width) - 1;
}

/// Returns the value stored at a given index.
pub fn get(self: *const Self, idx: usize) u64 {
    const pos = idx * self.width;
    const block = pos / @bitSizeOf(Int);
    const shift = @intCast(IntLog2, pos % @bitSizeOf(Int));

    if (@as(Int, shift) + self.width < @bitSizeOf(Int)) {
        return (self.data[block] >> shift) & self.getMask();
    } else {
        const res_shift = ~shift + 1; //  =:=  @bitSizeOf(Int) - shift;
        return (self.data[block] >> shift) | (self.data[block + 1] << res_shift & self.getMask());
    }
}

/// Sets a value at a given index with the assumption that the existing value was already zero.
pub fn setFromZero(self: *const Self, idx: usize, val: u64) void {
    const pos = idx * self.width;
    const block = pos / @bitSizeOf(Int);
    const shift = @intCast(IntLog2, pos % @bitSizeOf(Int));

    self.data[block] |= val << shift;

    if (shift > 0) {
        const res_shift = ~shift + 1; //  =:=  @bitSizeOf(Int) - shift;
        if (res_shift < self.width) {
            self.data[block + 1] |= val >> res_shift;
        }
    }
}

/// Encodes an array into the smallest compact array possible.
pub fn encode(allocator: std.mem.Allocator, data: []const u64) !Self {
    const width = @intCast(IntLog2, std.math.log2_int_ceil(u64, std.mem.max(u64, data)));
    var arr = try init(allocator, width, data.len);
    for (data) |val, idx| {
        arr.setFromZero(idx, val);
    }
    return arr;
}

const testing = std.testing;

test "basic" {
    const n = 100;
    const width = 5;
    const max_val = 30;

    var c = try Self.init(testing.allocator, width, n);
    defer c.deinit(testing.allocator);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        const value = (i * i) % max_val;
        c.setFromZero(i, value);
    }

    i = 0;
    while (i < n) : (i += 1) {
        const value = (i * i) % max_val;
        try testing.expectEqual(value, c.get(i));
    }
}

test "encode" {
    const vals = [_]u64{ 5, 2, 9, 100, 0, 5, 10, 90, 9, 1, 65, 10 };
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    // 100 fits in 6 bits. There's 12 elements. These 72 bits fit in 2 u64.
    try testing.expectEqual(@as(usize, 2), arr.data.len);

    for (vals) |val, idx| {
        try testing.expectEqual(val, arr.get(idx));
    }
}
