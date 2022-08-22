/// CompactArray stores a list of n-bit integers packed tightly.
const std = @import("std");

const utils = @import("./utils.zig");

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

pub fn bits(self: *const Self) usize {
    return self.data.len * @bitSizeOf(Int);
}

fn getMask(self: *const Self) u64 {
    return (@as(Int, 1) << self.width) - 1;
}

/// Returns the value stored at a given index.
pub fn get(self: *const Self, idx: usize) u64 {
    const pos = idx * self.width;
    const block = pos / @bitSizeOf(Int);
    const shift = @intCast(IntLog2, pos % @bitSizeOf(Int));

    if (@as(Int, shift) + self.width <= @bitSizeOf(Int)) {
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
    if (data.len == 0) return Self{ .data = &[_]Int{}, .width = 1 };

    const width = @intCast(IntLog2, std.math.log2_int(u64, std.mem.max(u64, data)) + 1);
    var arr = try init(allocator, width, data.len);
    for (data) |val, idx| {
        arr.setFromZero(idx, val);
    }
    return arr;
}

/// Writes the array into an std.io.Writer. This can be read using `readFrom`.
pub fn writeTo(self: *const Self, w: anytype) !void {
    try w.writeIntNative(Int, self.width);
    try utils.writeSlice(w, self.data);
}

/// Reads an array from a buffer. Note that this will not allocate, but will
/// instead create a new CompactArray which points directly to the data in
/// the buffer.
pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
    var r = stream.reader();
    var width = try r.readIntNative(Int);
    var data = try utils.readSlice(stream, Int);
    return Self{
        .width = @intCast(IntLog2, width),
        .data = data,
    };
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

test "encode empty" {
    var arr = try Self.encode(testing.allocator, &[_]u64{});
    defer arr.deinit(testing.allocator);
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

test "encode #2" {
    const vals = [_]u64{ 0, 0, 2, 0, 4, 0 };
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    for (vals) |val, idx| {
        try testing.expectEqual(val, arr.get(idx));
    }
}

test "encode #3" {
    const vals = [_]u64{255} ** 64;
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    for (vals) |val, idx| {
        try testing.expectEqual(val, arr.get(idx));
    }
}

test "write and read" {
    const vals = [_]u64{ 0, 0, 2, 0, 4, 0 };
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    // ensure alignment
    var buf = try testing.allocator.alignedAlloc(u8, @alignOf(u64), 100);
    defer testing.allocator.free(buf);

    {
        // Write
        var fbs = std.io.fixedBufferStream(buf);
        try arr.writeTo(fbs.writer());
    }

    {
        // Read
        var fbs = std.io.fixedBufferStream(@as([]const u8, buf));
        var arr2 = try Self.readFrom(&fbs);

        for (vals) |val, idx| {
            try testing.expectEqual(val, arr2.get(idx));
        }
    }
}
