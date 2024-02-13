/// CompactArray stores a list of n-bit integers packed tightly.
const std = @import("std");
const builtin = @import("builtin");

const utils = @import("./utils.zig");

const Self = @This();

const Int = u64;
const IntLog2 = std.math.Log2Int(Int);
const endian = builtin.cpu.arch.endian();

data: []const Int,
width: IntLog2,

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    allocator.free(self.data);
    self.* = undefined;
}

pub fn bits(self: *const Self) usize {
    return utils.bitSizeOfSlice(self.data);
}

fn getMask(self: *const Self) u64 {
    return (@as(Int, 1) << self.width) - 1;
}

/// Returns the value stored at a given index.
pub fn get(self: *const Self, idx: usize) u64 {
    const pos = idx * self.width;
    const block = pos / @bitSizeOf(Int);
    const shift: IntLog2 = @intCast(pos % @bitSizeOf(Int));

    if (@as(Int, shift) + self.width <= @bitSizeOf(Int)) {
        return (self.data[block] >> shift) & self.getMask();
    } else {
        const res_shift = ~shift + 1; //  =:=  @bitSizeOf(Int) - shift;
        return (self.data[block] >> shift) | (self.data[block + 1] << res_shift & self.getMask());
    }
}

/// Encodes an array into the smallest compact array possible.
pub fn encode(allocator: std.mem.Allocator, data: []const u64) !Self {
    if (data.len == 0) return Self{ .data = &[_]Int{}, .width = 1 };

    const width: IntLog2 = @intCast(std.math.log2_int(u64, std.mem.max(u64, data)) + 1);
    var arr = try Mutable.init(allocator, width, data.len);
    for (data, 0..) |val, idx| {
        arr.setFromZero(idx, val);
    }
    return arr.finalize();
}

/// Writes the array into an std.io.Writer. This can be read using `readFrom`.
pub fn writeTo(self: *const Self, w: anytype) !void {
    try w.writeInt(Int, self.width, endian);
    try utils.writeSlice(w, self.data);
}

/// Reads an array from a buffer. Note that this will not allocate, but will
/// instead create a new CompactArray which points directly to the data in
/// the buffer.
pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !Self {
    var r = stream.reader();
    const width = try r.readInt(Int, endian);
    const data = try utils.readSlice(stream, Int);
    return Self{
        .width = @intCast(width),
        .data = data,
    };
}

pub const Mutable = struct {
    data: []Int,
    width: IntLog2,

    /// Creates a new array that can store `n` values of `width` bits each.
    pub fn init(allocator: std.mem.Allocator, width: IntLog2, n: usize) !Mutable {
        const m = std.math.divCeil(usize, width * n, @bitSizeOf(Int)) catch unreachable;

        const data = try allocator.alloc(Int, m);
        @memset(data, 0);

        return Mutable{
            .data = data,
            .width = width,
        };
    }

    pub fn deinit(self: *Mutable, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        self.* = undefined;
    }

    pub fn finalize(self: *Mutable) Self {
        const result = self.asImmutable();
        self.* = undefined;
        return result;
    }

    pub fn asImmutable(self: Mutable) Self {
        return Self{
            .data = self.data,
            .width = self.width,
        };
    }

    pub fn get(self: Mutable, idx: usize) u64 {
        return self.asImmutable().get(idx);
    }

    /// Sets a value at a given index with the assumption that the existing value was already zero.
    pub fn setFromZero(self: Mutable, idx: usize, val: u64) void {
        const pos = idx * self.width;
        const block = pos / @bitSizeOf(Int);
        const shift: IntLog2 = @intCast(pos % @bitSizeOf(Int));

        self.data[block] |= val << shift;

        if (shift > 0) {
            const res_shift = ~shift + 1; //  =:=  @bitSizeOf(Int) - shift;
            if (res_shift < self.width) {
                self.data[block + 1] |= val >> res_shift;
            }
        }
    }

    /// Sets a value at a given index to zero.
    pub fn setToZero(self: Mutable, idx: usize) void {
        const pos = idx * self.width;
        const block = pos / @bitSizeOf(Int);
        const shift: IntLog2 = @intCast(pos % @bitSizeOf(Int));

        // This is easier to understand with an example:
        //   block size=8 (this is actually 64 in our implementation)
        //   width=5
        //   shift=6
        //
        // Let "V" be a value bit and "P" a "padding bit" (other value).
        //
        // Block 1: VV PPPPPP
        // Block 2: PPPPP VVV

        // There's also the case where it _doesn't_ cross a block:
        //   shift=2
        //   Block 1: PP VVVVV PP

        // Here we need to make sure we don't zero out those upper paddings.
        const upper_mask = ~@as(Int, 0) << self.width << shift;
        const lower_mask = ((@as(Int, 1) << shift) - 1);

        // Clear out VV by AND-ing 00111111;
        self.data[block] &= lower_mask | upper_mask;

        if (shift > 0) {
            const res_shift = ~shift + 1; //  =:=  @bitSizeOf(Int) - shift;

            if (res_shift < self.width) {
                // res_shift in this example is 2 and thus width-res_shift = 3.
                // We then build the mask 11111000 by NOT-ing 00000111.

                self.data[block + 1] &= ~((@as(Int, 1) << (self.width - res_shift)) - 1);
            }
        }
    }
};

const testing = std.testing;

test "basic" {
    const n = 100;
    const width = 5;
    const max_val = 30;

    var c = try Self.Mutable.init(testing.allocator, width, n);
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

    for (vals, 0..) |val, idx| {
        try testing.expectEqual(val, arr.get(idx));
    }
}

test "encode #2" {
    const vals = [_]u64{ 0, 0, 2, 0, 4, 0 };
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    for (vals, 0..) |val, idx| {
        try testing.expectEqual(val, arr.get(idx));
    }
}

test "encode #3" {
    const vals = [_]u64{255} ** 64;
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    for (vals, 0..) |val, idx| {
        try testing.expectEqual(val, arr.get(idx));
    }
}

test "write and read" {
    const vals = [_]u64{ 0, 0, 2, 0, 4, 0 };
    var arr = try Self.encode(testing.allocator, &vals);
    defer arr.deinit(testing.allocator);

    // ensure alignment
    const buf = try testing.allocator.alignedAlloc(u8, @alignOf(u64), 100);
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

        for (vals, 0..) |val, idx| {
            try testing.expectEqual(val, arr2.get(idx));
        }
    }
}
