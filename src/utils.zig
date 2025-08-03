const std = @import("std");
const builtin = @import("builtin");
const endian = builtin.cpu.arch.endian();

pub fn writeSlice(w: *std.io.Writer, arr: anytype) !void {
    const T = @TypeOf(arr[0]);
    try w.writeInt(u64, arr.len, endian);
    const byte_len = arr.len * @sizeOf(T);
    if (byte_len == 0) return;
    try w.writeAll(@as([*]const u8, @ptrCast(&arr[0]))[0..byte_len]);
    // Make sure we're always at a 64-bit boundary.
    const padding = (@alignOf(u64) - (byte_len % @alignOf(u64))) % @alignOf(u64);
    _ = try w.splatByte(0, padding);
}

pub fn readSlice(r: *std.Io.Reader, T: anytype) ![]const T {
    // Invariant: stream.pos should be 8-byte aligned before and after `readSlice`
    std.debug.assert(r.seek % @alignOf(u64) == 0);
    defer std.debug.assert(r.seek % @alignOf(u64) == 0);

    const len = try r.takeInt(u64, endian);
    const byte_len = len * @sizeOf(T);
    if (byte_len == 0) return &[_]T{};
    const data = r.buffer[r.seek..][0..byte_len];
    r.seek = std.mem.alignForward(usize, r.seek + byte_len, @alignOf(u64));
    const cast_data: [*]const T = @ptrCast(@alignCast(&data[0]));
    return cast_data[0..len];
}

pub fn bitSizeOfSlice(arr: anytype) u64 {
    return arr.len * @bitSizeOf(@TypeOf(arr[0]));
}

pub fn autoHash(comptime Key: type) fn (seed: u64, key: Key) u64 {
    return struct {
        fn hash(seed: u64, key: Key) u64 {
            if (comptime std.meta.hasUniqueRepresentation(Key)) {
                return std.hash.Wyhash.hash(seed, std.mem.asBytes(&key));
            } else {
                var hasher = std.hash.Wyhash.init(seed);
                std.hash.autoHash(&hasher, key);
                return hasher.final();
            }
        }
    }.hash;
}

pub fn testFailingAllocator(comptime t: fn (allocator: std.mem.Allocator) anyerror!void) !void {
    var idx: usize = 0;
    while (true) : (idx += 1) {
        var failing_alloc = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = idx });

        try (t(failing_alloc.allocator()) catch |err| switch (err) {
            error.OutOfMemory => continue,
            else => err,
        });

        return;
    }
}

const testing = std.testing;

test "readSlice / writeSlice must maintain 8-byte alignment" {
    var buf: [128]u8 align(8) = undefined;
    var writer = std.Io.Writer.fixed(&buf);

    try writeSlice(&writer, [_]u8{ 1, 2, 3 });
    try writeSlice(&writer, [_]u64{2});

    var read_stream = std.Io.Reader.fixed(&buf);

    try testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, try readSlice(&read_stream, u8));
    try testing.expectEqualSlices(u64, &.{2}, try readSlice(&read_stream, u64));
}
