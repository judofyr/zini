const std = @import("std");

pub fn writeSlice(w: anytype, arr: anytype) !void {
    const T = @TypeOf(arr[0]);
    try w.writeIntNative(u64, arr.len);
    const byte_len = arr.len * @sizeOf(T);
    if (byte_len == 0) return;
    try w.writeAll(@as([*]const u8, @ptrCast(&arr[0]))[0..byte_len]);
    // Make sure we're always at a 64-bit boundary.
    try w.writeByteNTimes(0, byte_len % @alignOf(u64));
}

pub fn readSlice(stream: *std.io.FixedBufferStream([]const u8), T: anytype) ![]const T {
    var r = stream.reader();
    var len = try r.readIntNative(u64);
    const byte_len = len * @sizeOf(T);
    if (byte_len == 0) return &[_]T{};
    const data = stream.buffer[stream.pos..][0..byte_len];
    stream.pos += byte_len;
    stream.pos += byte_len % @alignOf(u64);
    const cast_data: [*]const T = @ptrCast(@alignCast(&data[0]));
    return cast_data[0..len];
}

pub fn bitSizeOfSlice(arr: anytype) u64 {
    return arr.len * @bitSizeOf(@TypeOf(arr[0]));
}

pub fn autoHash(comptime Key: type) fn (seed: u64, key: Key) u64 {
    return struct {
        fn hash(seed: u64, key: Key) u64 {
            if (comptime std.meta.trait.hasUniqueRepresentation(Key)) {
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
        var failing_alloc = std.testing.FailingAllocator.init(std.testing.allocator, idx);

        try (t(failing_alloc.allocator()) catch |err| switch (err) {
            error.OutOfMemory => continue,
            else => err,
        });

        return;
    }
}
