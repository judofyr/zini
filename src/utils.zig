const std = @import("std");

pub fn writeSlice(w: anytype, arr: anytype) !void {
    const T = @TypeOf(arr[0]);
    try w.writeIntNative(u64, arr.len);
    const byte_len = arr.len * @sizeOf(T);
    if (byte_len == 0) return;
    try w.writeAll(@ptrCast([*]const u8, &arr[0])[0..byte_len]);
    // Make sure we're always at a 64-bit boundary.
    try w.writeByteNTimes(0, byte_len % @alignOf(u64));
}

pub fn readSlice(stream: *std.io.FixedBufferStream([]const u8), T: anytype) ![]T {
    var r = stream.reader();
    var len = try r.readIntNative(u64);
    const byte_len = len * @sizeOf(T);
    if (byte_len == 0) return &[_]T{};
    const data = stream.buffer[stream.pos..][0..byte_len];
    const aligned_data = @alignCast(@alignOf(T), &data[0]);
    stream.pos += byte_len;
    stream.pos += byte_len % @alignOf(u64);
    return @ptrCast([]T, @ptrCast([*]const T, aligned_data)[0..len]);
}
