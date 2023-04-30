const std = @import("std");

const StringDict = @This();

dict: []const u8,

pub fn deinit(self: *StringDict, allocator: std.mem.Allocator) void {
    allocator.free(self.dict);
    self.* = undefined;
}

pub fn writeTo(self: *const StringDict, w: anytype) !void {
    try w.writeIntNative(u64, self.dict.len);
    try w.writeAll(self.dict);
}

pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !StringDict {
    var r = stream.reader();
    const len = try r.readIntNative(u64);
    const dict = stream.buffer[stream.pos..][0..len];
    stream.pos += len;
    return StringDict{
        .dict = dict,
    };
}

pub fn bits(self: *const StringDict) u64 {
    return self.dict.len * 8;
}

pub fn get(self: *const StringDict, idx: u64) []const u8 {
    const len = self.dict[idx];
    return self.dict[idx + 1 ..][0..len];
}

pub const Builder = struct {
    dict_values: std.ArrayList(u8),
    dict_positions: std.StringHashMap(usize),

    pub fn init(allocator: std.mem.Allocator) !Builder {
        return Builder{
            .dict_values = std.ArrayList(u8).init(allocator),
            .dict_positions = std.StringHashMap(usize).init(allocator),
        };
    }

    pub fn deinit(self: *Builder) void {
        self.dict_values.deinit();
        self.dict_positions.deinit();
        self.* = undefined;
    }

    pub fn intern(self: *Builder, key: []const u8) !u64 {
        var result = try self.dict_positions.getOrPut(key);
        if (!result.found_existing) {
            result.value_ptr.* = self.dict_values.items.len;
            try self.dict_values.append(@intCast(u8, key.len));
            for (key) |byte| {
                try self.dict_values.append(byte);
            }
        }
        return result.value_ptr.*;
    }

    pub fn build(self: *Builder) !StringDict {
        return StringDict{
            .dict = try self.dict_values.toOwnedSlice(),
        };
    }
};
