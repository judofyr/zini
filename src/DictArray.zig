//! DictArray stores a list of integers by placing the unique items in a separate
//! array and refering to indexes into that array.

const std = @import("std");
const CompactArray = @import("./CompactArray.zig");

const Self = @This();

dict: CompactArray,
arr: CompactArray,

pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
    self.dict.deinit(allocator);
    self.arr.deinit(allocator);
    self.* = undefined;
}

pub fn bits(self: *const Self) usize {
    return self.dict.bits() + self.arr.bits();
}

pub fn get(self: *const Self, idx: usize) u64 {
    return self.dict.get(self.arr.get(idx));
}

pub fn encode(allocator: std.mem.Allocator, data: []const u64) !Self {
    var dict: std.ArrayList(u64) = .empty;
    defer dict.deinit(allocator);

    var arr = try std.ArrayList(u64).initCapacity(allocator, data.len);
    defer arr.deinit(allocator);

    var mapping = std.hash_map.AutoHashMap(u64, usize).init(allocator);
    defer mapping.deinit();

    for (data) |val| {
        const result = try mapping.getOrPut(val);
        if (!result.found_existing) {
            result.value_ptr.* = dict.items.len;
            try dict.append(allocator, val);
        }
        try arr.append(allocator, result.value_ptr.*);
    }

    return Self{
        .dict = try CompactArray.encode(allocator, dict.items),
        .arr = try CompactArray.encode(allocator, arr.items),
    };
}

pub fn writeTo(self: *const Self, w: anytype) !void {
    try self.dict.writeTo(w);
    try self.arr.writeTo(w);
}

pub fn readFrom(r: *std.Io.Reader) !Self {
    const dict = try CompactArray.readFrom(r);
    const arr = try CompactArray.readFrom(r);
    return Self{
        .dict = dict,
        .arr = arr,
    };
}
