const std = @import("std");

pub const pthash = @import("./pthash.zig");

comptime {
    std.testing.refAllDecls(@This());
}
