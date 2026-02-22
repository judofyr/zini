const std = @import("std");
const zini = @import("zini");
const parg = @import("parg");

const HashFn = zini.pthash.BytesHashFn(zini.DictArray);
const StringDict = zini.StringDict;

const usage =
    \\USAGE
    \\  {s} [build | lookup] <options>
    \\
    \\COMMAND: build
    \\  Builds hash function for plain text file.
    \\ 
    \\  -i, --input <file>
    \\  -o, --output <file>
    \\  -c <int>
    \\  -a, --alpha <float>
    \\  -s, --seed <int>
    \\  -d, --dict
    \\
    \\COMMAND: lookup
    \\
    \\  -i, --input <file>
    \\  -k, --key <key>
    \\  -b, --benchmark
    \\
;

fn fail(comptime msg: []const u8, args: anytype) noreturn {
    std.debug.print("error: ", .{});
    std.debug.print(msg, args);
    std.debug.print("\n", .{});
    std.process.exit(1);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var p = try parg.parseProcess(init, .{});
    defer p.deinit();

    const program_name = p.nextValue() orelse @panic("no executable name");

    while (p.next()) |token| {
        switch (token) {
            .flag => |flag| {
                fail("uknown flag: {s}", .{flag.name});
            },
            .arg => |arg| {
                if (std.mem.eql(u8, arg, "lookup")) {
                    return lookup(allocator, init.io, &p);
                } else if (std.mem.eql(u8, arg, "build")) {
                    return build(allocator, init.io, &p);
                } else {
                    fail("uknown argument: {s}", .{arg});
                }
            },
            .unexpected_value => |val| fail("uknown argument: {s}", .{val}),
        }
    }

    std.debug.print(usage, .{program_name});
}

fn printHashStats(hash: HashFn, dict: ?StringDict, arr: ?zini.DictArray) !void {
    const bits = hash.bits() + @bitSizeOf(HashFn);
    std.debug.print("  seed: {}\n", .{hash.seed});
    std.debug.print("  bits: {}\n", .{bits});
    std.debug.print("  bits/n: {d}\n", .{@as(f64, @floatFromInt(bits)) / @as(f64, @floatFromInt(hash.n))});
    std.debug.print("\n", .{});

    if (dict != null) {
        const dict_size = dict.?.bits() + @bitSizeOf(StringDict) + arr.?.bits() + @bitSizeOf(zini.DictArray);
        std.debug.print("File contains dictionary as well:\n", .{});
        std.debug.print("  bits: {}\n", .{dict_size});
        std.debug.print("  bits/n: {d}\n", .{@as(f64, @floatFromInt(dict_size)) / @as(f64, @floatFromInt(hash.n))});
        std.debug.print("\n", .{});

        const total_bits = bits + dict_size;

        std.debug.print("Combined:\n", .{});
        std.debug.print("  bits: {}\n", .{total_bits});
        std.debug.print("  bits/n: {d}\n", .{@as(f64, @floatFromInt(total_bits)) / @as(f64, @floatFromInt(hash.n))});
        std.debug.print("\n", .{});
    }
}

pub fn build(allocator: std.mem.Allocator, io: std.Io, p: anytype) !void {
    var params = zini.pthash.Params{ .c = 7, .alpha = 0.95 };
    var input: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var seed: ?u64 = null;
    var build_dict: bool = false;

    while (p.next()) |token| {
        switch (token) {
            .flag => |flag| {
                if (flag.isShort("i") or flag.isLong("input")) {
                    const val = p.nextValue() orelse fail("-i/--input requires value", .{});
                    input = val;
                } else if (flag.isShort("o") or flag.isLong("output")) {
                    const val = p.nextValue() orelse @panic("value required");
                    output = val;
                } else if (flag.isShort("s") or flag.isLong("seed")) {
                    const val = p.nextValue() orelse @panic("value required");
                    seed = try std.fmt.parseInt(usize, val, 10);
                } else if (flag.isShort("c")) {
                    const val = p.nextValue() orelse @panic("value required");
                    params.c = try std.fmt.parseInt(usize, val, 10);
                } else if (flag.isShort("a") or flag.isLong("alpha")) {
                    const val = p.nextValue() orelse @panic("value required");
                    params.alpha = try std.fmt.parseFloat(f64, val);
                } else if (flag.isShort("d") or flag.isLong("dict")) {
                    build_dict = true;
                } else {
                    fail("uknown flag: {s}", .{flag.name});
                }
            },
            .arg => |arg| fail("uknown argument: {s}", .{arg}),
            .unexpected_value => |val| fail("uknown argument: {s}", .{val}),
        }
    }

    if (input == null) {
        fail("-i/--input is required", .{});
    }

    std.debug.print("Reading {s}...\n", .{input.?});
    var file = try std.Io.Dir.cwd().openFile(io, input.?, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    const data = try reader.interface.allocRemaining(allocator, .unlimited);
    defer allocator.free(data);

    var keys: std.ArrayList([]const u8) = .empty;
    defer keys.deinit(allocator);

    var iter = std.mem.tokenizeScalar(u8, data, '\n');
    while (iter.next()) |line| {
        var split = std.mem.splitScalar(u8, line, ' ');
        try keys.append(allocator, split.next().?);
    }

    std.debug.print("\n", .{});
    std.debug.print("Building hash function...\n", .{});
    var hash: HashFn = undefined;
    if (seed) |s| {
        hash = try HashFn.buildUsingSeed(allocator, keys.items, params, s);
    } else {
        hash = try HashFn.buildUsingRandomSeed(allocator, io, keys.items, params, 1000);
    }
    defer hash.deinit(allocator);

    var dict: ?StringDict = null;
    defer if (dict) |*d| d.deinit(allocator);

    var arr: ?zini.DictArray = null;
    defer if (arr) |*a| a.deinit(allocator);

    if (build_dict) {
        var dict_builder = try StringDict.Builder.init(allocator);
        defer dict_builder.deinit();

        var arr_slice = try allocator.alloc(u64, hash.n);
        defer allocator.free(arr_slice);

        iter = std.mem.tokenizeScalar(u8, data, '\n');
        while (iter.next()) |line| {
            var split = std.mem.splitScalar(u8, line, ' ');
            const key = split.next().?;
            const value = split.next().?;
            const key_idx = hash.get(key);
            const val_idx = try dict_builder.intern(value);
            arr_slice[key_idx] = val_idx;
        }

        dict = try dict_builder.build();
        arr = try zini.DictArray.encode(allocator, arr_slice);
    }

    std.debug.print("\n", .{});
    std.debug.print("Successfully built hash function:\n", .{});
    try printHashStats(hash, dict, arr);

    if (output) |o| {
        std.debug.print("Writing to {s}\n", .{o});
        const outfile = try std.Io.Dir.cwd().createFile(io, o, .{});
        defer outfile.close(io);

        var buf: [4096]u8 = undefined;
        var writer = outfile.writer(io, &buf);

        try hash.writeTo(&writer.interface);

        if (build_dict) {
            try dict.?.writeTo(&writer.interface);
            try arr.?.writeTo(&writer.interface);
        }

        try writer.interface.flush();
    }
}

pub fn lookup(allocator: std.mem.Allocator, io: std.Io, p: anytype) !void {
    var stdout_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writer(io, &stdout_buf);

    var input: ?[]const u8 = null;
    var key: ?[]const u8 = null;
    var bench: bool = false;

    while (p.next()) |token| {
        switch (token) {
            .flag => |flag| {
                if (flag.isShort("i") or flag.isLong("input")) {
                    const val = p.nextValue() orelse fail("-i/--input requires value", .{});
                    input = val;
                } else if (flag.isShort("k") or flag.isLong("key")) {
                    const val = p.nextValue() orelse fail("-k/--key requires value", .{});
                    key = val;
                } else if (flag.isShort("b") or flag.isLong("bench")) {
                    bench = true;
                } else {
                    fail("unknown flag: {s}", .{flag.name});
                }
            },
            .arg => |arg| fail("unexpected argument: {s}", .{arg}),
            .unexpected_value => |val| fail("unexpected argument: {s}", .{val}),
        }
    }

    if (input == null) {
        fail("-i/--input is required", .{});
    }

    std.debug.print("Reading {s}...\n", .{input.?});
    const buf = try std.Io.Dir.cwd().readFileAlloc(io, input.?, allocator, .unlimited);
    defer allocator.free(buf);

    var r = std.Io.Reader.fixed(buf);
    const hash = try HashFn.readFrom(&r);
    var dict: ?StringDict = null;
    var arr: ?zini.DictArray = null;

    if (r.end < r.buffer.len) {
        dict = try StringDict.readFrom(&r);
        arr = try zini.DictArray.readFrom(&r);
    }

    std.debug.print("\n", .{});

    std.debug.print("Successfully loaded hash function:\n", .{});
    try printHashStats(hash, dict, arr);

    if (key) |k| {
        std.debug.print("Looking up key={s}:\n", .{k});
        const h = hash.get(k);
        try stdout.interface.print("{}\n", .{h});
        if (dict) |d| {
            try stdout.interface.print("{s}\n", .{d.get(arr.?.get(h))});
        }

        if (bench) {
            const n = 1000;
            std.debug.print("\nBenchmarking...\n", .{});
            const start_ts = std.Io.Clock.awake.now(io);
            var i: usize = 0;
            // TODO: Is this actually a good way of benchmarking?
            while (i < n) : (i += 1) {
                std.mem.doNotOptimizeAway(hash.get(k));
            }
            const dur = start_ts.untilNow(io, .awake);
            std.debug.print("{} ns/read (avg of {} iterations)\n", .{ @divTrunc(dur.toNanoseconds(), n), n });
        }
    }

    try stdout.interface.flush();
}
