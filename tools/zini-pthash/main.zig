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
    std.os.exit(1);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const check = gpa.deinit();
        if (check == .leak) @panic("memory leaked");
    }

    const allocator = gpa.allocator();

    var p = try parg.parseProcess(allocator, .{});
    defer p.deinit();

    const program_name = p.nextValue() orelse @panic("no executable name");

    while (p.next()) |token| {
        switch (token) {
            .flag => |flag| {
                fail("uknown flag: {s}", .{flag.name});
            },
            .arg => |arg| {
                if (std.mem.eql(u8, arg, "lookup")) {
                    return lookup(allocator, &p);
                } else if (std.mem.eql(u8, arg, "build")) {
                    return build(allocator, &p);
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

pub fn build(allocator: std.mem.Allocator, p: anytype) !void {
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
    var file = try std.fs.cwd().openFile(input.?, .{});
    defer file.close();

    var data = try file.reader().readAllAlloc(allocator, 10 * 1024 * 1024);
    defer allocator.free(data);

    var keys = std.ArrayList([]const u8).init(allocator);
    defer keys.deinit();

    var iter = std.mem.tokenize(u8, data, "\n");
    while (iter.next()) |line| {
        var split = std.mem.split(u8, line, " ");
        try keys.append(split.next().?);
    }

    std.debug.print("\n", .{});
    std.debug.print("Building hash function...\n", .{});
    var hash = try HashFn.build(allocator, keys.items, params, seed);
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

        iter = std.mem.tokenize(u8, data, "\n");
        while (iter.next()) |line| {
            var split = std.mem.split(u8, line, " ");
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
        const outfile = try std.fs.cwd().createFile(o, .{});
        defer outfile.close();

        try hash.writeTo(outfile.writer());

        if (build_dict) {
            try dict.?.writeTo(outfile.writer());
            try arr.?.writeTo(outfile.writer());
        }
    }
}

pub fn lookup(allocator: std.mem.Allocator, p: anytype) !void {
    const stdout = std.io.getStdOut().writer();

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
    const buf = try std.fs.cwd().readFileAlloc(allocator, input.?, 10 * 1024 * 1024);
    defer allocator.free(buf);

    var fbs = std.io.fixedBufferStream(@as([]const u8, buf));
    const hash = try HashFn.readFrom(&fbs);
    var dict: ?StringDict = null;
    var arr: ?zini.DictArray = null;

    if (fbs.pos < fbs.buffer.len) {
        dict = try StringDict.readFrom(&fbs);
        arr = try zini.DictArray.readFrom(&fbs);
    }

    std.debug.print("\n", .{});

    std.debug.print("Successfully loaded hash function:\n", .{});
    try printHashStats(hash, dict, arr);

    if (key) |k| {
        std.debug.print("Looking up key={s}:\n", .{k});
        var h = hash.get(k);
        try stdout.print("{}\n", .{h});
        if (dict) |d| {
            try stdout.print("{s}\n", .{d.get(arr.?.get(h))});
        }

        if (bench) {
            const n = 1000;
            std.debug.print("\nBenchmarking...\n", .{});
            var timer = try std.time.Timer.start();
            const start = timer.lap();
            var i: usize = 0;
            // TODO: Is this actually a good way of benchmarking?
            while (i < n) : (i += 1) {
                std.mem.doNotOptimizeAway(hash.get(k));
            }
            const end = timer.read();
            const dur = end - start;
            std.debug.print("{} ns/read (avg of {} iterations)\n", .{ dur / n, n });
        }
    }
}
