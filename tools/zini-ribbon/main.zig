const std = @import("std");
const zini = @import("zini");
const parg = @import("parg");

const HashFn = zini.pthash.BytesHashFn(zini.DictArray);
const HashRibbon = zini.ribbon.Ribbon([]const u8, std.hash.Wyhash.hash);
const StringDict = zini.StringDict;

const usage =
    \\USAGE
    \\  {s} [build | lookup] <options>
    \\
    \\COMMAND: build
    \\  Builds Ribbon table for plain text file.
    \\ 
    \\  -i, --input <file>
    \\  -o, --output <file>
    \\  -w <int>
    \\  -s, --seed <int>
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
        const leaked = gpa.deinit();
        if (leaked) @panic("memory leaked");
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

fn printStats(table: HashRibbon, dict: StringDict, n: usize) !void {
    const bits = table.bits() + @bitSizeOf(HashRibbon) + dict.bits() + @bitSizeOf(StringDict);
    std.debug.print("  seed: {}\n", .{table.seed});
    std.debug.print("  bits: {}\n", .{bits});
    std.debug.print("  bits/n: {d}\n", .{@intToFloat(f64, bits) / @intToFloat(f64, n)});
}

pub fn build(allocator: std.mem.Allocator, p: anytype) !void {
    var w: u6 = 32;
    var input: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var seed: ?u64 = null;

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
                } else if (flag.isShort("w")) {
                    const val = p.nextValue() orelse @panic("value required");
                    w = @intCast(u6, try std.fmt.parseInt(usize, val, 10));
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

    if (seed == null) {
        try std.os.getrandom(std.mem.asBytes(&seed));
    }

    var builder = try HashRibbon.IterativeBuilder.init(allocator, 32, seed.?);
    defer builder.deinit(allocator);

    var value_dict_builder = try StringDict.Builder.init(allocator);
    defer value_dict_builder.deinit();

    var max_val: u64 = 0;
    var n: usize = 0;

    var iter = std.mem.tokenize(u8, data, "\n");
    while (iter.next()) |line| {
        var split = std.mem.split(u8, line, " ");
        var key = split.next().?;
        var value = split.next().?;
        const val_idx = try value_dict_builder.intern(value);
        max_val = @max(val_idx, max_val);
        try builder.insertWithAllocator(allocator, key, val_idx);
        n += 1;
    }

    std.debug.print("\n", .{});
    std.debug.print("Building table...\n", .{});
    var table = try builder.build(allocator, .{
        .r = @intCast(u6, std.math.log2_int_ceil(u64, max_val + 1)),
        .w = w,
        .seed = seed.?,
    });
    defer table.deinit(allocator);

    var value_dict = try value_dict_builder.build();
    defer value_dict.deinit(allocator);

    std.debug.print("\n", .{});
    std.debug.print("Successfully built table:\n", .{});
    try printStats(table, value_dict, n);

    if (output) |o| {
        std.debug.print("\n", .{});
        std.debug.print("Writing to {s}\n", .{o});
        const outfile = try std.fs.cwd().createFile(o, .{});
        defer outfile.close();

        try outfile.writer().writeIntNative(u64, n);
        try table.writeTo(outfile.writer());
        try value_dict.writeTo(outfile.writer());
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
    var n = try fbs.reader().readIntNative(u64);
    var table = try HashRibbon.readFrom(&fbs);
    var dict = try StringDict.readFrom(&fbs);
    std.debug.print("\n", .{});

    std.debug.print("Successfully loaded hash function:\n", .{});
    try printStats(table, dict, n);
    std.debug.print("\n", .{});

    if (key) |k| {
        std.debug.print("Looking up key={s}:\n", .{k});
        var idx = table.lookup(k);
        try stdout.print("index={} value={s}\n", .{ idx, dict.get(idx) });

        if (bench) {
            const m = 1000;
            std.debug.print("\nBenchmarking...\n", .{});
            var timer = try std.time.Timer.start();
            const start = timer.lap();
            var i: usize = 0;
            // TODO: Is this actually a good way of benchmarking?
            while (i < m) : (i += 1) {
                std.mem.doNotOptimizeAway(dict.get(table.lookup(k)));
            }
            const end = timer.read();
            const dur = end - start;
            std.debug.print("{} ns/read (avg of {} iterations)\n", .{ dur / m, m });
        }
    }
}
