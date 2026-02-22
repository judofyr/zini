const std = @import("std");
const builtin = @import("builtin");
const zini = @import("zini");
const parg = @import("parg");

const HashFn = zini.pthash.BytesHashFn(zini.DictArray);
const HashRibbon = zini.ribbon.Ribbon([]const u8, std.hash.Wyhash.hash);
const StringDict = zini.StringDict;
const endian = builtin.cpu.arch.endian();

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

fn printStats(table: anytype, n: usize) !void {
    const bits = table.bits() + @bitSizeOf(@TypeOf(table));
    std.debug.print("  seed: {}\n", .{table.seed});
    std.debug.print("  bits: {}\n", .{bits});
    std.debug.print("  bits/n: {d}\n", .{@as(f64, @floatFromInt(bits)) / @as(f64, @floatFromInt(n))});
}

pub fn build(allocator: std.mem.Allocator, io: std.Io, p: anytype) !void {
    var w: u6 = 32;
    var input: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var seed: ?u64 = null;
    var eps: f64 = 0;

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
                    w = @intCast(try std.fmt.parseInt(usize, val, 10));
                } else if (flag.isLong("eps")) {
                    const val = p.nextValue() orelse @panic("value required");
                    eps = try std.fmt.parseFloat(f64, val);
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

    if (seed == null) {
        io.random(std.mem.asBytes(&seed));
    }

    var max_val: u64 = 0;
    var n: usize = 0;

    var iter = std.mem.tokenizeScalar(u8, data, '\n');
    while (iter.next()) |line| {
        var split = std.mem.splitScalar(u8, line, ',');
        _ = split.next().?; // the key
        const value = try std.fmt.parseInt(u64, split.next().?, 10);
        max_val = @max(max_val, value);
        n += 1;
    }

    const r: u6 = @intCast(std.math.log2_int_ceil(u64, max_val + 1));

    std.debug.print("\n", .{});
    std.debug.print("Building table for r={} value bits and eps={}...\n", .{ r, eps });

    const opts = zini.ribbon.BuildOptions{
        .r = r,
        .w = w,
        .seed = seed.?,
    };

    var builder = try HashRibbon.BumpedBuilder.init(allocator, n, eps, opts);
    defer builder.deinit(allocator);

    iter = std.mem.tokenizeScalar(u8, data, '\n');
    while (iter.next()) |line| {
        var split = std.mem.splitScalar(u8, line, ',');
        const key = split.next().?; // the key
        const value = try std.fmt.parseInt(u64, split.next().?, 10);
        builder.insert(key, value);
    }

    var table = try builder.build(allocator);
    defer table.deinit(allocator);

    std.debug.print("\n", .{});
    std.debug.print("Successfully built table:\n", .{});
    try printStats(table, n);

    if (output) |o| {
        std.debug.print("\n", .{});
        std.debug.print("Writing to {s}\n", .{o});
        var buf: [4096]u8 = undefined;
        const outfile = try std.Io.Dir.cwd().createFile(io, o, .{});
        defer outfile.close(io);

        var writer = outfile.writer(io, &buf);

        try writer.interface.writeInt(u64, n, endian);
        try table.writeTo(&writer.interface);

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
                } else if (flag.isShort("b") or flag.isLong("benchmark")) {
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

    var reader = std.Io.Reader.fixed(buf);
    const n = try reader.takeInt(u64, endian);
    var table = try HashRibbon.Bumped.readFrom(&reader);
    std.debug.print("\n", .{});

    std.debug.print("Successfully loaded hash function:\n", .{});
    try printStats(table, n);
    std.debug.print("\n", .{});

    if (key) |k| {
        std.debug.print("Looking up key={s}:\n", .{k});
        const value = table.lookup(k);
        try stdout.interface.print("{}\n", .{value});
        try stdout.interface.flush();

        if (bench) {
            const m = 1000;
            std.debug.print("\nBenchmarking...\n", .{});
            const start_ts = std.Io.Clock.awake.now(io);
            var i: usize = 0;
            // TODO: Is this actually a good way of benchmarking?
            while (i < m) : (i += 1) {
                std.mem.doNotOptimizeAway(table.lookup(k));
            }
            const dur = start_ts.untilNow(io, .awake);
            std.debug.print("{} ns/read (avg of {} iterations)\n", .{ @divTrunc(dur.toNanoseconds(), n), n });
        }
    }
}
