const std = @import("std");
const zini = @import("zini");
const parg = @import("parg");

const HashFn = zini.pthash.BytesHashFn(zini.DictArray);

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

fn printHashStats(hash: HashFn) !void {
    const bits = hash.bits() + @bitSizeOf(HashFn);
    std.debug.print("  seed: {}\n", .{hash.seed});
    std.debug.print("  bits: {}\n", .{bits});
    std.debug.print("  bits/n: {d}\n", .{@intToFloat(f64, bits) / @intToFloat(f64, hash.n)});
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

    std.debug.print("\n", .{});
    std.debug.print("Successfully built hash function:\n", .{});
    try printHashStats(hash);

    if (output) |o| {
        std.debug.print("\n", .{});
        std.debug.print("Writing to {s}\n", .{o});
        const outfile = try std.fs.cwd().createFile(o, .{});
        defer outfile.close();

        try hash.writeTo(outfile.writer());

        if (build_dict) {
            var dict_builder = try StringDictBuilder.init(allocator, hash.n);
            defer dict_builder.deinit();

            iter = std.mem.tokenize(u8, data, "\n");
            while (iter.next()) |line| {
                var split = std.mem.split(u8, line, " ");
                const key = split.next().?;
                const value = split.next().?;
                const idx = hash.get(key);
                try dict_builder.set(idx, value);
            }

            var dict = try dict_builder.build();
            defer dict.deinit(allocator);
            try dict.writeTo(outfile.writer());
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
    std.debug.print("\n", .{});

    std.debug.print("Successfully loaded hash function:\n", .{});
    try printHashStats(hash);
    std.debug.print("\n", .{});

    var dict: ?StringDict = null;

    if (fbs.pos < fbs.buffer.len) {
        dict = try StringDict.readFrom(&fbs);

        const dict_size = dict.?.bits() + @bitSizeOf(StringDict);
        std.debug.print("File contains dictionary as well:\n", .{});
        std.debug.print("  bits: {}\n", .{dict_size});
        std.debug.print("  bits/n: {d}\n", .{@intToFloat(f64, dict_size) / @intToFloat(f64, hash.n)});
        std.debug.print("\n", .{});

        const total_bits = hash.bits() + @bitSizeOf(HashFn) + dict_size;

        std.debug.print("Combined:\n", .{});
        std.debug.print("  bits: {}\n", .{total_bits});
        std.debug.print("  bits/n: {d}\n", .{@intToFloat(f64, total_bits) / @intToFloat(f64, hash.n)});
        std.debug.print("\n", .{});
    }

    if (key) |k| {
        std.debug.print("Looking up key={s}:\n", .{k});
        var h = hash.get(k);
        try stdout.print("{}\n", .{h});
        if (dict) |d| {
            try stdout.print("{s}\n", .{d.get(h)});
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

const StringDict = struct {
    dict: []const u8,
    arr: zini.DictArray,

    pub fn deinit(self: *StringDict, allocator: std.mem.Allocator) void {
        allocator.free(self.dict);
        self.arr.deinit(allocator);
        self.* = undefined;
    }

    pub fn writeTo(self: *const StringDict, w: anytype) !void {
        try self.arr.writeTo(w);
        try w.writeIntNative(u64, self.dict.len);
        try w.writeAll(self.dict);
    }

    pub fn readFrom(stream: *std.io.FixedBufferStream([]const u8)) !StringDict {
        var r = stream.reader();
        const arr = try zini.DictArray.readFrom(stream);
        const len = try r.readIntNative(u64);
        const dict = stream.buffer[stream.pos..][0..len];
        stream.pos += len;
        return StringDict{
            .dict = dict,
            .arr = arr,
        };
    }

    pub fn bits(self: *const StringDict) u64 {
        return self.arr.bits() + self.dict.len * 8;
    }

    pub fn get(self: *const StringDict, idx: u64) []const u8 {
        const dict_idx = self.arr.get(idx);
        const len = self.dict[dict_idx];
        return self.dict[dict_idx + 1 ..][0..len];
    }
};

const StringDictBuilder = struct {
    dict_values: std.ArrayList(u8),
    dict_positions: std.StringHashMap(usize),
    arr: []u64,

    pub fn init(allocator: std.mem.Allocator, n: usize) !StringDictBuilder {
        return StringDictBuilder{
            .dict_values = std.ArrayList(u8).init(allocator),
            .dict_positions = std.StringHashMap(usize).init(allocator),
            .arr = try allocator.alloc(u64, n),
        };
    }

    pub fn deinit(self: *StringDictBuilder) void {
        self.dict_values.allocator.free(self.arr);
        self.dict_values.deinit();
        self.dict_positions.deinit();
        self.* = undefined;
    }

    pub fn set(self: *StringDictBuilder, idx: usize, key: []const u8) !void {
        var result = try self.dict_positions.getOrPut(key);
        if (!result.found_existing) {
            result.value_ptr.* = self.dict_values.items.len;
            try self.dict_values.append(@intCast(u8, key.len));
            for (key) |byte| {
                try self.dict_values.append(byte);
            }
        }
        self.arr[idx] = result.value_ptr.*;
    }

    pub fn build(self: *StringDictBuilder) !StringDict {
        return StringDict{
            .dict = try self.dict_values.toOwnedSlice(),
            .arr = try zini.DictArray.encode(self.dict_values.allocator, self.arr),
        };
    }
};
