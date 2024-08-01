const std = @import("std");
const zini = @import("zini");
const parg = @import("parg");

const usage =
    \\USAGE
    \\  {s} <filename>
    \\
    \\A simple tool which reads a list of numbers (u64) from a file,
    \\compresses them using Elias-Fano, and reports the number of
    \\bytes it would take.
    \\
;

fn fail(comptime msg: []const u8, args: anytype) noreturn {
    std.debug.print("error: ", .{});
    std.debug.print(msg, args);
    std.debug.print("\n", .{});
    std.posix.exit(1);
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

    var filename: ?[]const u8 = null;

    while (p.next()) |token| {
        switch (token) {
            .flag => |flag| {
                if (flag.isLong("help") or flag.isShort("h")) {
                    std.debug.print(usage, .{program_name});
                    std.process.exit(0);
                } else {
                    fail("uknown flag: {s}", .{flag.name});
                }
            },
            .arg => |arg| {
                if (filename == null) {
                    filename = arg;
                } else {
                    fail("uknown argument: {s}", .{arg});
                }
            },
            .unexpected_value => |val| fail("uknown argument: {s}", .{val}),
        }
    }

    const f = filename orelse fail("filename expected as argument", .{});
    var file = try std.fs.cwd().openFile(f, .{});
    defer file.close();

    var counting_file = std.io.countingReader(file.reader());

    var numbers = std.ArrayList(u64).init(allocator);
    defer numbers.deinit();

    std.debug.print("Reading {s}\n", .{f});

    var r = counting_file.reader();
    while (true) {
        var buf: [32]u8 = undefined;
        const line = r.readUntilDelimiter(&buf, '\n') catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        const num = try std.fmt.parseInt(u64, line, 10);
        try numbers.append(num);
    }

    std.mem.sort(u64, numbers.items, {}, std.sort.asc(u64));

    std.debug.print("Compressing {} numbers ({} bytes)...\n", .{ numbers.items.len, counting_file.bytes_read });

    var encoded = try zini.EliasFano.encode(allocator, numbers.items);
    defer encoded.deinit(allocator);

    var counter = std.io.countingWriter(std.io.null_writer);
    try encoded.writeTo(counter.writer());

    std.debug.print("The data would compress to: {} bytes\n", .{counter.bytes_written});
}
