const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const optimize = b.standardOptimizeOption(.{});
    const target = b.standardTargetOptions(.{});

    const zini = b.addModule("zini", .{
        .source_file = .{ .path = "src/main.zig" },
    });

    const tests = b.addTest(.{
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    const tests_run_step = b.addRunArtifact(tests);
    tests_run_step.has_side_effects = true;

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&tests_run_step.step);

    const parg = b.createModule(.{ .source_file = .{
        .path = "../parg/src/parser.zig",
    } });

    const pthash = b.addExecutable(.{
        .name = "zini-pthash",
        .root_source_file = .{ .path = "tools/zini-pthash/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    pthash.addModule("zini", zini);
    pthash.addModule("parg", parg);
    b.installArtifact(pthash);
}
