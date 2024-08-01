const std = @import("std");

pub fn build(b: *std.Build) !void {
    const optimize = b.standardOptimizeOption(.{});
    const target = b.standardTargetOptions(.{});

    const zini = b.addModule("zini", .{
        .root_source_file = b.path("src/main.zig"),
    });

    const tests = b.addTest(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    const tests_run_step = b.addRunArtifact(tests);
    tests_run_step.has_side_effects = true;

    const coverage = b.option(bool, "test-coverage", "Generate test coverage") orelse false;
    if (coverage) {
        const runner = [_][]const u8{
            "kcov",
            "--include-path",
            ".",
            "coverage", // output dir
        };

        const dst = try tests_run_step.argv.addManyAt(b.allocator, 0, runner.len);
        for (runner, 0..) |arg, idx| {
            dst[idx] = .{ .bytes = b.dupe(arg) };
        }
    }

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&tests_run_step.step);

    const parg = b.dependency("parg", .{ .target = target, .optimize = optimize });

    const pthash = b.addExecutable(.{
        .name = "zini-pthash",
        .root_source_file = b.path("tools/zini-pthash/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    pthash.root_module.addImport("zini", zini);
    pthash.root_module.addImport("parg", parg.module("parg"));
    b.installArtifact(pthash);

    const ribbon = b.addExecutable(.{
        .name = "zini-ribbon",
        .root_source_file = b.path("tools/zini-ribbon/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    ribbon.root_module.addImport("zini", zini);
    ribbon.root_module.addImport("parg", parg.module("parg"));
    b.installArtifact(ribbon);

    const seqz = b.addExecutable(.{
        .name = "zini-seqz",
        .root_source_file = b.path("tools/zini-seqz/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    seqz.root_module.addImport("zini", zini);
    seqz.root_module.addImport("parg", parg.module("parg"));
    b.installArtifact(seqz);
}
