const std = @import("std");
const sqlite = @import("sqlite.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 3) {
        try std.io.getStdErr().writer().print("Usage: {s} <database_file_path> <command>\n", .{args[0]});
        return;
    }

    const database_file_path: []const u8 = args[1];
    const command: []const u8 = args[2];

    if (std.mem.eql(u8, command, ".dbinfo")) {
        var file = try std.fs.cwd().openFile(database_file_path, .{});
        defer file.close();

        // You can use print statements as follows for debugging, they'll be visible when running tests.
        try std.io.getStdOut().writer().print("Logs from your src will appear here\n", .{});

        var db = try sqlite.DB.init(allocator, file);
        defer db.deinit();

        const table_count = try db.count_tables();

        try std.io.getStdOut().writer().print("database page size: {}\n", .{db.info.page_size});
        //try std.io.getStdOut().writer().print("database page count: {}\n", .{header.page_count});
        try std.io.getStdOut().writer().print("number of tables: {}\n", .{table_count});
    }
}
