const std = @import("std");

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

        const header = try DBHeader.read(file);

        // First page is metadata
        var page_type_buf: [1]u8 = undefined;
        _ = try file.seekTo(100);
        _ = try file.read(&page_type_buf);
        std.debug.assert(page_type_buf[0] == 0x0D);

        var table_count_buf: [2]u8 = undefined;
        _ = try file.seekBy(2);
        _ = try file.read(&table_count_buf);
        const table_count = std.mem.readInt(u16, &table_count_buf, .big);

        try std.io.getStdOut().writer().print("database page size: {}\n", .{header.page_size});
        try std.io.getStdOut().writer().print("number of tables: {}\n", .{table_count});
    }
}

const DBHeader = struct {
    page_size: u16,
    page_count: u32,

    fn read(file: std.fs.File) !DBHeader {
        var page_size_buf: [2]u8 = undefined;
        _ = try file.seekTo(16);
        _ = try file.read(&page_size_buf);
        const page_size = std.mem.readInt(u16, &page_size_buf, .big);

        var page_count_buf: [4]u8 = undefined;
        _ = try file.seekTo(28);
        _ = try file.read(&page_count_buf);
        const page_count = std.mem.readInt(u32, &page_count_buf, .big);

        // Skip the rest of the header for now
        _ = try file.seekTo(100);

        return DBHeader{ .page_size = page_size, .page_count = page_count };
    }
};
