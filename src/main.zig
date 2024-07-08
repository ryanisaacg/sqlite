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

        // Read the first page
        const first_page_buf = try allocator.alloc(u8, header.page_size);
        defer allocator.free(first_page_buf);
        _ = try file.seekTo(100);
        _ = try file.read(first_page_buf);

        const first_page_view = DBPageView.new(first_page_buf);

        try std.io.getStdOut().writer().print("database page size: {}\n", .{header.page_size});
        //try std.io.getStdOut().writer().print("database page count: {}\n", .{header.page_count});
        try std.io.getStdOut().writer().print("number of tables: {}\n", .{first_page_view.cell_count});
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

const DBPageView = struct {
    page_type: DBPageType,
    cell_count: u16,
    cell_data: []u8,

    pub fn new(buffer: []u8) DBPageView {
        const page_type = switch (buffer[0]) {
            0x02 => DBPageType.InteriorIndex,
            0x05 => DBPageType.InteriorTable,
            0x0A => DBPageType.LeafIndex,
            0x0D => DBPageType.LeafTable,
            else => unreachable,
        };
        // TODO: freeblocks
        const cell_count = std.mem.readInt(u16, buffer[3..5], .big);
        const cell_content_start = std.mem.readInt(u16, buffer[5..7], .big);
        _ = cell_content_start; // TODO: what to do with this?
        // TODO: fragmented free bytes
        // TODO: right-most pointer for interior b trees
        // LearnZig TODO: how do I create a conditional constant?
        var header_size: usize = undefined;
        if (page_type == DBPageType.InteriorTable) {
            header_size = 12;
        } else {
            header_size = 8;
        }
        const cell_data = buffer[header_size..];

        return DBPageView{ .page_type = page_type, .cell_count = cell_count, .cell_data = cell_data };
    }
};

const DBPageType = enum {
    InteriorTable,
    LeafTable,
    InteriorIndex,
    LeafIndex,
};
