const std = @import("std");

const DB_HEADER_SIZE = 100;
const PageIndex = u32;

pub const DB = struct {
    const PageCache = std.AutoHashMap(PageIndex, DBPage);

    // misc
    allocator: std.mem.Allocator,
    file: std.fs.File,

    // database
    info: DBHeader,
    page_cache: PageCache,

    pub fn init(allocator: std.mem.Allocator, file: std.fs.File) !DB {
        const header = try DBHeader.read(file);
        const page_cache = PageCache.init(allocator);

        return DB{ .info = header, .allocator = allocator, .page_cache = page_cache, .file = file };
    }

    pub fn count_tables(self: *DB) !PageIndex {
        return self.count_tables_recur(0);
    }

    fn count_tables_recur(self: *DB, page_idx: PageIndex) !PageIndex {
        const page = try self.load_page(page_idx);
        switch (page.page_type) {
            .LeafTable => return page.cell_count,
            .LeafIndex => unreachable,
            .InteriorIndex => unreachable,
            .InteriorTable => {},
        }
        // Handle interior table pages
        var table_count: PageIndex = 0;
        for (0..page.cell_count) |idx| {
            const cell_ptr = page.cell(@intCast(idx));
            const cell_buf = cell_ptr[0..4];
            const child_page_idx = std.mem.readInt(PageIndex, cell_buf, .big);
            table_count += try self.count_tables_recur(child_page_idx);
        }
        return table_count;
    }

    fn load_page(self: *DB, page_idx: PageIndex) !DBPageView {
        std.debug.assert(page_idx < self.info.page_count);

        if (self.page_cache.get(page_idx)) |page| {
            return page.view;
        } else {
            _ = try self.file.seekTo(DB_HEADER_SIZE + self.info.page_size * page_idx);
            const page_buf = try self.allocator.alloc(u8, self.info.page_size);
            _ = try self.file.read(page_buf);
            const view = DBPageView.new(page_buf);
            const page = DBPage{ .buffer = page_buf, .view = view };
            _ = try self.page_cache.put(page_idx, page);
            return view;
        }
    }

    pub fn deinit(self: *DB) void {
        var iter = self.page_cache.valueIterator();
        while (iter.next()) |db_page| {
            self.allocator.free(db_page.buffer);
        }
        self.page_cache.deinit();
    }
};

const DBHeader = struct {
    page_size: u16,
    page_count: u32,

    fn read(file: std.fs.File) !DBHeader {
        _ = try file.seekTo(0);

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

const DBPage = struct {
    buffer: []u8, // TODO: merge with DBPageView
    view: DBPageView,
};

const DBPageView = struct {
    page_type: DBPageType,
    cell_count: u16,
    cell_offsets: [*]u16,
    cell_content_start: u16,
    cell_data: []u8,
    buffer: []u8,

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
        const cell_offsets_buf = buffer[header_size..(header_size + cell_count * 2)];
        const cell_offsets: [*]u16 = @ptrCast(@alignCast(cell_offsets_buf));

        return DBPageView{
            .page_type = page_type,
            .cell_count = cell_count,
            .cell_offsets = cell_offsets,
            .cell_data = cell_data,
            .buffer = buffer,
            .cell_content_start = cell_content_start,
        };
    }

    pub fn cell(self: *const DBPageView, cell_idx: u16) [*]u8 {
        _ = std.io.getStdOut().writer().print("{}\n", .{cell_idx}) catch {};
        std.debug.assert(cell_idx < self.cell_count);
        const offset = self.cell_offsets[cell_idx];
        @breakpoint();
        _ = std.io.getStdOut().writer().print("{}\n", .{self.cell_content_start}) catch {};
        _ = std.io.getStdOut().writer().print("{}\n", .{offset}) catch {};
        const ptr: [*]u8 = @ptrCast(&self.buffer[offset]);
        return ptr;
    }
};

const DBPageType = enum {
    InteriorTable,
    LeafTable,
    InteriorIndex,
    LeafIndex,
};
