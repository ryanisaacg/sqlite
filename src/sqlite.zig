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
        return self.count_tables_recur(1);
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
            table_count += try self.count_tables_recur(page.table_interior_cell_data(@intCast(idx)));
        }
        table_count += try self.count_tables_recur(page.right_most);
        return table_count;
    }

    pub fn print_names(self: *DB) !void {
        try self.print_names_recur(1);
    }

    fn print_names_recur(self: *DB, page_idx: PageIndex) !void {
        const page = try self.load_page(page_idx);
        if (page.page_type == .LeafTable) {
            for (0..page.cell_count) |idx| {
                const cell = page.table_leaf_cell_data(@intCast(idx));
                var header_ptr: [*]u8 = @ptrCast(cell.header);
                var encoded_type: u64 = undefined;
                header_ptr = read_varint(header_ptr, &encoded_type);

                // TODO: helpers for managing leaf cell headers and contents

                std.debug.assert(encoded_type > 13);
                const type_len = (encoded_type - 13) / 2;
                const type_str = cell.contents[0..type_len];
                if (!std.mem.eql(u8, type_str, "table")) {
                    continue;
                }

                var name_varint: u64 = undefined;
                header_ptr = read_varint(header_ptr, &name_varint);
                const name_len = (name_varint - 13) / 2;
                const name = cell.contents[type_len .. type_len + name_len];

                if (!std.mem.eql(u8, name, "sqlite_sequence")) {
                    try std.io.getStdOut().writer().print("{s}\n", .{name});
                }
            }
        } else {
            for (0..page.cell_count) |idx| {
                try self.print_names_recur(page.table_interior_cell_data(@intCast(idx)));
            }
            try self.print_names_recur(page.right_most);
        }
    }

    fn load_page(self: *DB, page_idx: PageIndex) !DBPageView {
        std.debug.assert(page_idx < self.info.page_count);

        if (self.page_cache.get(page_idx)) |page| {
            return page.view;
        } else {
            const is_first_page = page_idx == 1;
            var page_offset = self.info.page_size * (page_idx - 1);
            if (is_first_page) {
                page_offset += DB_HEADER_SIZE;
            }
            _ = try self.file.seekTo(page_offset);
            const page_buf = try self.allocator.alloc(u8, self.info.page_size);
            _ = try self.file.read(page_buf);

            const view = DBPageView.new(page_buf, is_first_page);
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
    cell_offsets: []u8,
    cell_content_start: u16,
    cell_data: []u8,
    buffer: []u8,
    is_first_page: bool,
    right_most: PageIndex,

    pub fn new(buffer: []u8, is_first_page: bool) DBPageView {
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
        var right_most: PageIndex = undefined;
        if (page_type == DBPageType.InteriorTable) {
            header_size = 12;
            right_most = std.mem.readInt(PageIndex, buffer[8..12], .big);
        } else {
            header_size = 8;
        }
        const cell_data = buffer[header_size..];
        const cell_offsets_buf = buffer[header_size..(header_size + cell_count * 2)];

        return DBPageView{ .page_type = page_type, .cell_count = cell_count, .cell_offsets = cell_offsets_buf, .cell_data = cell_data, .buffer = buffer, .cell_content_start = cell_content_start, .is_first_page = is_first_page, .right_most = right_most };
    }

    pub fn cell_ptr(self: *const DBPageView, cell_idx: u16) [*]u8 {
        std.debug.assert(cell_idx < self.cell_count);
        var offset_buf: [2]u8 = undefined;
        std.mem.copyForwards(u8, &offset_buf, self.cell_offsets[cell_idx * 2 .. cell_idx * 2 + 2]);
        var offset = std.mem.readInt(u16, &offset_buf, .big);
        if (self.is_first_page) {
            offset -= 100;
        }
        const ptr: [*]u8 = @ptrCast(&self.buffer[@intCast(offset)]);
        return ptr;
    }

    pub fn table_interior_cell_data(self: *const DBPageView, cell_idx: u16) u32 {
        const cell_ptr_ = self.cell_ptr(cell_idx);
        const cell_buf = cell_ptr_[0..4];
        const child_page_idx = std.mem.readInt(PageIndex, cell_buf, .big);
        return child_page_idx;
    }

    pub fn table_leaf_cell_data(self: *const DBPageView, cell_idx: u16) TableLeafCellData {
        var cell_ptr_ = self.cell_ptr(@intCast(cell_idx));
        var cell_len: u64 = undefined;
        cell_ptr_ = read_varint(cell_ptr_, &cell_len);
        var rowid: u64 = undefined;
        cell_ptr_ = read_varint(cell_ptr_, &rowid);

        // Importantly header_len includes itself - don't move cell_ptr_
        var header_len: u64 = undefined;
        const after_header = read_varint(cell_ptr_, &header_len);
        const header_offset = @intFromPtr(after_header) - @intFromPtr(cell_ptr_);

        // TODO: care about header?
        // TODO: handle overflow

        const header = cell_ptr_[header_offset..header_len];
        const contents = cell_ptr_[header_len..cell_len];
        return .{ .header = header, .contents = contents, .rowid = rowid };
    }
};

const DBPageType = enum {
    InteriorTable,
    LeafTable,
    InteriorIndex,
    LeafIndex,
};

const TableLeafCellData = struct {
    header: []u8,
    contents: []u8,
    rowid: u64,
};

const ReadVarint = struct {
    value: u64,
    bytes_read: usize,
};

fn read_varint(source: [*]u8, value: *u64) [*]u8 {
    value.* = 0;
    var bytes_read: usize = 0;

    while (true) {
        const byte = source[bytes_read];
        bytes_read += 1;

        value.* <<= 7;
        value.* |= byte & 0x7F;

        if (value.* & 0x80 == 0) {
            break;
        }
    }

    return source + bytes_read;
}

//func ReadVarint(reader *bytes.Reader) (int64, int, error) {
//	var result int64
//	var bytesRead int
//
//	for {
//		// Read a single byte
//		b, err := reader.ReadByte()
//		if err != nil {
//			return 0, bytesRead, err
//		}
//
//		bytesRead++
//
//		result = result << 7 // FIXME: handle last byte?
//
//		// Combine the lower 7 bits into the result
//		result |= int64(b & 0x7F)
//
//		// Check if the MSB is set; if not, we are done
//		if b&0x80 == 0 {
//			break
//		}
//	}
//
//	return result, bytesRead, nil
//}
