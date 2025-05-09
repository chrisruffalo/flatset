package io.github.chrisruffalo.flatset;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FlatSet {

    static final int INITIAL_RECORD_SIZE = 20;
    static final int RECORD_SIZE_EXPANSION_AMOUNT = 4;
    private static final int BUFFER_SIZE = 1024 * 32; //32K

    final Path backing;
    MappedByteBuffer mmap;

    long entries;
    int recordSize;

    final ThreadLocal<byte[]> pool = new ThreadLocal<>();

    public FlatSet(Path backing) {
        this.backing = backing;
        this.recordSize = INITIAL_RECORD_SIZE;
        if (Files.exists(backing)) {
            try {
                Files.deleteIfExists(backing);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!Files.exists(backing.getParent())) {
            try {
                Files.createDirectories(backing);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public long load(final Path path) throws IOException {
        this.recordSize = 32; // Start with default
        this.entries = 0;

        byte[] buffer = new byte[BUFFER_SIZE];
        ByteBuffer lineBuffer = ByteBuffer.allocateDirect(recordSize);
        int lineLength = 0;

        try (
                InputStream fis = Files.newInputStream(path);
                FileChannel out = FileChannel.open(backing,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE)
        ) {
            int read;
            long fileOffset = 0;

            while ((read = fis.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    byte b = buffer[i];

                    if (b == '\n') {
                        // Pad with spaces
                        while (lineLength < recordSize) {
                            lineBuffer.put((byte)0);
                            lineLength++;
                        }

                        lineBuffer.flip(); // Switch to reading mode
                        out.position(fileOffset);
                        out.write(lineBuffer);
                        fileOffset += recordSize;

                        lineBuffer.clear();
                        lineLength = 0;
                        entries++;
                    } else if (b != '\r') {
                        if (lineLength == lineBuffer.capacity()) {
                            // Resize logic
                            int newRecordSize = nextRecordSize(recordSize + 1);
                            recordSize = resizeFileSet(out, newRecordSize);
                            ByteBuffer newBuffer = ByteBuffer.allocateDirect(recordSize);
                            lineBuffer.flip(); // Prepare to read from old
                            newBuffer.put(lineBuffer); // Copy contents
                            lineBuffer = newBuffer;
                            fileOffset = entries * recordSize; // need to adjust the fileOffset to match the previous projected record
                        }
                        lineBuffer.put(b);
                        lineLength++;
                    }
                }
            }

            // Write last line if missing newline
            if (lineLength > 0) {
                while (lineLength < recordSize) {
                    lineBuffer.put((byte)0);
                    lineLength++;
                }

                lineBuffer.flip();
                out.position(fileOffset);
                out.write(lineBuffer);
                entries++;
            }
        }

        return entries;
    }

    public void add(String value) throws IOException {
        int newRecordSize = value.length();

        try (FileChannel channel = FileChannel.open(backing, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            if (newRecordSize > recordSize) {
                recordSize = resizeFileSet(channel, nextRecordSize(newRecordSize));
            }

            long pos = entries * recordSize;
            if (pos + recordSize > channel.size()) {
                channel.truncate(pos + recordSize);
            }

            channel.position(pos);

            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            byte[] record = new byte[recordSize];
            System.arraycopy(bytes, 0, record, 0, newRecordSize);
            for (int i = newRecordSize; i < recordSize; i++) {
                record[i] = 0;
            }
            channel.write(java.nio.ByteBuffer.wrap(record));
            entries++;
            mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        }
    }

    private int resizeFileSet(FileChannel channel, int newRecordSize) throws IOException {
        //long oldSize = entries * recordSize;
        long newSize = entries * newRecordSize;

        channel.truncate(newSize); // Resize the file to new size
        channel.position(newSize - 1);
        //channel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));

        // Map the file to memory with the new size
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, newSize);

        byte[] buffer = new byte[recordSize];

        // Copy data from old position to new position
        for (long i = entries - 1; i >= 0; i--) {
            long oldPos = i * recordSize;
            long newPos = i * newRecordSize;

            map.position((int) oldPos);
            map.get(buffer, 0, recordSize);

            map.position((int) newPos);
            map.put(buffer, 0, recordSize);
            for (int j = recordSize; j < newRecordSize; j++) {
                map.put((byte) 0);
            }
        }

        return newRecordSize;
    }

    static int nextRecordSize(int value) {
        if (value < INITIAL_RECORD_SIZE) {
            return INITIAL_RECORD_SIZE;
        }
        return value + RECORD_SIZE_EXPANSION_AMOUNT;
    }

    public void sort() throws IOException {
        // need at least 2 entries to sort
        if (entries < 2) {
            return;
        }

        try (FileChannel channel = FileChannel.open(backing, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, recordSize * entries);

            byte[] a = new byte[recordSize];
            byte[] b = new byte[recordSize];

            for (long i = entries / 2 - 1; i >= 0; i--) {
                siftDown(a, b, i, entries - 1);
            }

            for (long end = entries - 1; end > 0; end--) {
                swap(a, b, 0, end);
                siftDown(a, b, 0, end - 1);
            }
        }
    }

    private void siftDown(byte[] a, byte[] b, long start, long end) throws IOException {
        long root = start;
        while (root * 2 + 1 <= end) {
            long child = root * 2 + 1;
            long swap = root;

            read(this.mmap, child, a);
            read(this.mmap, swap, b);

            if (compare(a, b) > 0) {
                swap = child;
                read(this.mmap, swap, b);
            }

            child += 1;
            if (child <= end) {
                read(this.mmap, child, a);
                if (compare(a, b) > 0) {
                    swap = child;
                    read(this.mmap, swap, b);
                }
            }

            if (swap == root) {
                return;
            } else {
                if (root != child) {
                    read(this.mmap, root, a);
                }
                write(this.mmap, root, b);
                write(this.mmap, swap, a);
                root = swap;
            }
        }
    }

    private int compare(byte[] a, byte[] b) {
        for (int k = 0; k < recordSize; k++) {
            int cmp = (a[k] & 0xFF) - (b[k] & 0xFF);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    private void swap(byte[] a, byte[] b, long i, long j) throws IOException {
        if (i == j) return;
        read(this.mmap, i, a);
        read(this.mmap, j, b);
        write(this.mmap, i, b);
        write(this.mmap, j, a);
    }

    private void read(MappedByteBuffer source, long recordIndex, byte[] buffer) {
        int pos = (int) (recordIndex * recordSize);
        source.position(pos);
        source.get(buffer, 0, recordSize);
    }

    private void write(MappedByteBuffer source, long recordIndex, byte[] buffer) {
        int pos = (int) (recordIndex * recordSize);
        source.position(pos);
        source.put(buffer, 0, recordSize);
    }

    private synchronized MappedByteBuffer getMmap() {
        if (mmap == null) {
            try (FileChannel channel = FileChannel.open(backing, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, recordSize * entries);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return mmap;
    }

    public long search(String query) {
        // can't find anything in an empty list
        if (entries < 1) {
            return -1;
        }

        // if the length of the query is greater
        // than that of every record it can't be
        // contained in the set
        if (query.length() > recordSize) {
            return -1;
        }

        // doing this keeps it _reasonably_ thread safe
        // but i'm not entirely sure
        final MappedByteBuffer map = getMmap().duplicate();

        byte[] buffer = pool.get();
        if (buffer == null || buffer.length < recordSize) {
            buffer = new byte[recordSize];
            pool.set(buffer);
        }

        long low = 0;
        long high = entries - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;
            read(map, mid, buffer);

            int cmp = compareRecordToQuery(buffer, query);
            if (cmp > 0) {
                low = mid + 1;
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    private int compareRecordToQuery(byte[] record, String query) {
        int queryLength = query.length();
        int limit = Math.min(recordSize, queryLength);

        for (int i = 0; i < limit; i++) {
            char qc = query.charAt(i);
            int rc = record[i] & 0xFF;
            if (qc != rc) {
                return qc - rc;
            }
        }

        if (queryLength < recordSize && record[queryLength] != 0) {
            return -1;
        }

        return 0;
    }
}