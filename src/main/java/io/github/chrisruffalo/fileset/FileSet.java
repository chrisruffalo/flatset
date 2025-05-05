package io.github.chrisruffalo.fileset;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FileSet {

    final Path backing;
    MappedByteBuffer mmap;

    long entries;
    int recordSize;

    private final ThreadLocal<byte[]> threadBufferPool = new ThreadLocal<>();

    public FileSet(Path backing) {
        this.backing = backing;
    }

    public long load(final Path path) throws IOException {
        byte[] buffer = new byte[8192];
        int currentRecordSize = 0;

        // First pass: determine record size
        try (final FileInputStream fis = new FileInputStream(path.toFile())) {
            int read;
            while ((read = fis.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    if (buffer[i] == '\n') {
                        recordSize = Math.max(recordSize, currentRecordSize);
                        currentRecordSize = 0;
                    } else {
                        currentRecordSize++;
                    }
                }
            }
        }

        // Second pass: convert to fixed-width format
        this.entries = 0;
        try (
            final InputStream fis = Files.newInputStream(path);
            final OutputStream out = new BufferedOutputStream(Files.newOutputStream(this.backing))
        ) {
            int read;
            int linePos = 0;
            byte[] lineBuffer = new byte[recordSize];

            while ((read = fis.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    byte b = buffer[i];
                    if (b == '\n') {
                        for (int j = linePos; j < recordSize; j++) {
                            lineBuffer[j] = ' ';
                        }
                        out.write(lineBuffer, 0, recordSize);
                        entries++;
                        linePos = 0;
                    } else if (b != '\r') {
                        if (linePos < recordSize) {
                            lineBuffer[linePos++] = b;
                        }
                    }
                }
            }

            // Final line
            if (linePos > 0) {
                for (int j = linePos; j < recordSize; j++) {
                    lineBuffer[j] = ' ';
                }
                out.write(lineBuffer, 0, recordSize);
                entries++;
            }
        }

        return entries;
    }

    public void sort() throws IOException {
        // Memory map the backing file
        try (FileChannel channel = FileChannel.open(backing, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            mmap = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());

            byte[] a = new byte[recordSize];
            byte[] b = new byte[recordSize];

            // Heapify the array
            for (long i = entries / 2 - 1; i >= 0; i--) {
                siftDown(a, b, i, entries - 1);
            }

            // Sort the heap
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

            if (compare(a, b, child, swap) > 0) {
                swap = child;
            }
            if (child + 1 <= end && compare(a, b, child + 1, swap) > 0) {
                swap = child + 1;
            }

            if (swap == root) {
                return;
            } else {
                swap(a, b, root, swap);
                root = swap;
            }
        }
    }

    private int compare(byte[] a, byte[] b, long i, long j) throws IOException {
        read(i, a);
        read(j, b);
        return Arrays.compareUnsigned(a, 0, recordSize, b, 0, recordSize);
    }

    private void swap(byte[] a, byte[] b, long i, long j) throws IOException {
        if (i == j) return;
        read(i, a);
        read(j, b);
        write(i, b);
        write(j, a);
    }

    private void read(long recordIndex, byte[] buffer) {
        int pos = (int) (recordIndex * recordSize);
        mmap.position(pos);
        mmap.get(buffer, 0, recordSize);
    }

    private void write(long recordIndex, byte[] buffer) {
        int pos = (int) (recordIndex * recordSize);
        mmap.position(pos);
        mmap.put(buffer, 0, recordSize);
    }

    public long search(String query) {
        if (query.length() > recordSize) {
            return -1;
        }

        // get an element from the queue
        byte[] buffer = threadBufferPool.get();
        if (buffer == null) {
            buffer = new byte[recordSize];
            threadBufferPool.set(buffer);
        }

        long low = 0;
        long high = entries - 1;

        while (low <= high) {
            long mid = (low + high) >>> 1;
            read(mid, buffer);

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

        if (queryLength < recordSize && record[queryLength] != ' ') {
            return -1;
        }

        return 0;
    }
}
