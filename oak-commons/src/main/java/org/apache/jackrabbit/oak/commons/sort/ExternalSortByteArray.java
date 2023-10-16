package org.apache.jackrabbit.oak.commons.sort;

import org.apache.jackrabbit.oak.commons.Compression;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;

/**
 * Variation of ExternalSort that stores the lines read from intermediate files as byte arrays to avoid the conversion
 * from byte[] to String and then back.
 */
public class ExternalSortByteArray {
    public static <T> int mergeSortedFilesBinary(List<Path> files, BufferedOutputStream fbw, final Comparator<T> cmp,
                                                 boolean distinct, Compression algorithm,
                                                 Function<T, byte[]> typeToByteArray, Function<byte[], T> byteArrayToType)
            throws IOException {
        ArrayList<BinaryFileBufferBinary<T>> bfbs = new ArrayList<>();
        try {
            for (Path f : files) {
                InputStream in = algorithm.getInputStream(Files.newInputStream(f));
                BinaryFileBufferBinary<T> bfb = new BinaryFileBufferBinary<>(in, byteArrayToType);
                bfbs.add(bfb);
            }
            return mergeBinary(fbw, cmp, distinct, bfbs, typeToByteArray);
        } finally {
            for (BinaryFileBufferBinary<T> buffer : bfbs) {
                try {
                    buffer.close();
                } catch (Exception ignored) {
                }
            }
            for (Path f : files) {
                Files.deleteIfExists(f);
            }
        }
    }

    public static <T> int mergeBinary(BufferedOutputStream fbw, final Comparator<T> cmp, boolean distinct,
                                      List<BinaryFileBufferBinary<T>> buffers, Function<T, byte[]> typeToByteArray)
            throws IOException {
        PriorityQueue<BinaryFileBufferBinary<T>> pq = new PriorityQueue<>(
                11,
                (i, j) -> cmp.compare(i.peek(), j.peek())
        );
        for (BinaryFileBufferBinary<T> bfb : buffers) {
            if (!bfb.empty()) {
                pq.add(bfb);
            }
        }
        int rowcounter = 0;
        T lastLine = null;
        try (fbw) {
            while (!pq.isEmpty()) {
                BinaryFileBufferBinary<T> bfb = pq.poll();
                T r = bfb.pop();
                // Skip duplicate lines
                if (!distinct || lastLine == null || cmp.compare(r, lastLine) != 0) {
                    fbw.write(typeToByteArray.apply(r));
                    fbw.write('\n');
                    lastLine = r;
                }
                ++rowcounter;
                if (bfb.empty()) {
                    bfb.fbr.close();
                } else {
                    pq.add(bfb); // add it back
                }
            }
        } finally {
            for (BinaryFileBufferBinary<T> bfb : buffers) {
                bfb.close();
            }
        }
        return rowcounter;
    }

    /**
     * WARNING: Uses '\n' as a line separator, it will not work with other line separators.
     */
    public static class BinaryFileBufferBinary<T> {
        private final static int BUFFER_SIZE = 64 * 1024;
        public final InputStream fbr;
        private final Function<byte[], T> byteArrayToType;
        private T cache;
        private boolean empty;

        // Used to reassemble the lines read from the source input stream.
        private final ByteArrayOutputStream bais = new ByteArrayOutputStream();
        private final byte[] buffer = new byte[BUFFER_SIZE];
        private int bufferPos = 0;
        private int bufferLimit = 0;

        public BinaryFileBufferBinary(InputStream r, Function<byte[], T> byteArrayToType)
                throws IOException {
            this.fbr = r;
            this.byteArrayToType = byteArrayToType;
            reload();
        }

        public boolean empty() {
            return this.empty;
        }

        private void reload() throws IOException {
            try {
                byte[] line = readLine();
                this.cache = byteArrayToType.apply(line);
                this.empty = this.cache == null;
            } catch (EOFException oef) {
                this.empty = true;
                this.cache = null;
            }
        }

        private boolean bufferIsEmpty() {
            return bufferPos >= bufferLimit;
        }

        /*
         * Read a line from the source input as a byte array. This is adapted from the implementation of
         * BufferedReader#readLine() but without converting the line to a String.
         */
        private byte[] readLine() throws IOException {
            bais.reset();

            for (; ; ) {
                if (bufferIsEmpty()) {
                    bufferLimit = fbr.read(buffer);
                    bufferPos = 0;
                }
                if (bufferIsEmpty()) { /* EOF */
                    // Buffer is still empty even after trying to read from input stream. We have reached the EOF
                    // Return whatever is left on the bais
                    if (bais.size() == 0) {
                        return null;
                    } else {
                        return bais.toByteArray();
                    }
                }
                // Start reading a new line
                int startByte = bufferPos;
                while (!bufferIsEmpty()) {
                    byte c = buffer[bufferPos];
                    bufferPos++;
                    if (c == '\n') { /* EOL */
                        int lineSegmentSize = bufferPos - startByte - 1; // exclude \n
                        if (bais.size() == 0) {
                            // There is no partial data on the bais, which means that the whole line is in the
                            // buffer. In this case, we can extract the line directly from the buffer without
                            // copying first to the bais
                            if (lineSegmentSize == 0) {
                                return null;
                            } else {
                                // Copy the line from the buffer to a new byte array and return it
                                byte[] line = new byte[lineSegmentSize];
                                System.arraycopy(buffer, startByte, line, 0, lineSegmentSize);
                                return line;
                            }
                        } else {
                            // The first section of the line is in the bais, the remainder in the buffer. Finish
                            // reassembling the line in the bais and return it
                            bais.write(buffer, startByte, lineSegmentSize);
                            if (bais.size() == 0) {
                                return null;
                            } else {
                                return bais.toByteArray();
                            }
                        }
                    }
                }
                // Reached the end of the buffer. Copy whatever is left in the buffer and read more from the source stream
                bais.write(buffer, startByte, bufferPos - startByte);
            }
        }

        public void close() throws IOException {
            this.fbr.close();
        }

        public T peek() {
            if (empty()) {
                return null;
            }
            return this.cache;
        }

        public T pop() throws IOException {
            T answer = peek();
            reload();
            return answer;
        }
    }
}
