/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons.sort;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.conditions.Validate;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    private final static int DEFAULT_BUFFER_SIZE = 16 * 1024;

    public static <T> void mergeSortedFilesBinary(List<Path> files, OutputStream fbw, final Comparator<T> cmp,
                                                  boolean distinct, Compression algorithm,
                                                  Function<T, byte[]> typeToByteArray, Function<byte[], T> byteArrayToType)
            throws IOException {
        mergeSortedFilesBinary(files, fbw, cmp, distinct, algorithm, typeToByteArray, byteArrayToType, DEFAULT_BUFFER_SIZE);
    }

    public static <T> void mergeSortedFilesBinary(List<Path> files, OutputStream fbw, final Comparator<T> cmp,
                                                  boolean distinct, Compression algorithm,
                                                  Function<T, byte[]> typeToByteArray, Function<byte[], T> byteArrayToType, int readBufferSize)
            throws IOException {
        ArrayList<BinaryFileBuffer<T>> bfbs = new ArrayList<>();
        try {
            for (Path f : files) {
                InputStream in = algorithm.getInputStream(Files.newInputStream(f));
                bfbs.add(new BinaryFileBuffer<>(in, byteArrayToType, readBufferSize));
            }
            mergeBinary(fbw, cmp, distinct, bfbs, typeToByteArray);
        } finally {
            for (BinaryFileBuffer<T> buffer : bfbs) {
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

    private static <T> int mergeBinary(OutputStream fbw, final Comparator<T> cmp, boolean distinct,
                                       List<BinaryFileBuffer<T>> buffers, Function<T, byte[]> typeToByteArray)
            throws IOException {
        PriorityQueue<BinaryFileBuffer<T>> pq = new PriorityQueue<>(
                11,
                (i, j) -> cmp.compare(i.peek(), j.peek())
        );
        for (BinaryFileBuffer<T> bfb : buffers) {
            if (!bfb.empty()) {
                pq.add(bfb);
            }
        }
        int rowcounter = 0;
        T lastLine = null;
        while (!pq.isEmpty()) {
            BinaryFileBuffer<T> bfb = pq.poll();
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
        return rowcounter;
    }

    /**
     * WARNING: Uses '\n' as a line separator, it will not work with other line separators.
     */
    private static class BinaryFileBuffer<T> {
        public final InputStream fbr;
        private final Function<byte[], T> byteArrayToType;
        private T cache;
        private boolean empty;

        // Used to reassemble the lines read from the source input stream.
        private final ByteArrayOutputStream bais = new ByteArrayOutputStream();
        private final byte[] buffer;
        private int bufferPos = 0;
        private int bufferLimit = 0;

        public BinaryFileBuffer(InputStream r, Function<byte[], T> byteArrayToType, int bufferSize)
                throws IOException {
            Validate.checkArgument(bufferSize > 1024, "Buffer size must be greater than 1024 bytes");
            this.fbr = r;
            this.byteArrayToType = byteArrayToType;
            this.buffer = new byte[bufferSize];
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
