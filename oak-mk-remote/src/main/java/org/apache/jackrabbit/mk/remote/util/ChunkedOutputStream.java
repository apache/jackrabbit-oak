/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.remote.util;

import java.io.FilterOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.jackrabbit.mk.remote.util.ChunkedInputStream.MAX_CHUNK_SIZE;

/**
 * Output stream that encodes and writes HTTP chunks.
 */
public class ChunkedOutputStream extends FilterOutputStream {

    /**
     * CR + LF combination.
     */
    private static final byte[] CRLF = "\r\n".getBytes();

    /**
     * Last chunk.
     */
    private static final byte[] LAST_CHUNK = "0000\r\n\r\n".getBytes();

    /**
     * Chunk prefix (length encoded as hexadecimal string).
     */
    private final byte[] prefix = new byte[4];

    /**
     * Chunk data.
     */
    private final byte[] data;

    /**
     * Current offset.
     */
    private int offset;

    /**
     * Create a new instance of this class.
     *
     * @param out underlying output stream.
     * @param size internal buffer size
     * @throws IllegalArgumentException if {@code size} is smaller than 1
     *         or bigger than {@code 65535}
     */
    public ChunkedOutputStream(OutputStream out, int size) {
        super(out);

        if (size < 1 || size > MAX_CHUNK_SIZE) {
            String msg = "Chunk size smaller than 1 or bigger than " + MAX_CHUNK_SIZE;
            throw new IllegalArgumentException(msg);
        }
        this.data = new byte[size];
    }

    /**
     * Create a new instance of this class.
     *
     * @param out underlying output stream.
     */
    public ChunkedOutputStream(OutputStream out) {
        this(out, MAX_CHUNK_SIZE);
    }

    /* (non-Javadoc)
     * @see java.io.FilterOutputStream#write(int)
     */
    public void write(int b) throws IOException {
        if (offset == data.length) {
            writeChunk();
        }
        data[offset++] = (byte) (b & 0xff);
    }

    /* (non-Javadoc)
     * @see java.io.FilterOutputStream#write(byte[], int, int)
     */
    public void write(byte[] b, int off, int len) throws IOException {
        int written = 0;
        while (written < len) {
            if (offset == data.length) {
                writeChunk();
            }
            int available = Math.min(len - written, data.length - offset);
            System.arraycopy(b, off + written, data, offset, available);
            written += available;
            offset += available;
        }
    }

    /**
     * Writes the contents of the internal buffer as chunk to the underlying
     * output stream.
     *
     * @throws IOException if an error occurs
     */
    private void writeChunk() throws IOException {
        toHexString(offset, prefix);
        out.write(prefix);
        out.write(CRLF);
        out.write(data, 0, offset);
        out.write(CRLF);
        offset = 0;
    }

    /**
     * Convert an integer into a byte array, consisting of its hexadecimal
     * representation.
     *
     * @param n integer
     * @param b byte array
     */
    private static void toHexString(int n, byte[] b) {
        for (int i = b.length - 1; i >= 0; i--) {
            int c = n & 0x0f;
            if (c >= 0 && c <= 9) {
                c += '0';
            } else {
                c += 'A' - 10;
            }
            b[i] = (byte) c;
            n >>= 4;
        }
    }

    /**
     * Flush the contents of the internal buffer to the underlying output
     * stream as a chunk if it is non-zero. Never do that for a zero-size
     * chunk as this would indicate EOF.
     *
     * @see java.io.FilterOutputStream#flush()
     */
    public void flush() throws IOException {
        if (offset > 0) {
            writeChunk();
        }
        super.flush();
    }
    
    /**
     * Recycle this output stream.
     * 
     * @param out new underlying output stream
     */
    public void recycle(OutputStream out) {
        this.out = out;
        offset = 0;
    }

    /**
     * Close this output stream. Flush the contents of the internal buffer
     * and writes the last chunk to the underlying output stream. Sets
     * the internal reference to the underlying output stream to 
     * {@code null}. Does <b>not</b> close the underlying output stream.
     *
     * @see java.io.FilterOutputStream#close()
     */
    public void close() throws IOException {
        if (out == null) {
            return;
        }
        try {
            if (offset > 0) {
                writeChunk();
            }
            out.write(LAST_CHUNK);
        } finally {
            out = null;
        }
    }
}
