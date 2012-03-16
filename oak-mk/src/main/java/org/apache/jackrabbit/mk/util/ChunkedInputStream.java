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
package org.apache.jackrabbit.mk.util;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Input stream that reads and decodes HTTP chunks, assuming that no chunk
 * exceeds 32768 bytes and that a chunk's length is represented by exactly 4
 * hexadecimal characters.
 */
public class ChunkedInputStream extends FilterInputStream {

    /**
     * Maximum chunk size.
     */
    public static final int MAX_CHUNK_SIZE = 0x8000;

    /**
     * CR + LF combination.
     */
    private static final byte[] CRLF = "\r\n".getBytes();

    /**
     * Chunk prefix (length encoded as hexadecimal string).
     */
    private final byte[] prefix = new byte[4];

    /**
     * Chunk data.
     */
    private final byte[] data = new byte[MAX_CHUNK_SIZE];

    /**
     * Chunk suffix (CR + LF).
     */
    private final byte[] suffix = new byte[2];

    /**
     * Current offset.
     */
    private int offset;

    /**
     * Chunk length.
     */
    private int length;

    /**
     * Flag indicating whether the last chunk was read.
     */
    private boolean lastChunk;

    /**
     * Create a new instance of this class.
     *
     * @param in input stream
     */
    public ChunkedInputStream(InputStream in) {
        super(in);
    }

    /* (non-Javadoc)
     * @see java.io.FilterInputStream#read()
     */
    public int read() throws IOException {
        if (!lastChunk) {
            if (offset == length) {
                readChunk();
            }
            if (offset < length) {
                return data[offset++] & 0xff;
            }
        }
        return -1;
    }

    /* (non-Javadoc)
     * @see java.io.FilterInputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {
        int read = 0;
        while (read < len && !lastChunk) {
            if (offset == length) {
                readChunk();
            }
            int available = Math.min(len - read, length - offset);
            System.arraycopy(data, offset, b, off + read, available);
            read += available;
            offset += available;
        }
        return read == 0 && lastChunk ? -1 : read;
    }

    /**
     * Read a chunk from the underlying input stream.
     *
     * @throws IOException if an error occurs
     */
    private void readChunk() throws IOException {
        offset = length = 0;

        readFully(in, prefix);
        length = parseInt(prefix);
        if (length < 0 || length > MAX_CHUNK_SIZE) {
            String msg = "Chunk size smaller than 0 or bigger than " + MAX_CHUNK_SIZE;
            throw new IOException(msg);
        }
        readFully(in, suffix);
        if (!Arrays.equals(suffix, CRLF)) {
            String msg = "Missing carriage return/line feed combination.";
            throw new IOException(msg);
        }

        readFully(in, data, 0, length);
        readFully(in, suffix);
        if (!Arrays.equals(suffix, CRLF)) {
            String msg = "Missing carriage return/line feed combination.";
            throw new IOException(msg);
        }

        if (length == 0) {
            lastChunk = true;
        }
    }
    
    private static void readFully(InputStream in, byte[] b) throws IOException {
        readFully(in, b, 0, b.length);
    }
    
    private static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        int count = IOUtils.readFully(in, b, off, len);
        if (count < len) {
            String msg = String.format("Expected %d bytes, actually received: %d",
                    len, count);
            throw new EOFException(msg);
        }
    }

    /**
     * Parse an integer that is given in its hexadecimal representation as
     * a byte array.
     *
     * @param b byte array containing 4 ASCII characters
     * @return parsed integer
     */
    private static int parseInt(byte[] b) throws IOException {
        int result = 0;

        for (int i = 0; i < 4; i++) {
            int c = (int) b[i];
            result <<= 4;
            if (c >= '0' && c <= '9') {
                result += c - '0';
            } else if (c >= 'A' && c <= 'F') {
                result += c - 'A' + 10;
            } else if (c >= 'a' && c <= 'f') {
                result += c - 'a' + 10;
            } else {
                String msg = "Not a hexadecimal character: " + c;
                throw new IOException(msg);
            }
        }
        return result;
    }

    /**
     * Recycle this input stream.
     * 
     * @param in new underlying input stream
     */
    public void recycle(InputStream in) {
        this.in = in;
        
        offset = length = 0;
        lastChunk = false;
    }
    
    /**
     * Close this input stream. Finishes reading any pending chunks until
     * the last chunk is received. Does <b>not</b> close the underlying input
     * stream.
     *
     * @see java.io.FilterInputStream#close()
     */
    public void close() throws IOException {
        if (in != null) {
            try {
                while (!lastChunk) {
                    readChunk();
                }
            } finally {
                in = null;
            }
        }
    }
}
