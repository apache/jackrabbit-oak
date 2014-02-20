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

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.jackrabbit.oak.commons.IOUtils;

/**
 * Input stream that reads and decodes HTTP chunks, assuming that no chunk
 * exceeds 32768 bytes and that a chunk's length is represented by exactly 4
 * hexadecimal characters.
 */
public class ChunkedInputStream extends FilterInputStream {

    /**
     * Maximum chunk size.
     */
    public static final int MAX_CHUNK_SIZE = 0x100000;

    /**
     * CR + LF combination.
     */
    private static final byte[] CRLF = "\r\n".getBytes();

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
     * Flag indicating whether there was an error decomposing a chunk.
     */
    private boolean chunkError;

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

        length = readLength(in);
        if (length < 0 || length > MAX_CHUNK_SIZE) {
        	chunkError = true;
            String msg = "Chunk size smaller than 0 or bigger than " + MAX_CHUNK_SIZE;
            throw new IOException(msg);
        }
        readFully(in, data, 0, length);
        readFully(in, suffix);
        if (!Arrays.equals(suffix, CRLF)) {
        	chunkError = true;
            String msg = "Missing carriage return/line feed combination.";
            throw new IOException(msg);
        }

        if (length == 0) {
            lastChunk = true;
        }
    }
    
    private int readLength(InputStream in) throws IOException {
    	int len = 0;
    	
    	for (int i = 0; i < 5; i++) {
    		int n, ch = in.read();
    		if (ch == -1) {
    			break;
    		}
    		if (ch >= '0' && ch <= '9') {
    			n = (ch - '0');
    		} else if (ch >= 'A' && ch <= 'F') {
    			n = (ch - 'A' + 10);
    		} else if (ch >= 'a' && ch <= 'f') {
    			n = (ch - 'a' + 10);
    		} else if (ch == '\r') {
    	        ch = in.read();
    	        if (ch != '\n') {
    	        	chunkError = true;
    	            String msg = "Missing carriage return/line feed combination.";
    	            throw new IOException(msg);
    	        }
    			return len;
    		} else {
            	chunkError = true;
                String msg = String.format("Expected hexadecimal character, actual: %c", ch);
    			throw new IOException(msg);
    		}
    		len = len * 16 + n;
    	}
        readFully(in, suffix);
        if (!Arrays.equals(suffix, CRLF)) {
        	chunkError = true;
            String msg = "Missing carriage return/line feed combination.";
            throw new IOException(msg);
        }
    	return len;
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
                while (!chunkError && !lastChunk) {
                    readChunk();
                }
            } finally {
                in = null;
            }
        }
    }
}
