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
package org.apache.jackrabbit.core.data;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that returns pseudo-random bytes.
 */
public class RandomInputStream extends InputStream {

    private static final long MUL = 0x5DEECE66DL;
    private static final long ADD = 0xBL;
    private static final long MASK = (1L << 48) - 1;
    private static final int DEFAULT_MAX_READ_BLOCK_SIZE = 15;

    private final long initialSeed;
    private final long len;
    private long markedState;
    private long pos;
    private long markedPos;
    private long state;
    private int maxReadBlockSize;

    public String toString() {
        return "new RandomInputStream(" + initialSeed + ", " + len + ")";
    }

    public RandomInputStream(long seed, long len) {
        this(seed, len, DEFAULT_MAX_READ_BLOCK_SIZE);
    }

    public static void compareStreams(InputStream a, InputStream b) throws IOException {
        a = new BufferedInputStream(a);
        b = new BufferedInputStream(b);
        long pos = 0;
        while (true) {
            int x = a.read();
            int y = b.read();
            if (x == -1 || y == -1) {
                if (x == y) {
                    break;
                }
            }
            if (x != y) {
                throw new IOException("Incorrect byte at position " + pos + ": x=" + x + " y=" + y);
            }
        }
    }

    public RandomInputStream(long seed, long len, int maxReadBlockSize) {
        this.initialSeed = seed;
        this.len = len;
        this.maxReadBlockSize = maxReadBlockSize;
        setSeed(seed);
        reset();
    }

    public long skip(long n) {
        n = getReadBlock(n);
        if (n == 0) {
            return -1;
        }
        pos += n;
        return n;
    }

    private int getReadBlock(long n) {
        if (n > (len - pos)) {
            n = (len - pos);
        }
        if (n > maxReadBlockSize) {
            n = maxReadBlockSize;
        } else if (n < 0) {
            n = 0;
        }
        return (int) n;
    }

    public int read(byte[] b, int off, int len) {
        if (pos >= this.len) {
            return -1;
        }
        len = getReadBlock(len);
        if (len == 0) {
            return -1;
        }
        for (int i = 0; i < len; i++) {
            b[off + i] = (byte) (next() & 255);
        }
        pos += len;
        return len;
    }

    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    public void close() {
        pos = len;
    }

    private void setSeed(long seed) {
        markedState = (seed ^ MUL) & MASK;
    }

    private int next() {
        state = (state * MUL + ADD) & MASK;
        return (int) (state >>> (48 - 32));
    }

    public void reset() {
        pos = markedPos;
        state = markedState;
    }

    public int read() {
        if (pos >= len) {
            return -1;
        }
        pos++;
        return next() & 255;
    }

    public boolean markSupported() {
        return true;
    }

    public void mark(int readlimit) {
        markedPos = pos;
        markedState = state;
    }

}
