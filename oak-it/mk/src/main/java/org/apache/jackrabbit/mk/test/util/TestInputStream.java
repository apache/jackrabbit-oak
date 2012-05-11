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
package org.apache.jackrabbit.mk.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * An {@code InputStream} based on pseudo-random data useful for testing.
 * <p/>
 * Instances created with identical parameter values do produce identical byte
 * sequences.
 */
public class TestInputStream extends InputStream {

    private final Random random;
    private final long length;

    private long pos;
    private boolean closed;

    public TestInputStream(long length) {
        this(0, length);
    }

    public TestInputStream(long seed, long length) {
        super();
        random = new Random(seed);
        if (length < 0) {
            throw new IllegalArgumentException("length cannot be negative");
        }
        this.length = length;
        pos = 0;
        closed = false;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        long skipped;
        for (skipped = 0; skipped < n; skipped++) {
            if (read() == -1) {
                break;
            }
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (int) Math.min(Integer.MAX_VALUE, length - pos);
    }

    @Override
    public void close() throws IOException {
        super.close();
        closed = true;
    }

    @Override
    public int read() throws IOException {
        if (pos >= length) {
            return -1;
        }
        pos++;
        return random.nextInt() & 0xff;
    }
}
