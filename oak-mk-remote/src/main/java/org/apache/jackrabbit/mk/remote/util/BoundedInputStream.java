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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Implementation of an {@code InputStream} that is bounded by a limit
 * and will return {@code -1} on reads when this limit is exceeded.
 */
public class BoundedInputStream extends FilterInputStream {
    
    private final int limit;
    private int count;
    
    /**
     * Create a new instance of this class.
     *
     * @param in input stream
     * @param limit limit
     */
    public BoundedInputStream(InputStream in, int limit) {
        super(in);
        
        this.limit = limit;
    }

    @Override
    public int read() throws IOException {
        if (count < limit) {
            int c = in.read();
            if (c != -1) {
                count++;
            }
            return c;
        }
        return -1;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (count < limit) {
            if (limit - count < len) {
                len = limit - count;
            }
            int n = in.read(b, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }
        return -1;
    }
    
    /**
     * Close this input stream. Finishes reading any pending chunks until
     * the last chunk is received. Does <b>not</b> close the underlying input
     * stream.
     *
     * @see java.io.FilterInputStream#close()
     */
    @Override
    public void close() throws IOException {
        if (in == null) {
            return;
        }
        try {
            int remains = limit - count;
            if (remains > 0) {
                in.skip(remains);
            }
        } finally {
            in = null;
        }
    }
}
