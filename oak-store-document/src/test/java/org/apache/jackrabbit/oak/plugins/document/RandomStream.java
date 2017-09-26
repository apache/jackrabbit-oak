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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.InputStream;
import java.util.Random;

/**
 * A pseudo-random stream.
 */
public class RandomStream extends InputStream {

    private long pos, size;
    private Random random;

    public RandomStream(long size, int seed) {
        this.size = size;
        this.random = new Random(seed);
    }

    @Override
    public int read() {
        byte[] data = new byte[1];
        int len = read(data, 0, 1);
        return len <= 0 ? len : data[0] & 255;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (pos >= size) {
            return -1;
        }
        len = (int) Math.min(size - pos, len);
        long end = off + len;
        while (off < end) {
            int r = random.nextInt();
            b[off++] = (byte) (r ^ (r >>> 16));
        }
        pos += len;
        return len;
    }

}
