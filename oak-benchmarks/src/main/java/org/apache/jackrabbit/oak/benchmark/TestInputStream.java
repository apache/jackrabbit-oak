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
package org.apache.jackrabbit.oak.benchmark;

import java.io.InputStream;
import java.util.Random;

/**
 * An input stream that returns a given number of dummy data. The returned
 * data is designed to be non-compressible to prevent possible compression
 * mechanisms from affecting performance measurements.
 */
public class TestInputStream extends InputStream {

    private final int n;

    private int i;

    /**
     * Source of the random stream of bytes. No fixed seed is used to
     * prevent a solution like the Jackrabbit data store from using just
     * a single storage location for multiple streams.
     */
    private final Random random = new Random();

    public TestInputStream(int length) {
        n = length;
        i = 0;
    }

    @Override
    public int read() {
        if (i < n) {
            i++;
            byte[] b = new byte[1];
            random.nextBytes(b);
            return b[0];
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (i < n) {
            byte[] data = new byte[Math.min(len, n - i)];
            random.nextBytes(data);
            System.arraycopy(data, 0, b, off, data.length);
            i += data.length;
            return data.length;
        } else {
            return -1;
        }
    }


}
