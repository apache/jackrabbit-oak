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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.remote.RemoteBinaryFilters;

import java.io.IOException;
import java.io.InputStream;

class ContentRemoteInputStream extends InputStream {

    private final InputStream stream;

    private final long start;

    private final long count;

    private long index = 0;

    public ContentRemoteInputStream(InputStream stream, RemoteBinaryFilters filters) {
        this.stream = stream;

        long startFilter = filters.getStart();

        if (startFilter > 0) {
            this.start = startFilter;
        } else {
            this.start = 0;
        }

        this.count = filters.getCount();
    }

    @Override
    public int read() throws IOException {
        while (index < start - 1) {
            long skipped = stream.skip(start - index);

            if (skipped <= 0) {
                return -1;
            }

            index = index + skipped;
        }

        if (count >= 0 && index >= start + count) {
            return -1;
        }

        int result = stream.read();

        index = index + 1;

        return result;
    }

}
