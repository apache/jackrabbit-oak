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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

abstract class SegmentFile {

    protected static class Entry {

        int position;

        int length;

        Entry(int position, int length) {
            this.position = position;
            this.length = length;
        }

    }

    private volatile Map<UUID, Entry> entries = emptyMap();

    ByteBuffer readEntry(UUID id) throws IOException {
        Entry entry = entries.get(id);
        if (entry != null) {
            return read(entry.position, entry.length);
        } else {
            return null;
        }
    }

    protected abstract int length() throws IOException;

    protected abstract ByteBuffer read(int position, int length)
            throws IOException;

    void close() throws IOException {
    }
}
