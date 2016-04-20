/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Thread.currentThread;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * FIXME OAK-3348 document
 */
class SegmentBufferWriterPool implements WriteOperationHandler {
    private final Map<Object, SegmentBufferWriter> writers = newHashMap();
    private final Set<SegmentBufferWriter> borrowed = newHashSet();
    private final Set<SegmentBufferWriter> disposed = newHashSet();
    private final SegmentStore store;
    private final SegmentVersion version;
    private final String wid;

    private short writerId = -1;

    SegmentBufferWriterPool(SegmentStore store, SegmentVersion version, String wid) {
        this.store = store;
        this.version = version;
        this.wid = wid;
    }

    @Override
    public RecordId execute(WriteOperation writeOperation) throws IOException {
        SegmentBufferWriter writer = borrowWriter(currentThread());
        try {
            return writeOperation.execute(writer);
        } finally {
            returnWriter(currentThread(), writer);
        }
    }

    @Override
    public void flush() throws IOException {
        List<SegmentBufferWriter> toFlush = newArrayList();
        synchronized (this) {
            toFlush.addAll(writers.values());
            toFlush.addAll(disposed);
            writers.clear();
            disposed.clear();
            borrowed.clear();
        }
        // Call flush from outside a synchronized context to avoid
        // deadlocks of that method calling SegmentStore.writeSegment
        for (SegmentBufferWriter writer : toFlush) {
            writer.flush();
        }
    }

    private synchronized SegmentBufferWriter borrowWriter(Object key) {
        SegmentBufferWriter writer = writers.remove(key);
        if (writer == null) {
            writer = new SegmentBufferWriter(store, version, getWriterId(wid));
        } else if (writer.getGeneration() != store.getTracker().getGcGen()) {
            disposed.add(writer);
            writer = new SegmentBufferWriter(store, version, getWriterId(wid));
        }
        borrowed.add(writer);
        return writer;
    }

    private synchronized void returnWriter(Object key, SegmentBufferWriter writer) {
        if (borrowed.remove(writer)) {
            writers.put(key, writer);
        } else {
            // Defer flush this writer as it was borrowed while flush() was called.
            disposed.add(writer);
        }
    }

    private String getWriterId(String wid) {
        if (++writerId > 9999) {
            writerId = 0;
        }
        // Manual padding seems to be fastest here
        if (writerId < 10) {
            return wid + ".000" + writerId;
        } else if (writerId < 100) {
            return wid + ".00" + writerId;
        } else if (writerId < 1000) {
            return wid + ".0" + writerId;
        } else {
            return wid + "." + writerId;
        }
    }
}
