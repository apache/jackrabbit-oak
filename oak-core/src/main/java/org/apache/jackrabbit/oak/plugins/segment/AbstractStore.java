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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.jackrabbit.oak.cache.CacheLIRS;

import com.google.common.cache.Cache;

public abstract class AbstractStore implements SegmentStore {

    private final SegmentCache cache;

    private final Cache<RecordId, Object> records =
            CacheLIRS.newBuilder().maximumSize(1000).build();

    private final SegmentWriter writer = new SegmentWriter(this);

    protected AbstractStore(int cacheSize) {
        this.cache = SegmentCache.create(cacheSize);
    }

    protected abstract Segment loadSegment(UUID id) throws Exception;

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public Segment readSegment(final UUID id) {
        try {
            Segment segment = getWriter().getCurrentSegment(id);
            if (segment == null) {
                segment = cache.getSegment(id, new Callable<Segment>() {
                    @Override
                    public Segment call() throws Exception {
                        return loadSegment(id);
                    }
                });
            }
            return segment;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteSegment(UUID segmentId) {
    }

    @Override
    public <T> T getRecord(RecordId id, Callable<T> loader) {
        @SuppressWarnings("unchecked")
        T record = (T) records.getIfPresent(id);
        if (record == null) {
            try {
                record = loader.call();
                records.put(id, record);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to load record " + id, e);
            }
        }
        return record;
    }

    @Override
    public void close() {
        records.invalidateAll();
    }

}
