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
package org.apache.jackrabbit.oak.plugins.segment.http;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory.isBulkSegmentId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;

import com.google.common.cache.Cache;
import com.google.common.io.ByteStreams;

public class HttpStore implements SegmentStore {

    protected static final int MB = 1024 * 1024;

    private final SegmentIdFactory factory = new SegmentIdFactory();

    private final SegmentWriter writer = new SegmentWriter(this, factory);

    private final URL base;

    private final Cache<UUID, Segment> segments;

    /**
     * Identifiers of the segments that are currently being loaded.
     */
    private final Set<UUID> currentlyLoading = newHashSet();

    /**
     * Number of threads that are currently waiting for segments to be loaded.
     * Used to avoid extra {@link #notifyAll()} calls when nobody is waiting.
     */
    private int currentlyWaiting = 0;

    /**
     * @param base
     *            make sure the url ends with a slash "/", otherwise the
     *            requests will end up as absolute instead of relative
     * @param cacheSizeMB
     */
    public HttpStore(URL base, int cacheSizeMB) {
        this.base = base;
        this.segments = CacheLIRS.newBuilder()
                .weigher(Segment.WEIGHER)
                .maximumWeight(cacheSizeMB * MB)
                .build();
    }

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public SegmentNodeState getHead() {
        try {
            URLConnection connection = base.openConnection();
            InputStream stream = connection.getInputStream();
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stream, UTF_8));
                return new SegmentNodeState(
                        getWriter().getDummySegment(),
                        RecordId.fromString(reader.readLine()));
            } finally {
                stream.close();
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(e);
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        // TODO throw new UnsupportedOperationException();
        return true;
    }

    @Override
    public Segment readSegment(UUID id) {
        if (isBulkSegmentId(id)) {
            return loadSegment(id);
        }

        Segment segment = getWriter().getCurrentSegment(id);
        if (segment != null) {
            return segment;
        }

        synchronized (segments) {
            // check if the segment is already cached
            segment = segments.getIfPresent(id);
            // ... or currently being loaded
            while (segment == null && currentlyLoading.contains(id)) {
                currentlyWaiting++;
                try {
                    segments.wait(); // for another thread to load the segment
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                } finally {
                    currentlyWaiting--;
                }
                segment = segments.getIfPresent(id);
            }
            if (segment != null) {
                // found the segment in the cache
                return segment;
            }
            // not yet cached, so start let others know that we're loading it
            currentlyLoading.add(id);
        }

        try {
            segment = loadSegment(id);
        } finally {
            synchronized (segments) {
                if (segment != null) {
                    segments.put(id, segment);
                }
                currentlyLoading.remove(id);
                if (currentlyWaiting > 0) {
                    segments.notifyAll();
                }
            }
        }

        return segment;
    }

    private Segment loadSegment(UUID uuid) {
        try {
            URLConnection connection =
                    new URL(base, uuid.toString()).openConnection();
            InputStream stream = connection.getInputStream();
            try {
                byte[] data = ByteStreams.toByteArray(stream);
                return new Segment(this, factory, uuid, ByteBuffer.wrap(data));
            } finally {
                stream.close();
            }
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeSegment(UUID segmentId, byte[] bytes, int offset,
            int length) {
        try {
            URLConnection connection =
                    new URL(base, segmentId.toString()).openConnection();
            connection.setDoInput(false);
            connection.setDoOutput(true);
            OutputStream stream = connection.getOutputStream();
            try {
                stream.write(bytes, offset, length);
            } finally {
                stream.close();
            }
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        synchronized (segments) {
            while (!currentlyLoading.isEmpty()) {
                try {
                    segments.wait(); // for concurrent loads to finish
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                }
            }
            segments.invalidateAll();
        }
    }

    @Override @CheckForNull
    public Blob readBlob(String reference) {
        return null;
    }

}
