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
package org.apache.jackrabbit.oak.segment.http;

import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public class HttpStore implements SegmentStore {

    @Nonnull
    private final SegmentTracker tracker = new SegmentTracker();

    @Nonnull
    private final HttpStoreRevisions revisions = new HttpStoreRevisions(this);

    @Nonnull
    private final Supplier<SegmentWriter> getWriter = new Supplier<SegmentWriter>() {
        @Override
        public SegmentWriter get() {
            return getWriter();
        }
    };
    @Nonnull
    private final SegmentReader segmentReader = new CachingSegmentReader(
            getWriter, null, DEFAULT_STRING_CACHE_MB, DEFAULT_TEMPLATE_CACHE_MB);

    private final SegmentIdFactory segmentIdFactory = new SegmentIdFactory() {

        @Override
        @Nonnull
        public SegmentId newSegmentId(long msb, long lsb) {
            return new SegmentId(HttpStore.this, msb, lsb);
        }

    };

    @Nonnull
    private final SegmentWriter segmentWriter = segmentWriterBuilder("sys")
            .withWriterPool().build(this);

    private final URL base;

    /**
     * @param base
     *            make sure the url ends with a slash "/", otherwise the
     *            requests will end up as absolute instead of relative
     */
    public HttpStore(URL base) {
        this.base = base;
    }

    @Nonnull
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Nonnull
    public SegmentWriter getWriter() {
        return segmentWriter;
    }

    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    @Nonnull
    public Revisions getRevisions() {
        return revisions;
    }

    @Override
    @Nonnull
    public SegmentId newSegmentId(long msb, long lsb) {
        return tracker.newSegmentId(msb, lsb, segmentIdFactory);
    }

    @Override
    @Nonnull
    public SegmentId newBulkSegmentId() {
        return tracker.newBulkSegmentId(segmentIdFactory);
    }

    @Override
    @Nonnull
    public SegmentId newDataSegmentId() {
        return tracker.newDataSegmentId(segmentIdFactory);
    }

    /**
     * Builds a simple URLConnection. This method can be extended to add
     * authorization headers if needed.
     * 
     */
    URLConnection get(String fragment) throws IOException {
        final URL url;
        if (fragment == null) {
            url = base;
        } else {
            url = new URL(base, fragment);
        }
        return url.openConnection();
    }

    @Override
    public boolean containsSegment(SegmentId id) {
        if (id.sameStore(this)) {
            return true;
        }
        try {
            readRawSegment(id);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    @Nonnull
    public Segment readSegment(SegmentId id) {
        try {
            return new Segment(this, segmentReader, id, ByteBuffer.wrap(readRawSegment(id)));
        } catch (IOException e) {
            throw new SegmentNotFoundException(id, e);
        }
    }

    private byte[] readRawSegment(SegmentId id) throws IOException {
        try (InputStream stream = get(id.toString()).getInputStream()) {
            return ByteStreams.toByteArray(stream);
        }
    }

    @Override
    public void writeSegment(
            SegmentId id, byte[] bytes, int offset, int length) throws IOException {
        try {
            URLConnection connection = get(id.toString());
            connection.setDoInput(false);
            connection.setDoOutput(true);
            OutputStream stream = connection.getOutputStream();
            try {
                stream.write(bytes, offset, length);
            } finally {
                stream.close();
            }
        } catch (MalformedURLException e) {
            throw new IOException(e);
        }
    }

    /**
     * @return  {@code null}
     */
    @CheckForNull
    public BlobStore getBlobStore() {
        return null;
    }

}
