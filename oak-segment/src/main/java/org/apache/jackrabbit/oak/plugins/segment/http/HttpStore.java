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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.plugins.segment.SegmentTracker;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;

import com.google.common.io.ByteStreams;

import org.apache.jackrabbit.oak.spi.blob.BlobStore;

@Deprecated
public class HttpStore implements SegmentStore {

    private final SegmentTracker tracker = new SegmentTracker(this);

    private final URL base;

    /**
     * @param base
     *            make sure the url ends with a slash "/", otherwise the
     *            requests will end up as absolute instead of relative
     */
    @Deprecated
    public HttpStore(URL base) {
        this.base = base;
    }

    @Override
    @Deprecated
    public SegmentTracker getTracker() {
        return tracker;
    }

    /**
     * Builds a simple URLConnection. This method can be extended to add
     * authorization headers if needed.
     * 
     */
    protected URLConnection get(String fragment) throws MalformedURLException,
            IOException {
        final URL url;
        if (fragment == null) {
            url = base;
        } else {
            url = new URL(base, fragment);
        }
        return url.openConnection();
    }

    @Override
    @Deprecated
    public SegmentNodeState getHead() {
        try {
            URLConnection connection = get(null);
            InputStream stream = connection.getInputStream();
            try {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(stream, UTF_8));
                return new SegmentNodeState(
                        RecordId.fromString(tracker, reader.readLine()));
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
    @Deprecated
    public boolean setHead(SegmentNodeState base, SegmentNodeState head) {
        // TODO throw new UnsupportedOperationException();
        return true;
    }

    @Override
    @Deprecated
    public boolean containsSegment(SegmentId id) {
        return id.getTracker() == tracker || readSegment(id) != null;
    }

    @Override
    @Deprecated
    public Segment readSegment(SegmentId id) {
        try {
            URLConnection connection = get(id.toString());
            InputStream stream = connection.getInputStream();
            try {
                byte[] data = ByteStreams.toByteArray(stream);
                return new Segment(tracker, id, ByteBuffer.wrap(data));
            } finally {
                stream.close();
            }
        } catch (MalformedURLException e) {
            throw new SegmentNotFoundException(id, e);
        } catch (IOException e) {
            throw new SegmentNotFoundException(id, e);
        }
    }

    @Override
    @Deprecated
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

    @Override
    @Deprecated
    public void close() {
    }

    @Override @CheckForNull
    @Deprecated
    public Blob readBlob(String reference) {
        return null;
    }

    @Override @CheckForNull
    @Deprecated
    public BlobStore getBlobStore() {
        return null;
    }

    @Override
    @Deprecated
    public void gc() {
        // TODO: distributed gc
    }

}
