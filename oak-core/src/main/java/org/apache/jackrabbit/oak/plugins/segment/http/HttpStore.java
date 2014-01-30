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
import java.util.UUID;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.plugins.segment.AbstractStore;
import org.apache.jackrabbit.oak.plugins.segment.Journal;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory;

import com.google.common.io.ByteStreams;

public class HttpStore extends AbstractStore {

    private final SegmentIdFactory factory = new SegmentIdFactory();

    private final URL base;

    protected HttpStore(URL base, int cacheSizeMB) {
        super(cacheSizeMB);
        this.base = base;
    }

    @Override
    public Journal getJournal(String name) {
        try {
            final URL url = new URL(base, "/j/" + name);
            return new Journal() {
                @Override
                public RecordId getHead() {
                    try {
                        InputStream stream = url.openStream();
                        try {
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(stream, UTF_8));
                            return RecordId.fromString(reader.readLine());
                        } finally {
                            stream.close();
                        }
                    } catch (IllegalArgumentException e) {
                        throw new IllegalStateException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                @Override
                public boolean setHead(RecordId base, RecordId head) {
                    throw new UnsupportedOperationException();
                }
                @Override
                public void merge() {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override @CheckForNull
    protected Segment loadSegment(UUID id) {
        try {
            URL url = new URL(base, "/s/" + id);
            InputStream stream = url.openStream();
            try {
                byte[] data = ByteStreams.toByteArray(stream);
                return new Segment(this, factory, id, ByteBuffer.wrap(data));
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
    public void writeSegment(
            UUID segmentId, byte[] bytes, int offset, int length) {
        try {
            URL url = new URL(base, "/s/" + segmentId);
            URLConnection connection = url.openConnection();
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

}
