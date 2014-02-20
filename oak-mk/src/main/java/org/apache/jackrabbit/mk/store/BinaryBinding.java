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
package org.apache.jackrabbit.mk.store;

import org.apache.jackrabbit.oak.commons.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;

/**
 * Implementation note: the 'key' parameter is ignored
 * since it's not required for binary serialization.
 */
public class BinaryBinding implements Binding {

    protected InputStream in;
    protected OutputStream out;
    
    public BinaryBinding(InputStream in) {
        this.in = in;
        out = null;
    }

    public BinaryBinding(OutputStream out) {
        this.out = out;
        in = null;
    }

    @Override
    public void write(String key, String value) throws Exception {
        if (out == null) {
            throw new IllegalStateException("no OutputStream provided");
        }
        IOUtils.writeString(out, value);
    }

    @Override
    public void write(String key, byte[] value) throws Exception {
        if (out == null) {
            throw new IllegalStateException("no OutputStream provided");
        }
        IOUtils.writeBytes(out, value);
    }

    @Override
    public void write(String key, long value) throws Exception {
        if (out == null) {
            throw new IllegalStateException("no OutputStream provided");
        }
        IOUtils.writeVarLong(out, value);
    }

    @Override
    public void write(String key, int value) throws Exception {
        if (out == null) {
            throw new IllegalStateException("no OutputStream provided");
        }
        IOUtils.writeVarInt(out, value);
    }

    @Override
    public void writeMap(String key, int count, StringEntryIterator iterator) throws Exception {
        if (out == null) {
            throw new IllegalStateException("no OutputStream provided");
        }
        IOUtils.writeVarInt(out, count);
        while (iterator.hasNext()) {
            StringEntry entry = iterator.next();
            IOUtils.writeString(out, entry.getKey());
            IOUtils.writeString(out, entry.getValue());
        }
    }

    @Override
    public void writeMap(String key, int count, BytesEntryIterator iterator) throws Exception {
        if (out == null) {
            throw new IllegalStateException("no OutputStream provided");
        }
        IOUtils.writeVarInt(out, count);
        while (iterator.hasNext()) {
            BytesEntry entry = iterator.next();
            IOUtils.writeString(out, entry.getKey());
            IOUtils.writeBytes(out, entry.getValue());
        }
    }

    @Override
    public String readStringValue(String key) throws Exception {
        if (in == null) {
            throw new IllegalStateException("no InputStream provided");
        }
        return IOUtils.readString(in);
    }

    @Override
    public byte[] readBytesValue(String key) throws Exception {
        if (in == null) {
            throw new IllegalStateException("no InputStream provided");
        }
        return IOUtils.readBytes(in);
    }

    @Override
    public long readLongValue(String key) throws Exception {
        if (in == null) {
            throw new IllegalStateException("no InputStream provided");
        }
        return IOUtils.readVarLong(in);
    }

    @Override
    public int readIntValue(String key) throws Exception {
        if (in == null) {
            throw new IllegalStateException("no InputStream provided");
        }
        return IOUtils.readVarInt(in);
    }

    @Override
    public StringEntryIterator readStringMap(String key) throws Exception {
        if (in == null) {
            throw new IllegalStateException("no InputStream provided");
        }
        final int size = IOUtils.readVarInt(in);
        return new StringEntryIterator() {
            int count = size;

            public boolean hasNext() {
                return count > 0;
            }

            public StringEntry next() {
                if (count-- > 0) {
                    try {
                        String key = IOUtils.readString(in);
                        String value = IOUtils.readString(in);
                        return new StringEntry(key, value);
                    } catch (IOException e) {
                        throw new RuntimeException("deserialization failed", e);                       
                    }
                }
                throw new NoSuchElementException();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public BytesEntryIterator readBytesMap(String key) throws Exception {
        if (in == null) {
            throw new IllegalStateException("no InputStream provided");
        }
        final int size = IOUtils.readVarInt(in);
        return new BytesEntryIterator() {
            int count = size;

            public boolean hasNext() {
                return count > 0;
            }

            public BytesEntry next() {
                if (count-- > 0) {
                    try {
                        String key = IOUtils.readString(in);
                        byte[] value = IOUtils.readBytes(in);
                        return new BytesEntry(key, value);
                    } catch (IOException e) {
                        throw new RuntimeException("deserialization failed", e);
                    }
                }
                throw new NoSuchElementException();
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
