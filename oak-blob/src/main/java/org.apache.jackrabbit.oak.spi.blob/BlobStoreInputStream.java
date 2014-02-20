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
package org.apache.jackrabbit.oak.spi.blob;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.commons.IOUtils;

/**
 * An input stream to simplify reading from a store.
 * See also MicroKernelInputStream.
 */
public class BlobStoreInputStream extends InputStream {

    private final BlobStore store;
    private final String id;
    private long pos;
    private byte[] oneByteBuff;

    public BlobStoreInputStream(BlobStore store, String id, long pos) {
        this.store = store;
        this.id = id;
        this.pos = pos;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int l;
        try {
            l = store.readBlob(id, pos, b, off, len);
        } catch (Exception e) {
            throw new IOException(e);
        }
        if (l < 0) {
            return l;
        }
        pos += l;
        return l;
    }

    @Override
    public int read() throws IOException {
        if (oneByteBuff == null) {
            oneByteBuff = new byte[1];
        }
        int len = read(oneByteBuff, 0, 1);
        if (len < 0) {
            return len;
        }
        return oneByteBuff[0] & 0xff;
    }

    public static byte[] readFully(BlobStore store, String id) throws IOException {
        int len = (int) store.getBlobLength(id);
        byte[] buff = new byte[len];
        BlobStoreInputStream in = new BlobStoreInputStream(store, id, 0);
        IOUtils.readFully(in, buff, 0, len);
        return buff;
    }

}