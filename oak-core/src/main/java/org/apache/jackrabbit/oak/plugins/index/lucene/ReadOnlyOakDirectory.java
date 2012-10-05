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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import javax.jcr.UnsupportedRepositoryOperationException;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import static org.apache.jackrabbit.oak.api.Type.BINARY;

/**
 * A read-only implementation of the Lucene {@link Directory} (a flat list of
 * files) that only allows reading of the Lucene index content stored in an Oak
 * repository.
 */
class ReadOnlyOakDirectory extends Directory {

    protected final NodeBuilder directoryBuilder;

    public ReadOnlyOakDirectory(NodeBuilder directoryBuilder) {
        this.lockFactory = NoLockFactory.getNoLockFactory();
        this.directoryBuilder = directoryBuilder;
    }

    @Override
    public String[] listAll() throws IOException {
        return Iterables.toArray(
                directoryBuilder.getChildNodeNames(), String.class);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return directoryBuilder.hasChildNode(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        directoryBuilder.removeNode(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (!fileExists(name)) {
            return 0;
        }

        NodeBuilder fileBuilder = directoryBuilder.getChildBuilder(name);
        PropertyState property = fileBuilder.getProperty("jcr:data");
        if (property == null || property.isArray()) {
            return 0;
        }

        return property.size();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        throw new IOException(new UnsupportedRepositoryOperationException());
    }

    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {
        return new OakIndexInput(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // ?
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    protected byte[] readFile(String name) throws IOException {
        if (!fileExists(name)) {
            return new byte[0];
        }

        NodeBuilder fileBuilder = directoryBuilder.getChildBuilder(name);
        PropertyState property = fileBuilder.getProperty("jcr:data");
        if (property == null || property.isArray()) {
            return new byte[0];
        }

        InputStream stream = property.getValue(BINARY).getNewStream();
        try {
            byte[] buffer = new byte[(int) property.size()];

            int size = 0;
            do {
                int n = stream.read(buffer, size, buffer.length - size);
                if (n == -1) {
                    throw new IOException(
                            "Unexpected end of index file: " + name);
                }
                size += n;
            } while (size < buffer.length);

            return buffer;
        } finally {
            stream.close();
        }
    }

    private final class OakIndexInput extends IndexInput {

        private final byte[] data;

        private int position;

        public OakIndexInput(String name) throws IOException {
            super(name);
            this.data = readFile(name);
            this.position = 0;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len)
                throws IOException {
            if (len < 0 || position + len > data.length) {
                throw new IOException("Invalid byte range request");
            } else {
                System.arraycopy(data, position, b, offset, len);
                position += len;
            }
        }

        @Override
        public byte readByte() throws IOException {
            if (position >= data.length) {
                throw new IOException("Invalid byte range request");
            } else {
                return data[position++];
            }
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos >= data.length) {
                throw new IOException("Invalid seek request");
            } else {
                position = (int) pos;
            }
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public long getFilePointer() {
            return position;
        }

        @Override
        public void close() {
            // do nothing
        }

    }

}
