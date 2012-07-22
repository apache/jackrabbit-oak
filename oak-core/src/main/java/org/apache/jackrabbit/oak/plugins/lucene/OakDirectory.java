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
package org.apache.jackrabbit.oak.plugins.lucene;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import com.google.common.collect.Iterables;

class OakDirectory extends Directory {

    private final CoreValueFactory factory;

    private final NodeStateBuilder rootBuilder;

    private final NodeStateBuilder directoryBuilder;

    public OakDirectory(NodeStore store, NodeState root, String... path) {
        this.lockFactory = NoLockFactory.getNoLockFactory();
        this.factory = store.getValueFactory();
        this.rootBuilder = store.getBuilder(root);

        NodeStateBuilder builder = rootBuilder;
        for (String name : path) {
            builder = builder.getChildBuilder(name);
        }
        this.directoryBuilder = builder;
    }

    @Nonnull
    NodeState getRoot() {
        return rootBuilder.getNodeState();
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

        NodeStateBuilder fileBuilder = directoryBuilder.getChildBuilder(name);
        PropertyState property = fileBuilder.getProperty("jcr:data");
        if (property == null || property.isArray()) {
            return 0;
        }

        return property.getValue().length();
    }
    

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        return new OakIndexOutput(name);
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

    private byte[] readFile(String name) throws IOException {
        if (!fileExists(name)) {
            return new byte[0];
        }

        NodeStateBuilder fileBuilder = directoryBuilder.getChildBuilder(name);
        PropertyState property = fileBuilder.getProperty("jcr:data");
        if (property == null || property.isArray()) {
            return new byte[0];
        }

        CoreValue value = property.getValue();
        InputStream stream = value.getNewStream();
        try {
            byte[] buffer = new byte[(int) value.length()];

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

    private final class OakIndexOutput extends IndexOutput {

        private final String name;

        private byte[] buffer;

        private int size;

        private int position;

        public OakIndexOutput(String name) throws IOException {
            this.name = name;
            this.buffer = readFile(name);
            this.size = buffer.length;
            this.position = 0;
        }

        @Override
        public long length() {
            return size;
        }

        @Override
        public long getFilePointer() {
            return position;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0 || pos > Integer.MAX_VALUE) {
                throw new IOException("Invalid file position: " + pos);
            }
            this.position = (int) pos;
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) {
            while (position + length > buffer.length) {
                byte[] tmp = new byte[Math.max(4096, buffer.length * 2)];
                System.arraycopy(buffer, 0, tmp, 0, size);
                buffer = tmp;
            }

            System.arraycopy(b, offset, buffer, position, length);

            position += length;
            if (position > size) {
                size = position;
            }
        }

        @Override
        public void writeByte(byte b) {
            writeBytes(new byte[] { b }, 0, 1);
        }

        @Override
        public void flush() throws IOException {
            byte[] data = buffer;
            if (data.length > size) {
                data = new byte[size];
                System.arraycopy(buffer, 0, data, 0, size);
            }

            NodeStateBuilder fileBuilder =
                    directoryBuilder.getChildBuilder(name);
            fileBuilder.setProperty(
                    "jcr:lastModified",
                    factory.createValue(System.currentTimeMillis()));
            fileBuilder.setProperty(
                    "jcr:data",
                    factory.createValue(new ByteArrayInputStream(data)));
        }

        @Override
        public void close() throws IOException {
            flush();
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

    };

}
