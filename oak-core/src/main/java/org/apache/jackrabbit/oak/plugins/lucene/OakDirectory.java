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
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class OakDirectory extends Directory {

    private final NodeStore store;

    private final CoreValueFactory factory;

    private final String[] path;

    private final NodeStateBuilder[] builders;

    private final NodeStateBuilder directoryBuilder;

    private NodeState directory;

    private OakDirectory(NodeStore store, NodeState root, String... path) {
        this.store = store;
        this.factory = store.getValueFactory();
        this.path = path;
        this.builders = new NodeStateBuilder[path.length + 1];

        NodeState state = root;
        builders[0] = store.getBuilder(state);
        for (int i = 0; i < path.length; i++) {
            NodeState child = root.getChildNode(path[i]);
            if (child == null) {
                builders[i + 1] = store.getBuilder(null);
                state = builders[i + 1].getNodeState();
            } else {
                builders[i + 1] = store.getBuilder(child);
                state = child;
            }
        }
        this.directoryBuilder = builders[path.length];
        this.directory = state;
    }

    @Nonnull
    public NodeState getRoot() {
        NodeState state = getDirectory();
        for (int i = 1; i <= path.length; i++) {
            builders[path.length - i].setNode(
                    path[path.length - i], state);
            state = builders[path.length - i].getNodeState();
        }
        return state;
    }

    @Nonnull
    private NodeState getDirectory() {
        if (directory == null) {
            directory = directoryBuilder.getNodeState();
        }
        return directory;
    }

    @Override
    public String[] listAll() throws IOException {
        NodeState directory = getDirectory();
        List<String> names =
                new ArrayList<String>((int) directory.getChildNodeCount());
        for (ChildNodeEntry entry : directory.getChildNodeEntries()) {
            names.add(entry.getName());
        }
        return names.toArray(new String[names.size()]);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return getDirectory().getChildNode(name) != null;
    }

    @Override
    public long fileModified(String name) throws IOException {
        NodeState file = getDirectory().getChildNode(name);
        if (file == null) {
            return 0;
        }

        PropertyState property = file.getProperty("jcr:lastModified");
        if (property == null || property.isArray()) {
            return 0;
        }

        return property.getValue().getLong();
    }

    @Override
    public void touchFile(String name) throws IOException {
        NodeState file = getDirectory().getChildNode(name);
        NodeStateBuilder builder = store.getBuilder(file);
        builder.setProperty(
                "jcr:lastModified",
                factory.createValue(System.currentTimeMillis()));
        directoryBuilder.setNode(name, builder.getNodeState());
        directory = null;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        directoryBuilder.removeNode(name);
        directory = null;
    }

    @Override
    public long fileLength(String name) throws IOException {
        NodeState file = getDirectory().getChildNode(name);
        if (file == null) {
            return 0;
        }

        PropertyState property = file.getProperty("jcr:data");
        if (property == null || property.isArray()) {
            return 0;
        }

        return property.getValue().length();
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        return new OakIndexOutput(name);
    }

    @Override
    public IndexInput openInput(final String name) throws IOException {
        return new IndexInput(name) {

            private final byte[] data = readFile(name);

            private int position = 0;

            @Override
            public void readBytes(byte[] b, int offset, int len)
                    throws IOException {
                if (offset < 0 || len < 0 || position + len > data.length) {
                    throw new IOException("Invalid byte range request");
                } else {
                    System.arraycopy(data, position, b, offset, len);
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
            public void close() throws IOException {
                // do nothing
            }

        };
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    private byte[] readFile(String name) throws IOException {
        CoreValue value = null;
        NodeState file = getDirectory().getChildNode(name);
        if (file != null) {
            PropertyState property = file.getProperty("jcr:data");
            if (property != null && !property.isArray()) {
                value = property.getValue();
            }
        }

        if (value != null) {
            int size = (int) value.length();
            byte[] buffer = new byte[size];

            InputStream stream = value.getNewStream();
            try {
                do {
                    int n = stream.read(buffer, size, buffer.length - size);
                    if (n == -1) {
                        throw new IOException(
                                "Unexpected end of index file: " + name);
                    }
                    size += n;
                } while (size < buffer.length);
            } finally {
                stream.close();
            }

            return buffer;
        } else {
            return new byte[0];
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
        public long length() throws IOException {
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
                    store.getBuilder(getDirectory().getChildNode(name));
            fileBuilder.setProperty(
                    "jcr:lastModified",
                    factory.createValue(System.currentTimeMillis()));
            fileBuilder.setProperty(
                    "jcr:data",
                    factory.createValue(new ByteArrayInputStream(data)));

            directoryBuilder.setNode(name, fileBuilder.getNodeState());
            directory = null;
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }


}
