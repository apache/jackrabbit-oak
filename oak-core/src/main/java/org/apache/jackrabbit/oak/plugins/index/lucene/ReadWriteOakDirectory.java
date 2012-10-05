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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

/**
 * A red-write implementation of the Lucene {@link Directory} (a flat list of
 * files) that allows to store Lucene index content in an Oak repository.
 */
public class ReadWriteOakDirectory extends ReadOnlyOakDirectory {

    public ReadWriteOakDirectory(NodeBuilder directoryBuilder) {
        super(directoryBuilder);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        return new OakIndexOutput(name);
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

            NodeBuilder fileBuilder = directoryBuilder.getChildBuilder(name);
            fileBuilder.setProperty("jcr:lastModified", System.currentTimeMillis());
            fileBuilder.setProperty("jcr:data", data);
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }

}
