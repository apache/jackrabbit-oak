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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.IndexOutput;

/**
 * IndexOutput implementation that writes data to Oak repository using chunked storage.
 * Adapted from oak-lucene for Lucene 9.
 */
class OakIndexOutput extends IndexOutput {

    private final OakIndexFile file;
    private final CRC32 crc;

    public OakIndexOutput(String name, NodeBuilder fileNode, String dirDetails, BlobFactory blobFactory) {
        super("OakIndexOutput(" + name + ")", name);
        this.file = new OakBufferedIndexFile(name, fileNode, dirDetails, blobFactory);
        this.crc = new CRC32();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        crc.update(b);
        byte[] buf = new byte[]{b};
        file.writeBytes(buf, 0, 1);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        crc.update(b, offset, length);
        file.writeBytes(b, offset, length);
    }

    @Override
    public long getFilePointer() {
        return file.position();
    }

    @Override
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    @Override
    public void close() throws IOException {
        file.flush();
        file.close();
    }
}
