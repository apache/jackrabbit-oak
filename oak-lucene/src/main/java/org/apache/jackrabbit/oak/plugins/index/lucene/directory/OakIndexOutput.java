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
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakIndexFile.getOakIndexFile;

final class OakIndexOutput extends IndexOutput {
    private final String dirDetails;
    final OakIndexFile file;
    private final CRC32 crc32;

    public OakIndexOutput(String name, NodeBuilder file, String dirDetails,
                          BlobFactory blobFactory, boolean streamingWriteEnabled) {
        super("OakIndexOutput(" + name + ")");
        this.dirDetails = dirDetails;
        this.file = getOakIndexFile(name, file, dirDetails, blobFactory, streamingWriteEnabled);
        this.crc32 = new CRC32();
    }

    @Override
    public long getFilePointer() {
        return file.position();
    }

    @Override
    public long getChecksum() {
        return crc32.getValue();
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length)
            throws IOException {
        try {
            file.writeBytes(b, offset, length);
            crc32.update(b, offset, length);
        } catch (IOException e) {
            throw wrapWithDetails(e);
        }
    }

    @Override
    public void writeByte(byte b) throws IOException {
        writeBytes(new byte[] { b }, 0, 1);
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException {
        //TODO: Do we know that copyBytes would always reach us via copy??
        if (file.supportsCopyFromDataInput()) {
            file.copyBytes(input, numBytes);
        } else {
            super.copyBytes(input, numBytes);
        }
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    private IOException wrapWithDetails(IOException e) {
        String msg = String.format("Error occurred while writing to blob [%s][%s]", dirDetails, file.getName());
        return new IOException(msg, e);
    }

}
