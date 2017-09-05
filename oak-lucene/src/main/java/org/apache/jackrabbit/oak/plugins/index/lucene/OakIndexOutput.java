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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

final class OakIndexOutput extends IndexOutput {
    private final String dirDetails;
    private final OakIndexFile file;

    public OakIndexOutput(String name, NodeBuilder file, String dirDetails,
                          BlobFactory blobFactory) throws IOException {
        this.dirDetails = dirDetails;
        this.file = new OakIndexFile(name, file, dirDetails, blobFactory);
    }

    @Override
    public long length() {
        return file.length();
    }

    @Override
    public long getFilePointer() {
        return file.position();
    }

    @Override
    public void seek(long pos) throws IOException {
        file.seek(pos);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length)
            throws IOException {
        try {
            file.writeBytes(b, offset, length);
        } catch (IOException e) {
            throw wrapWithDetails(e);
        }
    }

    @Override
    public void writeByte(byte b) throws IOException {
        writeBytes(new byte[] { b }, 0, 1);
    }

    @Override
    public void flush() throws IOException {
        try {
            file.flush();
        } catch (IOException e) {
            throw wrapWithDetails(e);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        file.close();
    }

    private IOException wrapWithDetails(IOException e) {
        String msg = String.format("Error occurred while writing to blob [%s][%s]", dirDetails, file.getName());
        return new IOException(msg, e);
    }

}
