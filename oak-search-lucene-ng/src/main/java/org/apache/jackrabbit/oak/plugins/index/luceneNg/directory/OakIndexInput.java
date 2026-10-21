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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.IndexInput;

/**
 * IndexInput implementation that reads data from Oak repository using chunked storage.
 * Adapted from oak-lucene for Lucene 9.
 */
class OakIndexInput extends IndexInput {

    private final OakIndexFile file;
    private final long sliceOffset;
    private final long sliceLength;

    public OakIndexInput(String name, NodeBuilder fileNode, String dirDetails, BlobFactory blobFactory) {
        super("OakIndexInput(" + name + ")");
        this.file = new OakBufferedIndexFile(name, fileNode, dirDetails, blobFactory);
        this.sliceOffset = 0;
        this.sliceLength = file.length();
    }

    private OakIndexInput(OakIndexInput other, String sliceDescription, long offset, long length) throws IOException {
        super(other.getFullSliceDescription(sliceDescription));
        this.file = other.file.clone();
        this.sliceOffset = offset;
        this.sliceLength = length;
        // Position file at the slice offset
        this.file.seek(offset);
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (file.isClosed()) {
            throw new IOException("IndexInput is closed");
        }
        long pos = getFilePointer();
        if (pos + len > sliceLength) {
            throw new IOException("read past EOF: " + (pos + len) + " > " + sliceLength);
        }
        file.readBytes(b, offset, len);
    }

    @Override
    public byte readByte() throws IOException {
        if (file.isClosed()) {
            throw new IOException("IndexInput is closed");
        }
        if (getFilePointer() >= sliceLength) {
            throw new IOException("read past EOF: " + getFilePointer());
        }
        byte[] b = new byte[1];
        file.readBytes(b, 0, 1);
        return b[0];
    }

    @Override
    public void seek(long pos) throws IOException {
        if (file.isClosed()) {
            throw new IOException("IndexInput is closed");
        }
        if (pos < 0 || pos > sliceLength) {
            throw new IOException("seek position out of bounds: " + pos);
        }
        // Seek to absolute position in file
        file.seek(sliceOffset + pos);
    }

    @Override
    public long length() {
        if (file.isClosed()) {
            throw new IllegalStateException("IndexInput is closed");
        }
        // Return slice length, not full file length
        return sliceLength;
    }

    @Override
    public long getFilePointer() {
        // Return position relative to slice start
        return file.position() - sliceOffset;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (file.isClosed()) {
            throw new IOException("IndexInput is closed");
        }
        if (offset < 0 || length < 0 || offset + length > length()) {
            throw new IllegalArgumentException(String.format(
                    "Invalid slice: offset=%d, length=%d, file.length=%d",
                    offset, length, length()));
        }
        // Create a new slice with absolute offset in the underlying file
        return new OakIndexInput(this, sliceDescription, sliceOffset + offset, length);
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
