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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;

/**
 * An index file implementation that splits data into multiple blobs (chunks).
 * This avoids loading entire files into memory.
 * Adapted from oak-lucene for Lucene 9.
 */
class OakBufferedIndexFile implements OakIndexFile {

    /**
     * Size of the blob chunks. Set to 32KB (same as oak-lucene).
     * Higher than the 4KB inline limit for BlobStore.
     */
    static final int DEFAULT_BLOB_SIZE = 32 * 1024;

    private final String name;
    private final NodeBuilder file;
    private final int blobSize;
    private final String dirDetails;
    private final BlobFactory blobFactory;

    /**
     * Current position within the file.
     */
    private long position = 0;

    /**
     * Length of the file in bytes.
     */
    private long length;

    /**
     * List of blobs (chunks). All blobs have size blobSize except possibly the last.
     */
    private List<Blob> data;

    /**
     * Whether the data has been modified since last flush.
     */
    private boolean dataModified = false;

    /**
     * Index of the currently loaded blob/chunk.
     */
    private int index = -1;

    /**
     * Buffer holding the currently loaded blob/chunk.
     */
    private byte[] blob;

    /**
     * Whether the current blob has been modified.
     */
    private boolean blobModified = false;

    public OakBufferedIndexFile(String name, NodeBuilder file, String dirDetails,
                                @NotNull BlobFactory blobFactory) {
        this.name = name;
        this.file = file;
        this.dirDetails = dirDetails;
        this.blobSize = determineBlobSize(file);
        this.blob = new byte[blobSize];
        this.blobFactory = blobFactory;

        // Load existing data if present
        PropertyState property = file.getProperty(JCR_DATA);
        if (property != null && property.getType() == BINARIES) {
            this.data = new ArrayList<>();
            for (Blob b : property.getValue(BINARIES)) {
                this.data.add(b);
            }
        } else {
            this.data = new ArrayList<>();
        }

        // Calculate length
        this.length = (long) data.size() * blobSize;
        if (!data.isEmpty()) {
            Blob last = data.get(data.size() - 1);
            this.length -= blobSize - last.length();
        }
    }

    private OakBufferedIndexFile(OakBufferedIndexFile that) {
        this.name = that.name;
        this.file = that.file;
        this.dirDetails = that.dirDetails;
        this.blobSize = that.blobSize;
        this.blob = new byte[blobSize];
        this.blobFactory = that.blobFactory;

        this.position = that.position;
        this.length = that.length;
        this.data = new ArrayList<>(that.data);
        this.dataModified = that.dataModified;
    }

    private void loadBlob(int i) throws IOException {
        if (i < 0 || i >= data.size()) {
            throw new IndexOutOfBoundsException("Invalid chunk index: " + i);
        }

        if (index != i) {
            flushBlob();

            int bytesToRead = (int) Math.min(blobSize, length - (long) i * blobSize);
            try (InputStream stream = data.get(i).getNewStream()) {
                IOUtils.readFully(stream, blob, 0, bytesToRead);
            }

            index = i;
        }
    }

    private void flushBlob() throws IOException {
        if (blobModified) {
            int bytesToWrite = (int) Math.min(blobSize, length - (long) index * blobSize);
            InputStream in = new ByteArrayInputStream(blob, 0, bytesToWrite);

            Blob b = blobFactory.createBlob(in);
            if (index < data.size()) {
                data.set(index, b);
            } else {
                if (index != data.size()) {
                    throw new IllegalStateException("Gap in chunks: index=" + index + ", data.size=" + data.size());
                }
                data.add(b);
            }

            dataModified = true;
            blobModified = false;
        }
    }

    @Override
    public OakIndexFile clone() {
        return new OakBufferedIndexFile(this);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void close() {
        this.blob = null;
        this.data = null;
    }

    @Override
    public boolean isClosed() {
        return blob == null && data == null;
    }

    @Override
    public void seek(long pos) throws IOException {
        // seek() may be called with pos == length (see LUCENE-1196)
        if (pos < 0 || pos > length) {
            throw new IOException(String.format(
                    "Invalid seek for [%s][%s], position: %d, length: %d",
                    dirDetails, name, pos, length));
        }
        position = pos;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (b == null) {
            throw new IllegalArgumentException("byte array is null");
        }
        if (offset < 0 || offset + len > b.length) {
            throw new IndexOutOfBoundsException("Invalid offset/length");
        }
        if (len < 0 || position + len > length) {
            throw new IOException(String.format(
                    "Invalid read for [%s][%s], position: %d, length: %d, len: %d",
                    dirDetails, name, position, length, len));
        }

        int chunkIndex = (int) (position / blobSize);
        int chunkOffset = (int) (position % blobSize);

        while (len > 0) {
            loadBlob(chunkIndex);

            int bytesToCopy = Math.min(len, blobSize - chunkOffset);
            System.arraycopy(blob, chunkOffset, b, offset, bytesToCopy);

            offset += bytesToCopy;
            len -= bytesToCopy;
            position += bytesToCopy;
            chunkIndex++;
            chunkOffset = 0;
        }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) throws IOException {
        int chunkIndex = (int) (position / blobSize);
        int chunkOffset = (int) (position % blobSize);

        while (len > 0) {
            int bytesToCopy = Math.min(len, blobSize - chunkOffset);

            if (index != chunkIndex) {
                if (chunkOffset > 0 || (bytesToCopy < blobSize && position + bytesToCopy < length)) {
                    // Need to load existing data first (partial chunk write)
                    loadBlob(chunkIndex);
                } else {
                    // Full chunk overwrite, no need to load
                    flushBlob();
                    index = chunkIndex;
                }
            }

            System.arraycopy(b, offset, blob, chunkOffset, bytesToCopy);
            blobModified = true;

            offset += bytesToCopy;
            len -= bytesToCopy;
            position += bytesToCopy;
            length = Math.max(length, position);

            chunkIndex++;
            chunkOffset = 0;
        }
    }

    private static int determineBlobSize(NodeBuilder file) {
        if (file.hasProperty(OakDirectory.PROP_BLOB_SIZE)) {
            return Math.toIntExact(file.getProperty(OakDirectory.PROP_BLOB_SIZE).getValue(Type.LONG));
        }
        return DEFAULT_BLOB_SIZE;
    }

    @Override
    public void flush() throws IOException {
        flushBlob();
        if (dataModified) {
            file.setProperty(JCR_LASTMODIFIED, System.currentTimeMillis());
            file.setProperty(JCR_DATA, data, BINARIES);
            dataModified = false;
        }
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public String getName() {
        return name;
    }
}
