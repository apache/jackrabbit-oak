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

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.DataInput;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;

/**
 * A file, which might be split into multiple blobs.
 */
class OakBufferedIndexFile implements OakIndexFile {
    /**
     * Size of the blob entries to which the Lucene files are split.
     * Set to higher than the 4kB inline limit for the BlobStore,
     */
    static final int DEFAULT_BLOB_SIZE = 32 * 1024;

    /**
     * The file name.
     */
    private final String name;

    /**
     * The node that contains the data for this file.
     */
    private final NodeBuilder file;

    /**
     * The maximum size of each blob.
     */
    private final int blobSize;

    /**
     * The current position within the file (for positioned read and write
     * operations).
     */
    private long position = 0;

    /**
     * The length of the file.
     */
    private long length;

    /**
     * The list of blobs (might be empty).
     * The last blob has a size of 1 up to blobSize.
     * All other blobs have a size of blobSize.
     */
    private List<Blob> data;

    /**
     * Whether the data was modified since it was last flushed. If yes, on a
     * flush, the metadata, and the list of blobs need to be stored.
     */
    private boolean dataModified = false;

    /**
     * The index of the currently loaded blob.
     */
    private int index = -1;

    /**
     * The data of the currently loaded blob.
     */
    private byte[] blob;

    /**
     * The unique key that is used to make the content unique (to allow removing binaries from the blob store without risking to remove binaries that are still needed).
     */
    private final byte[] uniqueKey;

    /**
     * Whether the currently loaded blob was modified since the blob was
     * flushed.
     */
    private boolean blobModified = false;

    private final String dirDetails;

    private final BlobFactory blobFactory;

    public OakBufferedIndexFile(String name, NodeBuilder file, String dirDetails,
                                @NotNull BlobFactory blobFactory) {
        this.name = name;
        this.file = file;
        this.dirDetails = dirDetails;
        this.blobSize = determineBlobSize(file);
        this.uniqueKey = readUniqueKey(file);
        this.blob = new byte[blobSize];
        this.blobFactory = checkNotNull(blobFactory);

        PropertyState property = file.getProperty(JCR_DATA);
        if (property != null && property.getType() == BINARIES) {
            this.data = newArrayList(property.getValue(BINARIES));
        } else {
            this.data = newArrayList();
        }

        this.length = (long)data.size() * blobSize;
        if (!data.isEmpty()) {
            Blob last = data.get(data.size() - 1);
            this.length -= blobSize - last.length();
            if (uniqueKey != null) {
                this.length -= uniqueKey.length;
            }
        }
    }

    private OakBufferedIndexFile(OakBufferedIndexFile that) {
        this.name = that.name;
        this.file = that.file;
        this.dirDetails = that.dirDetails;
        this.blobSize = that.blobSize;
        this.uniqueKey = that.uniqueKey;
        this.blob = new byte[blobSize];

        this.position = that.position;
        this.length = that.length;
        this.data = newArrayList(that.data);
        this.dataModified = that.dataModified;
        this.blobFactory = that.blobFactory;
    }

    private void loadBlob(int i) throws IOException {
        checkElementIndex(i, data.size());
        if (index != i) {
            flushBlob();
            checkState(!blobModified);

            int n = (int) Math.min(blobSize, length - (long)i * blobSize);
            InputStream stream = data.get(i).getNewStream();
            try {
                ByteStreams.readFully(stream, blob, 0, n);
            } finally {
                stream.close();
            }
            index = i;
        }
    }

    private void flushBlob() throws IOException {
        if (blobModified) {
            int n = (int) Math.min(blobSize, length - (long)index * blobSize);
            InputStream in = new ByteArrayInputStream(blob, 0, n);
            if (uniqueKey != null) {
                in = new SequenceInputStream(in,
                        new ByteArrayInputStream(uniqueKey));
            }

            Blob b = blobFactory.createBlob(in);
            if (index < data.size()) {
                data.set(index, b);
            } else {
                checkState(index == data.size());
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
        // seek() may be called with pos == length
        // see https://issues.apache.org/jira/browse/LUCENE-1196
        if (pos < 0 || pos > length) {
            String msg = String.format("Invalid seek request for [%s][%s], " +
                    "position: %d, file length: %d", dirDetails, name, pos, length);
            throw new IOException(msg);
        } else {
            position = pos;
        }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len)
            throws IOException {
        checkPositionIndexes(offset, offset + len, checkNotNull(b).length);

        if (len < 0 || position + len > length) {
            String msg = String.format("Invalid byte range request for [%s][%s], " +
                    "position: %d, file length: %d, len: %d", dirDetails, name, position, length, len);
            throw new IOException(msg);
        }

        int i = (int) (position / blobSize);
        int o = (int) (position % blobSize);
        while (len > 0) {
            loadBlob(i);

            int l = Math.min(len, blobSize - o);
            System.arraycopy(blob, o, b, offset, l);

            offset += l;
            len -= l;
            position += l;
            // next block
            i++;
            // for the next block, we read from the beginning
            o = 0;
        }
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len)
            throws IOException {
        int i = (int) (position / blobSize);
        int o = (int) (position % blobSize);
        while (len > 0) {
            int l = Math.min(len, blobSize - o);

            if (index != i) {
                if (o > 0 || (l < blobSize && position + l < length)) {
                    // loadBlob first flushes the previous block,
                    // and it sets the index
                    loadBlob(i);
                } else {
                    // we don't need to load the block,
                    // as we anyway overwrite it fully, if:
                    // o == 0 (start writing at a block boundary)
                    // and either: l is the blockSize, or
                    // we write at least to the end of the file
                    flushBlob();
                    index = i;
                }
            }
            System.arraycopy(b, offset, blob, o, l);
            blobModified = true;

            offset += l;
            len -= l;
            position += l;
            length = Math.max(length, position);

            i++;
            o = 0;
        }
    }

    @Override
    public boolean supportsCopyFromDataInput() {
        return false;
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException {
        throw new IllegalArgumentException("Don't call copyBytes for buffered case");
    }

    private static int determineBlobSize(NodeBuilder file){
        if (file.hasProperty(OakDirectory.PROP_BLOB_SIZE)){
            return Ints.checkedCast(file.getProperty(OakDirectory.PROP_BLOB_SIZE).getValue(Type.LONG));
        }
        return DEFAULT_BLOB_SIZE;
    }

    private static byte[] readUniqueKey(NodeBuilder file) {
        if (file.hasProperty(OakDirectory.PROP_UNIQUE_KEY)) {
            String key = file.getString(OakDirectory.PROP_UNIQUE_KEY);
            return StringUtils.convertHexToBytes(key);
        }
        return null;
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
