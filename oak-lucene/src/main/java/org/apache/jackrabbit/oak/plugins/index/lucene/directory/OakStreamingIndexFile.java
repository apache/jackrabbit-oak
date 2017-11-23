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
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.DataInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.BINARY;

/**
 * A file which streams blob directly off of storage.
 */
class OakStreamingIndexFile implements OakIndexFile, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OakStreamingIndexFile.class.getName());

    /**
     * The file name.
     */
    private final String name;

    /**
     * The node that contains the blob for this file.
     */
    private final NodeBuilder file;

    /**
     * The current position within the file (in streaming case, useful only for reading).
     */
    private long position = 0;

    /**
     * The length of the file.
     */
    private long length;

    /**
     * The blob which has been read for reading case.
     * For writing case, it contains the blob that's pushed to repository
     */
    private Blob blob;

    /**
     * Whether the blob was modified since it was last flushed. If yes, on a
     * flush the metadata and the blob to the store.
     */
    private boolean blobModified = false;

    /**
     * The {@link InputStream} to read blob from blob.
     */
    private InputStream blobInputStream;

    /**
     * The unique key that is used to make the content unique (to allow removing binaries from the blob store without
     * risking to remove binaries that are still needed).
     */
    private final byte[] uniqueKey;

    private final String dirDetails;

    private final BlobFactory blobFactory;

    OakStreamingIndexFile(String name, NodeBuilder file, String dirDetails,
                                 @Nonnull BlobFactory blobFactory) {
        this.name = name;
        this.file = file;
        this.dirDetails = dirDetails;
        this.uniqueKey = readUniqueKey(file);
        this.blobFactory = checkNotNull(blobFactory);

        PropertyState property = file.getProperty(JCR_DATA);
        if (property != null) {
            if (property.getType() == BINARY) {
                this.blob = property.getValue(BINARY);
            } else {
                throw new IllegalArgumentException("Can't load blob for streaming for " + name + " under " + file);
            }
        } else {
            this.blob = null;
        }

        if (blob != null) {
            this.length = blob.length();
            if (uniqueKey != null) {
                this.length -= uniqueKey.length;
            }
        }

        this.blobInputStream = null;
    }

    private OakStreamingIndexFile(OakStreamingIndexFile that) {
        this.name = that.name;
        this.file = that.file;
        this.dirDetails = that.dirDetails;
        this.uniqueKey = that.uniqueKey;

        this.position = that.position;
        this.length = that.length;
        this.blob = that.blob;
        this.blobModified = that.blobModified;
        this.blobFactory = that.blobFactory;
    }

    private void setupInputStream() throws IOException {
        if (blobInputStream == null) {
            blobInputStream = blob.getNewStream();

            if (position > 0) {
                long pos = position;
                position = 0;
                seek(pos);
            }
        }
    }

    private void releaseInputStream() {
        if (blobInputStream != null) {
            try {
                blobInputStream.close();
            } catch (Exception ignored) {
                //ignore
            }
            blobInputStream = null;
        }
    }

    @Override
    public OakIndexFile clone() {
        return new OakStreamingIndexFile(this);
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
        IOUtils.closeQuietly(blobInputStream);
        this.blob = null;
    }

    @Override
    public boolean isClosed() {
        return blobInputStream == null && blob == null;
    }

    @Override
    public void seek(long pos) throws IOException {
        // seek() may be called with pos == length
        // see https://issues.apache.org/jira/browse/LUCENE-1196
        if (pos < 0 || pos > length) {
            String msg = String.format("Invalid seek request for [%s][%s], " +
                    "position: %d, file length: %d", dirDetails, name, pos, length);
            releaseInputStream();
            throw new IOException(msg);
        } else {
            if (blobInputStream == null) {
                position = pos;
            } else if (pos < position) {
                LOG.warn("Seeking back on streaming index file {}. Current position {}, requested position {}." +
                                "Please make sure that CopyOnRead and prefetch of index files are enabled.",
                        getName(), position(), pos);

                // seeking back on input stream. Close current one
                IOUtils.closeQuietly(blobInputStream);
                blobInputStream = null;
                position = pos;
            } else {
                while (position < pos) {
                    long skipCnt = blobInputStream.skip(pos - position());
                    if (skipCnt <= 0) {
                        String msg = String.format("Seek request for [%s][%s], " +
                                "position: %d, file length: %d failed. InputStream.skip returned %d",
                                dirDetails, name, pos, length, skipCnt);
                        releaseInputStream();
                        throw new IOException(msg);
                    }
                    position += skipCnt;
                }
            }
        }
    }

    @Override
    public void readBytes(byte[] b, int offset, int len)
            throws IOException {
        checkPositionIndexes(offset, offset + len, checkNotNull(b).length);

        if (len < 0 || position + len > length) {
            String msg = String.format("Invalid byte range request for [%s][%s], " +
                    "position: %d, file length: %d, len: %d", dirDetails, name, position, length, len);
            releaseInputStream();
            throw new IOException(msg);
        }

        setupInputStream();
        int readCnt = ByteStreams.read(blobInputStream, b, offset, len);
        if (readCnt < len) {
            String msg = String.format("Couldn't read byte range request for [%s][%s], " +
                    "position: %d, file length: %d, len: %d. Actual read bytes %d",
                    dirDetails, name, position, length, len, readCnt);
            releaseInputStream();
            throw new IOException(msg);
        }

        position += len;
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int len)
            throws IOException {
        if (blobModified) {
            throw new IllegalArgumentException("Can't do piece wise upload with streaming access");
        }

        InputStream in = new InputStream() {
            int position = offset;

            @Override
            public int available() throws IOException {
                return offset + len - position;
            }

            @Override
            public int read() throws IOException {
                if (available() <= 0) {
                    return -1;
                } else {
                    int ret = b[position++];
                    return ret < 0 ? 256 + ret: ret;
                }
            }

            @Override
            public int read(@Nonnull byte[] target, int off, int len) throws IOException {
                if (available() <= 0) {
                    return -1;
                }

                int read = (int)Math.min((long)len, available());
                System.arraycopy(b, position, target, off, read);

                position += read;

                return read;
            }
        };

        pushData(in);
    }

    @Override
    public boolean supportsCopyFromDataInput() {
        return true;
    }

    @Override
    public void copyBytes(DataInput input, final long numBytes) throws IOException {
        InputStream in = new InputStream() {
            long bytesLeftToRead = numBytes;

            @Override
            public int read() throws IOException {
                if (bytesLeftToRead <= 0) {
                    return -1;
                } else {
                    bytesLeftToRead--;
                    int ret = input.readByte();
                    return ret < 0 ? 256 + ret: ret;
                }
            }

            @Override
            public int read(@Nonnull byte[] b, int off, int len) throws IOException {
                if (bytesLeftToRead == 0) {
                    return -1;
                }

                int read = (int)Math.min((long)len, bytesLeftToRead);
                input.readBytes(b, off, read);

                bytesLeftToRead -= read;

                return read;
            }
        };

        pushData(in);
    }

    private void pushData(InputStream in) throws IOException {
        if (uniqueKey != null) {
            in = new SequenceInputStream(in,
                    new ByteArrayInputStream(uniqueKey));
        }

        blob = blobFactory.createBlob(in);
        blobModified = true;
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
        if (blobModified) {
            file.setProperty(JCR_LASTMODIFIED, System.currentTimeMillis());
            file.setProperty(JCR_DATA, blob, BINARY);
            blobModified = false;
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
