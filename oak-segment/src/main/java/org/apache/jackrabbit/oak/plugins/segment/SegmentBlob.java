/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * A BLOB (stream of bytes). This is a record of type "VALUE".
 */
@Deprecated
public class SegmentBlob extends Record implements Blob {

    @Deprecated
    public static Iterable<SegmentId> getBulkSegmentIds(Blob blob) {
        if (blob instanceof SegmentBlob) {
            return ((SegmentBlob) blob).getBulkSegmentIds();
        } else {
            return emptySet();
        }
    }

    SegmentBlob(RecordId id) {
        super(id);
    }

    private InputStream getInlineStream(
            Segment segment, int offset, int length) {
        byte[] inline = new byte[length];
        segment.readBytes(offset, inline, 0, length);
        return new SegmentStream(getRecordId(), inline);
    }

    @Override @Nonnull
    @Deprecated
    public InputStream getNewStream() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return getInlineStream(segment, offset + 1, head);
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            int length = (segment.readShort(offset) & 0x3fff) + SMALL_LIMIT;
            return getInlineStream(segment, offset + 2, length);
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (segment.readLong(offset) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(
                    segment.readRecordId(offset + 8), listSize);
            return new SegmentStream(getRecordId(), list, length);
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, short blob ID
            return getNewStream(readShortBlobId(segment, offset, head));
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return getNewStream(readLongBlobId(segment, offset));
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    @Deprecated
    public long length() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return head;
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            return (segment.readShort(offset) & 0x3fff) + SMALL_LIMIT;
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            return (segment.readLong(offset) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, short blob ID
            return getLength(readShortBlobId(segment, offset, head));
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return getLength(readLongBlobId(segment, offset));
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    @CheckForNull
    @Deprecated
    public String getReference() {
        String blobId = getBlobId();
        if (blobId != null) {
            BlobStore blobStore = getSegment().getSegmentId().getTracker().
                    getStore().getBlobStore();
            if (blobStore != null) {
                return blobStore.getReference(blobId);
            } else {
                throw new IllegalStateException("Attempt to read external blob with blobId [" + blobId + "] " +
                        "without specifying BlobStore");
            }
        }
        return null;
    }


    @Override
    @Deprecated
    public String getContentIdentity() {
        String blobId = getBlobId();
        if (blobId != null){
            return blobId;
        }
        return null;
    }

    @Deprecated
    public boolean isExternal() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        // 1110 xxxx or 1111 0xxx: external value
        return (head & 0xf0) == 0xe0 || (head & 0xf8) == 0xf0;
    }

    @Deprecated
    public String getBlobId() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, small blob ID
            return readShortBlobId(segment, offset, head);
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return readLongBlobId(segment, offset);
        } else {
            return null;
        }
    }

    @Deprecated
    public SegmentBlob clone(SegmentWriter writer, boolean cloneLargeBinaries) throws IOException {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return writer.writeStream(new BufferedInputStream(getNewStream()));
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            return writer.writeStream(new BufferedInputStream(getNewStream()));
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            if (cloneLargeBinaries) {
                return writer.writeStream(new BufferedInputStream(
                        getNewStream()));
            } else {
                // this was the previous (default) behavior
                long length = (segment.readLong(offset) & 0x1fffffffffffffffL)
                        + MEDIUM_LIMIT;
                int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
                ListRecord list = new ListRecord(
                        segment.readRecordId(offset + 8), listSize);
                return writer.writeLargeBlob(length, list.getEntries());
            }
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, short blob ID
            return writer.writeExternalBlob(getBlobId());
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return writer.writeExternalBlob(getBlobId());
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    @Deprecated
    public boolean equals(Object object) {
        if (object == this || fastEquals(this, object)) {
            return true;
        } else if (object instanceof SegmentBlob) {
            SegmentBlob that = (SegmentBlob) object;
            if (this.wasCompactedTo(that) || that.wasCompactedTo(this)) {
                return true;
            }
        }
        return object instanceof Blob
                && AbstractBlob.equal(this, (Blob) object);
    }

    @Override
    @Deprecated
    public int hashCode() {
        return 0;
    }

    //-----------------------------------------------------------< private >--

    private static String readShortBlobId(Segment segment, int offset, byte head) {
        int length = (head & 0x0f) << 8 | (segment.readByte(offset + 1) & 0xff);
        byte[] bytes = new byte[length];
        segment.readBytes(offset + 2, bytes, 0, length);
        return new String(bytes, UTF_8);
    }

    private static String readLongBlobId(Segment segment, int offset) {
        RecordId blobIdRecordId = segment.readRecordId(offset + 1);
        return Segment.readString(blobIdRecordId);
    }

    private Iterable<SegmentId> getBulkSegmentIds() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (segment.readLong(offset) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(
                    segment.readRecordId(offset + 8), listSize);
            Set<SegmentId> ids = newHashSet();
            for (RecordId id : list.getEntries()) {
                ids.add(id.getSegmentId());
            }
            return ids;
        } else {
            return emptySet();
        }
    }

    private Blob getBlob(String blobId) {
        return getSegment().getSegmentId().getTracker().getStore().readBlob(blobId);
    }

    private InputStream getNewStream(String blobId) {
        return getBlob(blobId).getNewStream();
    }

    private long getLength(String blobId) {
        long length = getBlob(blobId).length();

        if (length == -1) {
            throw new IllegalStateException(String.format("Unknown length of external binary: %s", blobId));
        }

        return length;
    }

}
