/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.segment.SegmentStream.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.segment.Segment.SMALL_LIMIT;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * A BLOB (stream of bytes). This is a record of type "VALUE".
 */
public class SegmentBlob extends Record implements Blob {

    @CheckForNull
    private final BlobStore blobStore;

    public static Iterable<SegmentId> getBulkSegmentIds(Blob blob) {
        if (blob instanceof SegmentBlob) {
            return ((SegmentBlob) blob).getBulkSegmentIds();
        } else {
            return emptySet();
        }
    }

    SegmentBlob(@Nullable BlobStore blobStore, @Nonnull RecordId id) {
        super(id);
        this.blobStore = blobStore;
    }

    private InputStream getInlineStream(Segment segment, int offset, int length) {
        return new SegmentStream(getRecordId(), segment.readBytes(getRecordNumber(), offset, length), length);
    }

    @Override @Nonnull
    public InputStream getNewStream() {
        Segment segment = getSegment();
        byte head = segment.readByte(getRecordNumber());
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return getInlineStream(segment, 1, head);
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            int length = (segment.readShort(getRecordNumber()) & 0x3fff) + SMALL_LIMIT;
            return getInlineStream(segment, 2, length);
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (segment.readLong(getRecordNumber()) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(segment.readRecordId(getRecordNumber(), 8), listSize);
            return new SegmentStream(getRecordId(), list, length);
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, short blob ID
            return getNewStream(readShortBlobId(segment, getRecordNumber(), head));
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return getNewStream(readLongBlobId(segment, getRecordNumber()));
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    public long length() {
        Segment segment = getSegment();
        byte head = segment.readByte(getRecordNumber());
        if ((head & 0x80) == 0x00) {
            // 0xxx xxxx: small value
            return head;
        } else if ((head & 0xc0) == 0x80) {
            // 10xx xxxx: medium value
            return (segment.readShort(getRecordNumber()) & 0x3fff) + SMALL_LIMIT;
        } else if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            return (segment.readLong(getRecordNumber()) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, short blob ID
            return getLength(readShortBlobId(segment, getRecordNumber(), head));
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return getLength(readLongBlobId(segment, getRecordNumber()));
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
    @CheckForNull
    public String getReference() {
        String blobId = getBlobId();
        if (blobId != null) {
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
    public String getContentIdentity() {
        String blobId = getBlobId();
        if (blobId != null){
            return blobId;
        }
        return null;
    }

    public boolean isExternal() {
        Segment segment = getSegment();
        byte head = segment.readByte(getRecordNumber());
        // 1110 xxxx or 1111 0xxx: external value
        return (head & 0xf0) == 0xe0 || (head & 0xf8) == 0xf0;
    }

    @CheckForNull
    public String getBlobId() {
        return readBlobId(getSegment(), getRecordNumber());
    }

    @CheckForNull
    public static String readBlobId(@Nonnull Segment segment, int recordNumber) {
        byte head = segment.readByte(recordNumber);
        if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value, small blob ID
            return readShortBlobId(segment, recordNumber, head);
        } else if ((head & 0xf8) == 0xf0) {
            // 1111 0xxx: external value, long blob ID
            return readLongBlobId(segment, recordNumber);
        } else {
            return null;
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (Record.fastEquals(this, object)) {
            return true;
        }

        if (object instanceof SegmentBlob) {
            SegmentBlob that = (SegmentBlob) object;
            if (this.length() != that.length()) {
                return false;
            }
            List<RecordId> bulkIds = this.getBulkRecordIds();
            if (bulkIds != null && bulkIds.equals(that.getBulkRecordIds())) {
                return true;
            }
        }

        return object instanceof Blob
                && AbstractBlob.equal(this, (Blob) object);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    //-----------------------------------------------------------< private >--

    private static String readShortBlobId(Segment segment, int recordNumber, byte head) {
        int length = (head & 0x0f) << 8 | (segment.readByte(recordNumber, 1) & 0xff);
        return UTF_8.decode(segment.readBytes(recordNumber, 2, length)).toString();
    }

    private static String readLongBlobId(Segment segment, int recordNumber) {
        RecordId blobId = segment.readRecordId(recordNumber, 1);
        return blobId.getSegment().readString(blobId.getRecordNumber());
    }

    private List<RecordId> getBulkRecordIds() {
        Segment segment = getSegment();
        byte head = segment.readByte(getRecordNumber());
        if ((head & 0xe0) == 0xc0) {
            // 110x xxxx: long value
            long length = (segment.readLong(getRecordNumber()) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(
                    segment.readRecordId(getRecordNumber(), 8), listSize);
            return list.getEntries();
        } else {
            return null;
        }
    }

    private Iterable<SegmentId> getBulkSegmentIds() {
        List<RecordId> recordIds = getBulkRecordIds();
        if (recordIds == null) {
            return emptySet();
        } else {
            Set<SegmentId> ids = newHashSet();
            for (RecordId id : recordIds) {
                ids.add(id.getSegmentId());
            }
            return ids;
        }
    }

    private Blob getBlob(String blobId) {
        if (blobStore != null) {
            return new BlobStoreBlob(blobStore, blobId);
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + blobId + "] " +
                "without specifying BlobStore");
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
