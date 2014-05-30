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
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public class SegmentBlob extends Record implements Blob {

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
            // 1110 xxxx: external value
            String refererence = readReference(segment, offset, head);
            return segment.getSegmentId().getTracker().getStore()
                    .readBlob(refererence).getNewStream();
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override
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
            // 1110 xxxx: external value
            String reference = readReference(segment, offset, head);
            long length = segment.getSegmentId().getTracker().getStore()
                    .readBlob(reference).length();
            if (length == -1) {
                throw new IllegalStateException(
                        "Unknown length of external binary: " + reference);
            }
            return length;
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
            BlobStore blobStore = getSegment().getSegmentId().getTracker().
                    getStore().getBlobStore();
            if (blobStore != null) {
                return blobStore.getReference(blobId);
            }else{
                throw new IllegalStateException("Attempt to read external blob with blobId [" + blobId + "] " +
                        "without specifying BlobStore");
            }
        }
        return null;
    }


    @Override
    public String getContentIdentity() {
        return getRecordId().toString();
    }

    public boolean isExternal() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        // 1110 xxxx: external value
        return (head & 0xf0) == 0xe0;
    }

    public String getBlobId() {
        Segment segment = getSegment();
        int offset = getOffset();
        byte head = segment.readByte(offset);
        if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            return readReference(segment, offset, head);
        } else {
            return null;
        }
    }

    public SegmentBlob clone(SegmentWriter writer) throws IOException {
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
            long length = (segment.readLong(offset) & 0x1fffffffffffffffL) + MEDIUM_LIMIT;
            int listSize = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list = new ListRecord(
                    segment.readRecordId(offset + 8), listSize);
            return writer.writeLargeBlob(length, list.getEntries());
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            return writer.writeExternalBlob(getBlobId());
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (object == this || fastEquals(this, object)) {
            return true;
        } else {
            return object instanceof Blob
                    && AbstractBlob.equal(this, (Blob) object);
        }
    }

    @Override
    public int hashCode() {
        return 0;
    }

    //-----------------------------------------------------------< private >--

    private static String readReference(
            Segment segment, int offset, byte head) {
        int length = (head & 0x0f) << 8 | (segment.readByte(offset + 1) & 0xff);
        byte[] bytes = new byte[length];
        segment.readBytes(offset + 2, bytes, 0, length);
        return new String(bytes, UTF_8);
    }

}
