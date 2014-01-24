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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;

import java.io.InputStream;

class SegmentBlob extends Record implements Blob {

    SegmentBlob(Segment segment, RecordId id) {
        super(segment, id);
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
                    segment, segment.readRecordId(offset + 8), listSize);
            return new SegmentStream(getStore(), getRecordId(), list, length);
        } else if ((head & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            int length = segment.readShort(offset) & 0x0fff;
            byte[] bytes = new byte[length];
            segment.readBytes(offset + 10, bytes, 0, length);
            String refererence = new String(bytes, UTF_8);
            return getStore().readBlob(refererence).getNewStream();
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
            return segment.readLong(offset + 2);
        } else {
            throw new IllegalStateException(String.format(
                    "Unexpected value record type: %02x", head & 0xff));
        }
    }

    @Override @CheckForNull
    public String getReference() {
        Segment segment = getSegment();
        int offset = getOffset();
        if ((segment.readByte(offset) & 0xf0) == 0xe0) {
            // 1110 xxxx: external value
            int length = segment.readShort(offset) & 0x0fff;
            byte[] bytes = new byte[length];
            segment.readBytes(offset + 10, bytes, 0, length);
            return new String(bytes, UTF_8);
        } else {
            return null;
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

}
