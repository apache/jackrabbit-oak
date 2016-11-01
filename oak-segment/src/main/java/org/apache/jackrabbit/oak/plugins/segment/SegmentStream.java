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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.annotation.CheckForNull;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

/**
 * For reading any record of type "VALUE" as binary streams.
 */
@Deprecated
public class SegmentStream extends InputStream {

    @CheckForNull
    @Deprecated
    public static RecordId getRecordIdIfAvailable(
            InputStream stream, SegmentStore store) {
        if (stream instanceof SegmentStream) {
            SegmentStream sstream = (SegmentStream) stream;
            RecordId id = sstream.recordId;
            if (sstream.position == 0
                    && store.containsSegment(id.getSegmentId())) {
                return id;
            }
        }
        return null;
    }

    private final RecordId recordId;

    private final byte[] inline;

    private final ListRecord blocks;

    private final long length;

    private long position = 0;

    private long mark = 0;

    SegmentStream(RecordId recordId, ListRecord blocks, long length) {
        this.recordId = checkNotNull(recordId);
        this.inline = null;
        this.blocks = checkNotNull(blocks);
        checkArgument(length >= 0);
        this.length = length;
    }

    SegmentStream(RecordId recordId, byte[] inline) {
        this.recordId = checkNotNull(recordId);
        this.inline = checkNotNull(inline);
        this.blocks = null;
        this.length = inline.length;
    }

    @Deprecated
    public long getLength() {
        return length;
    }

    @Deprecated
    public String getString() {
        if (inline != null) {
            return new String(inline, Charsets.UTF_8);
        } else if (length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Too long value: " + length);
        } else {
            SegmentStream stream = new SegmentStream(recordId, blocks, length);
            try {
                byte[] data = new byte[(int) length];
                ByteStreams.readFully(stream, data);
                return new String(data, Charsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException("Unexpected IOException", e);
            } finally {
                stream.close();
            }
        }
    }

    @Override
    @Deprecated
    public boolean markSupported() {
        return true;
    }

    @Override
    @Deprecated
    public synchronized void mark(int readlimit) {
        mark = position;
    }

    @Override
    @Deprecated
    public synchronized void reset() {
        position = mark;
    }

    @Override
    @Deprecated
    public int read() {
        byte[] b = new byte[1];
        if (read(b, 0, 1) != -1) {
            return b[0] & 0xff;
        } else {
            return -1;
        }
    }

    @Override
    @Deprecated
    public int read(byte[] b, int off, int len) {
        checkNotNull(b);
        checkPositionIndexes(off, off + len, b.length);

        if (len == 0) {
            return 0;
        } else if (position == length) {
            return -1;
        }

        if (position + len > length) {
            len = (int) (length - position); // > 0 given the earlier check
        }

        if (inline != null) {
            System.arraycopy(inline, (int) position, b, off, len);
        } else {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int blockOffset = (int) (position % BLOCK_SIZE);
            int blockCount =
                    (blockOffset + len + BLOCK_SIZE - 1) // round up
                    / BLOCK_SIZE;

            int remaining = len;
            List<RecordId> ids = blocks.getEntries(blockIndex, blockCount);
            RecordId first = ids.get(0); // guaranteed to contain at least one
            int count = 1;
            for (int i = 1; i <= ids.size(); i++) {
                RecordId id = null;
                if (i < ids.size()) {
                    id = ids.get(i);
                }

                if (id != null
                        && id.getSegmentId().equals(first.getSegmentId())
                        && id.getOffset() == first.getOffset() + count * BLOCK_SIZE) {
                    count++;
                } else {
                    int blockSize = Math.min(
                            blockOffset + remaining, count * BLOCK_SIZE);
                    BlockRecord block = new BlockRecord(first, blockSize);
                    int n = blockSize - blockOffset;
                    checkState(block.read(blockOffset, b, off, n) == n);
                    off += n;
                    remaining -= n;

                    first = id;
                    count = 1;
                    blockOffset = 0;
                }
            }
            checkState(remaining == 0);
        }

        position += len;
        return len;
    }

    @Override
    @Deprecated
    public long skip(long n) {
        if (position + n > length) {
            n = length - position;
        } else if (position + n < 0) {
            n = -position;
        }
        position += n;
        return n;
    }

    @Override
    @Deprecated
    public int available() {
        if (inline != null) {
            return (int) (length - position); // <= inline.length
        } else {
            return 0;
        }
    }

    @Override
    @Deprecated
    public void close() {
        position = length;
    }

}
