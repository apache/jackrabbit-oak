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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

/**
 * For reading any record of type "VALUE" as binary streams.
 */
public class SegmentStream extends InputStream {

    static final int BLOCK_SIZE = 1 << 12; // 4kB

    @CheckForNull
    public static RecordId getRecordIdIfAvailable(
            InputStream stream, SegmentStore store) {
        if (stream instanceof SegmentStream) {
            SegmentStream sStream = (SegmentStream) stream;
            RecordId id = sStream.recordId;
            if (sStream.position == 0 && id.getSegmentId().sameStore(store)) {
                return id;
            }
        }
        return null;
    }

    private final RecordId recordId;

    private final ByteBuffer inline;

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

    SegmentStream(RecordId recordId, ByteBuffer inline, int length) {
        this.recordId = checkNotNull(recordId);
        this.inline = inline.duplicate();
        this.blocks = null;
        this.length = length;
    }

    List<RecordId> getBlockIds() {
        if (blocks == null) {
            return null;
        } else {
            return blocks.getEntries();
        }
    }

    public long getLength() {
        return length;
    }

    public String getString() {
        if (inline != null) {
            return Charsets.UTF_8.decode(inline).toString();
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
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readLimit) {
        mark = position;
    }

    @Override
    public synchronized void reset() {
        position = mark;
    }

    @Override
    public int read() {
        byte[] b = new byte[1];
        if (read(b, 0, 1) != -1) {
            return b[0] & 0xff;
        } else {
            return -1;
        }
    }

    @Override
    public int read(@Nonnull byte[] b, int off, int len) {
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
            inline.position((int) position);
            inline.get(b, off, len);
        } else {
            int blockIndex = (int) (position / BLOCK_SIZE);
            int blockOffset = (int) (position % BLOCK_SIZE);
            int blockCount =
                    (blockOffset + len + BLOCK_SIZE - 1) // round up
                    / BLOCK_SIZE;

            int remaining = len;
            List<RecordId> ids = blocks.getEntries(blockIndex, blockCount);
            RecordId previousId = ids.get(0); // guaranteed to contain at least one
            int consecutiveBlocks = 1;
            for (int i = 1; i <= ids.size(); i++) {
                RecordId id = null;           // id is null for the last iteration only
                if (i < ids.size()) {
                    id = ids.get(i);
                }

                // Id can only be null in the last iteration. But then previousId cannot be
                // null as this would imply id was null in the previous iteration.
                assert id == null || previousId != null;
                if (id != null
                        // blocks are in the same segment
                        && id.getSegmentId().equals(previousId.getSegmentId())

                        // blocks are consecutive
                        && id.getRecordNumber() == previousId.getRecordNumber() + consecutiveBlocks * BLOCK_SIZE) {

                    consecutiveBlocks++;
                } else {
                    // read the current chunk of consecutive blocks
                    int blockSize = Math.min(
                            blockOffset + remaining, consecutiveBlocks * BLOCK_SIZE);
                    BlockRecord block = new BlockRecord(previousId, blockSize);
                    int n = blockSize - blockOffset;
                    checkState(block.read(blockOffset, b, off, n) == n);
                    off += n;
                    remaining -= n;

                    previousId = id;
                    consecutiveBlocks = 1;
                    blockOffset = 0;
                }
            }
            checkState(remaining == 0);
        }

        position += len;
        return len;
    }

    @Override
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
    public int available() {
        if (inline != null) {
            return (int) (length - position); // <= inline.length
        } else {
            return 0;
        }
    }

    @Override
    public void close() {
        position = length;
    }

}
