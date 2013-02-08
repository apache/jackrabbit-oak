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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.InputStream;

public class SegmentStream extends InputStream {

    private final SegmentReader reader;

    private final ListRecord blocks;

    private final long length;

    private long position = 0;

    private long mark = 0;

    SegmentStream(SegmentReader reader, ListRecord blocks, long length) {
        this.reader = reader;
        this.blocks = blocks;
        this.length = length;
    }

    public long getLength() {
        return length;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readlimit) {
        mark = position;
    }

    @Override
    public synchronized void reset() {
        position = mark;
    }

    @Override
    public int read() {
        byte[] b = new byte[1];
        if (read(b) != -1) {
            return b[0] & 0xff;
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] b) {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) {
        checkNotNull(b);
        checkPositionIndexes(off, off + len, b.length);
        if (len == 0) {
            return 0;
        } else if (position == length) {
            return -1;
        } else {
            int blockIndex = (int) (position / SegmentWriter.BLOCK_SIZE);
            int blockOffset = (int) (position % SegmentWriter.BLOCK_SIZE);

            if (blockOffset + len > SegmentWriter.BLOCK_SIZE) {
                len = SegmentWriter.BLOCK_SIZE - blockOffset;
            }
            if (position + len > length) {
                len = (int) (length - position);
            }

            BlockRecord block = reader.readBlock(
                    blocks.getEntry(reader, blockIndex), BLOCK_SIZE);
            len = block.read(reader, blockOffset, b, off, len);
            position += len;
            return len;
        }
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
    public void close() {
        position = length;
    }

}
