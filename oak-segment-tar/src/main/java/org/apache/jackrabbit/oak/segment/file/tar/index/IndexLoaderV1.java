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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import static java.nio.ByteBuffer.wrap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;

class IndexLoaderV1 {

    static final int MAGIC = ('\n' << 24) + ('0' << 16) + ('K' << 8) + '\n';

    private final int blockSize;

    IndexLoaderV1(int blockSize) {
        this.blockSize = blockSize;
    }

    IndexV1 loadIndex(ReaderAtEnd reader) throws InvalidIndexException, IOException {
        ByteBuffer meta = reader.readAtEnd(IndexV1.FOOTER_SIZE, IndexV1.FOOTER_SIZE);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != MAGIC) {
            throw new InvalidIndexException("Magic number mismatch");
        }
        if (count < 1) {
            throw new InvalidIndexException("Invalid entry count");
        }
        if (bytes < count * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE) {
            throw new InvalidIndexException("Invalid size");
        }
        if (bytes % blockSize != 0) {
            throw new InvalidIndexException("Invalid size alignment");
        }

        ByteBuffer entries = reader.readAtEnd(IndexV1.FOOTER_SIZE + count * IndexEntryV1.SIZE, count * IndexEntryV1.SIZE);

        CRC32 checksum = new CRC32();
        entries.mark();
        checksum.update(entries);
        entries.reset();
        if (crc32 != (int) checksum.getValue()) {
            throw new InvalidIndexException("Invalid checksum");
        }

        long lastMsb = Long.MIN_VALUE;
        long lastLsb = Long.MIN_VALUE;
        byte[] entry = new byte[IndexEntryV1.SIZE];
        entries.mark();
        for (int i = 0; i < count; i++) {
            entries.get(entry);

            ByteBuffer buffer = wrap(entry);
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int offset = buffer.getInt();
            int size = buffer.getInt();

            if (lastMsb > msb || (lastMsb == msb && lastLsb > lsb)) {
                throw new InvalidIndexException("Incorrect entry ordering");
            }
            if (lastMsb == msb && lastLsb == lsb && i > 0) {
                throw new InvalidIndexException("Duplicate entry");
            }
            if (offset < 0) {
                throw new InvalidIndexException("Invalid entry offset");
            }
            if (offset % blockSize != 0) {
                throw new InvalidIndexException("Invalid entry offset alignment");
            }
            if (size < 1) {
                throw new InvalidIndexException("Invalid entry size");
            }

            lastMsb = msb;
            lastLsb = lsb;
        }
        entries.reset();

        return new IndexV1(entries);
    }

}
