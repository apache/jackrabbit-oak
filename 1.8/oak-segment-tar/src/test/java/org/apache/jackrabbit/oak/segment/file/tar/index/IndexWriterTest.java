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

import static org.apache.jackrabbit.oak.segment.file.tar.index.IndexWriter.newIndexWriter;
import static org.junit.Assert.assertArrayEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

public class IndexWriterTest {

    @Test
    public void testWrite() throws Exception {
        IndexWriter writer = newIndexWriter(1);
        writer.addEntry(7, 8, 9, 10, 11, 12, true);
        writer.addEntry(1, 2, 3, 4, 5, 6, false);
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0)
                .putLong(7).putLong(8).putInt(9).putInt(10).putInt(11).putInt(12).put((byte) 1)
                .putInt(0xE2138EB4)
                .putInt(2)
                .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        assertArrayEquals(buffer.array(), writer.write());
    }

    @Test
    public void testPadding() throws Exception {
        IndexWriter writer = newIndexWriter(256);
        writer.addEntry(7, 8, 9, 10, 11, 12, true);
        writer.addEntry(1, 2, 3, 4, 5, 6, false);
        int dataSize = 2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(256);
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(duplicate.limit() - dataSize);
        duplicate
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0)
                .putLong(7).putLong(8).putInt(9).putInt(10).putInt(11).putInt(12).put((byte) 1)
                .putInt(0xE2138EB4)
                .putInt(2)
                .putInt(256)
                .putInt(IndexLoaderV2.MAGIC);
        assertArrayEquals(buffer.array(), writer.write());
    }

}
