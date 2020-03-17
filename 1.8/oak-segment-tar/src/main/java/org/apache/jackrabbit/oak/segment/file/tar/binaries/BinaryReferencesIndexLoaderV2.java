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

package org.apache.jackrabbit.oak.segment.file.tar.binaries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;

class BinaryReferencesIndexLoaderV2 {

    static final int MAGIC = ('\n' << 24) + ('1' << 16) + ('B' << 8) + '\n';

    static final int FOOTER_SIZE = 16;

    static BinaryReferencesIndex loadBinaryReferencesIndex(ReaderAtEnd reader) throws IOException, InvalidBinaryReferencesIndexException {
        ByteBuffer meta = reader.readAtEnd(FOOTER_SIZE, FOOTER_SIZE);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int size = meta.getInt();
        int magic = meta.getInt();

        if (magic != MAGIC) {
            throw new InvalidBinaryReferencesIndexException("Invalid magic number");
        }
        if (count < 0) {
            throw new InvalidBinaryReferencesIndexException("Invalid count");
        }
        if (size < count * 22 + 16) {
            throw new InvalidBinaryReferencesIndexException("Invalid size");
        }

        ByteBuffer buffer = reader.readAtEnd(size, size - FOOTER_SIZE);

        CRC32 checksum = new CRC32();
        byte[] data = new byte[size - FOOTER_SIZE];
        buffer.mark();
        buffer.get(data);
        buffer.reset();
        checksum.update(data);

        if ((int) (checksum.getValue()) != crc32) {
            throw new InvalidBinaryReferencesIndexException("Invalid checksum");
        }

        return new BinaryReferencesIndex(parseBinaryReferencesIndex(count, buffer));
    }

    private static Map<Generation, Map<UUID, Set<String>>> parseBinaryReferencesIndex(int count, ByteBuffer buffer) {
        Map<Generation, Map<UUID, Set<String>>> result = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            Generation k = parseGeneration(buffer);
            Map<UUID, Set<String>> v = parseEntriesBySegment(buffer);
            result.put(k, v);
        }
        return result;
    }

    private static Generation parseGeneration(ByteBuffer buffer) {
        int generation = buffer.getInt();
        int full = buffer.getInt();
        boolean compacted = buffer.get() != 0;
        return new Generation(generation, full, compacted);
    }

    private static Map<UUID, Set<String>> parseEntriesBySegment(ByteBuffer buffer) {
        return parseEntriesBySegment(buffer.getInt(), buffer);
    }

    private static Map<UUID, Set<String>> parseEntriesBySegment(int count, ByteBuffer buffer) {
        Map<UUID, Set<String>> result = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            UUID k = parseUUID(buffer);
            Set<String> v = parseEntries(buffer);
            result.put(k, v);
        }
        return result;
    }

    private static UUID parseUUID(ByteBuffer buffer) {
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }

    private static Set<String> parseEntries(ByteBuffer buffer) {
        return parseEntries(buffer.getInt(), buffer);
    }

    private static Set<String> parseEntries(int count, ByteBuffer buffer) {
        Set<String> entries = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(parseString(buffer));
        }
        return entries;
    }

    private static String parseString(ByteBuffer buffer) {
        return parseString(buffer.getInt(), buffer);
    }

    private static String parseString(int length, ByteBuffer buffer) {
        byte[] data = new byte[length];
        buffer.get(data);
        return new String(data, Charsets.UTF_8);
    }

}
