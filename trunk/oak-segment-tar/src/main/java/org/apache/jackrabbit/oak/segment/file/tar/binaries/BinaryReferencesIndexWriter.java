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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import com.google.common.base.Charsets;

/**
 * Maintains the transient state of a binary references index, formats it and
 * serializes it.
 */
public class BinaryReferencesIndexWriter {

    /**
     * Create a new, empty instance of {@link BinaryReferencesIndexWriter}.
     *
     * @return An instance of {@link BinaryReferencesIndexWriter}.
     */
    public static BinaryReferencesIndexWriter newBinaryReferencesIndexWriter() {
        return new BinaryReferencesIndexWriter();
    }

    private final Map<Generation, Map<UUID, Set<String>>> entries;

    private BinaryReferencesIndexWriter() {
        entries = new HashMap<>();
    }

    /**
     * Add an entry to the binary references index.
     *
     * @param generation The generation of the segment containing the
     *                   reference.
     * @param full       The full generation of the segment containing the
     *                   reference.
     * @param compacted  {@code true} if the segment containing the reference is
     *                   created by a compaction operation.
     * @param segment    The identifier of the segment containing the
     *                   reference.
     * @param reference  The binary reference.
     */
    public void addEntry(int generation, int full, boolean compacted, UUID segment, String reference) {
        entries
            .computeIfAbsent(new Generation(generation, full, compacted), k -> new HashMap<>())
            .computeIfAbsent(segment, k -> new HashSet<>())
            .add(reference);
    }

    /**
     * Write the current state of this instance to an array of bytes.
     *
     * @return An array of bytes containing the serialized state of the binary
     * references index.
     */
    public byte[] write() {
        int binaryReferenceSize = 0;

        // The following information are stored in the footer as meta-
        // information about the entry.

        // 4 bytes to store a magic number identifying this entry as containing
        // references to binary values.
        binaryReferenceSize += 4;

        // 4 bytes to store the CRC32 checksum of the data in this entry.
        binaryReferenceSize += 4;

        // 4 bytes to store the length of this entry, without including the
        // optional padding.
        binaryReferenceSize += 4;

        // 4 bytes to store the number of generations pairs in the binary
        // references map.
        binaryReferenceSize += 4;

        // The following information are stored as part of the main content of
        // this entry, after the optional padding.

        for (Map<UUID, Set<String>> segmentToReferences : entries.values()) {
            // 4 bytes per generation to store the generation number.
            binaryReferenceSize += 4;

            // 4 bytes per generation to store the full generation number.
            binaryReferenceSize += 4;

            // 1 byte per generation to store the "compacted" flag.
            binaryReferenceSize += 1;

            // 4 bytes per generation to store the number of segments.
            binaryReferenceSize += 4;

            for (Set<String> references : segmentToReferences.values()) {
                // 16 bytes per segment identifier.
                binaryReferenceSize += 16;

                // 4 bytes to store the number of references for this segment.
                binaryReferenceSize += 4;

                for (String reference : references) {
                    // 4 bytes for each reference to store the length of the reference.
                    binaryReferenceSize += 4;

                    // A variable amount of bytes, depending on the reference itself.
                    binaryReferenceSize += reference.getBytes(Charsets.UTF_8).length;
                }
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(binaryReferenceSize);

        for (Entry<Generation, Map<UUID, Set<String>>> be : entries.entrySet()) {
            Generation generation = be.getKey();
            Map<UUID, Set<String>> segmentToReferences = be.getValue();

            buffer.putInt(generation.generation);
            buffer.putInt(generation.full);
            buffer.put((byte) (generation.compacted ? 1 : 0));
            buffer.putInt(segmentToReferences.size());

            for (Entry<UUID, Set<String>> se : segmentToReferences.entrySet()) {
                UUID segmentId = se.getKey();
                Set<String> references = se.getValue();

                buffer.putLong(segmentId.getMostSignificantBits());
                buffer.putLong(segmentId.getLeastSignificantBits());
                buffer.putInt(references.size());

                for (String reference : references) {
                    byte[] bytes = reference.getBytes(Charsets.UTF_8);

                    buffer.putInt(bytes.length);
                    buffer.put(bytes);
                }
            }
        }

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, buffer.position());

        buffer.putInt((int) checksum.getValue());
        buffer.putInt(entries.size());
        buffer.putInt(binaryReferenceSize);
        buffer.putInt(BinaryReferencesIndexLoaderV2.MAGIC);

        return buffer.array();
    }

}
