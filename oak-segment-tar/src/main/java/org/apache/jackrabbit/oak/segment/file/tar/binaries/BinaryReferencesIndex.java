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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An index of binary references.
 */
public class BinaryReferencesIndex {

    /**
     * A consumer of entries from a binary references index.
     */
    public interface EntryConsumer {

        /**
         * Consume an entry from a binary references index.
         *
         * @param generation The generation of the segment containing the binary
         *                   reference.
         * @param full       The full generation of the segment containing the
         *                   binary reference.
         * @param compacted  {@code true} if the segment was created by a
         *                   compaction operation.
         * @param segment    The identifier of the segment containing the binary
         *                   reference.
         * @param reference  The binary reference.
         */
        void consume(int generation, int full, boolean compacted, UUID segment, String reference);

    }

    private final Map<Generation, Map<UUID, Set<String>>> references;

    BinaryReferencesIndex(Map<Generation, Map<UUID, Set<String>>> references) {
        this.references = references;
    }

    /**
     * Iterate over every entry in this index.
     *
     * @param consumer An instance of {@link EntryConsumer}.
     */
    public void forEach(EntryConsumer consumer) {
        references.forEach((generation, entries) -> {
            entries.forEach((segment, references) -> {
                references.forEach(reference -> {
                    consumer.consume(
                        generation.generation,
                        generation.full,
                        generation.compacted,
                        segment,
                        reference
                    );
                });
            });
        });
    }

}
