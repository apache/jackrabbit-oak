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
package org.apache.jackrabbit.oak.segment.file.tar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.jetbrains.annotations.NotNull;

/**
 * Recovers the binary references of segments with the {@link TarRecovery} of the store they belong
 * to, without writing anything. The segments must be readable through that store, so that long
 * blob ids stored in other segments can be resolved.
 */
public final class BinaryReferencesRecovery {

    @FunctionalInterface
    public interface BinaryReferenceConsumer {
        void accept(GCGeneration generation, UUID segmentId, String reference);
    }

    private final TarRecovery recovery;

    private final EntryRecovery entryRecovery;

    public BinaryReferencesRecovery(@NotNull TarRecovery recovery, @NotNull BinaryReferenceConsumer consumer) {
        this.recovery = recovery;
        this.entryRecovery = new BinaryReferencesEntryRecovery(consumer);
    }

    public void recover(@NotNull UUID segmentId, byte @NotNull [] data) throws IOException {
        recovery.recoverEntry(segmentId, data, entryRecovery);
    }

    public void recover(@NotNull UUID segmentId, @NotNull ByteBuffer data) throws IOException {
        ByteBuffer buffer = data.duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        recover(segmentId, bytes);
    }

    private static class BinaryReferencesEntryRecovery implements EntryRecovery {

        private final BinaryReferenceConsumer consumer;

        BinaryReferencesEntryRecovery(BinaryReferenceConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void recoverEntry(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) {
        }

        @Override
        public void recoverGraphEdge(UUID from, UUID to) {
        }

        @Override
        public void recoverBinaryReference(GCGeneration generation, UUID segmentId, String reference) {
            consumer.accept(generation, segmentId, reference);
        }

        @Override
        public Segment getSegment(SegmentId id) {
            return null;
        }

        @Override
        public void addSegment(Segment segment) {
        }

        // Segments are readable through the store, so they don't need to be kept here.
        @Override
        public Map<SegmentId, Segment> getRecoveredSegments() {
            return Collections.emptyMap();
        }
    }
}
