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

package org.apache.jackrabbit.oak.segment;

import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.segment.file.FileStore;

/**
 * Utility methods for working with binary references.
 */
public final class BinaryReferences {
    private BinaryReferences() {}

    /**
     * Creates a new instance of {@link BinaryReferenceConsumer} that ignores
     * every binary reference it consumes.
     *
     * @return A new instance of {@link BinaryReferenceConsumer}.
     */
    public static BinaryReferenceConsumer newDiscardBinaryReferenceConsumer() {
        return new BinaryReferenceConsumer() {

            @Override
            public void consume(int generation, UUID segmentId, String binaryReference) {
                // Discard the binary reference
            }

        };
    }

    /**
     * @param blobId
     * @return  A new id for a binary reference from an inlined {@code blobId}.
     */
    @Nonnull
    public static String newReference(@Nonnull String blobId) {
        return "I" + blobId;
    }

    /**
     * @param blobId
     * @return  A new id for a binary reference from a record id identifying a
     *          string record where the {@code blobId} is stored.
     */
    @Nonnull
    public static String newReference(@Nonnull RecordId blobId) {
        return "R" + blobId.toString10();
    }

    /**
     * @param fileStore
     * @return  A new reader for binary references for the passed {@code fileStore}.
     * @see #newReference(String)
     * @see #newReference(RecordId)
     */
    @Nonnull
    public static Function<String, String> newReferenceReader(@Nonnull final FileStore fileStore) {
        return new Function<String, String>() {
            @Nullable @Override
            public String apply(String reference) {
                if (reference.charAt(0) == 'I') {
                    return reference.substring(1);
                } else {
                    return fileStore.getReader().readString(
                            RecordId.fromString(fileStore, reference.substring(1)));
                }
            }

        };
    }
}
