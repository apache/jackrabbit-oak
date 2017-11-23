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

import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;

public class BinaryReferencesIndexLoader {

    private BinaryReferencesIndexLoader() {
        // Prevent external instantiation
    }

    /**
     * Load and validate a binary references index. The binary references index
     * is read backward. The medium the index is read from is represented by an
     * instance of {@link ReaderAtEnd}. The {@link ReaderAtEnd} should behave as
     * it is positioned at the end of the binary references index.
     *
     * @param reader An instance of {@link ReaderAtEnd}.
     * @return The binary references index.
     * @throws IOException                           if an error occurs while
     *                                               reading the index.
     * @throws InvalidBinaryReferencesIndexException if the index is invalid or
     *                                               malformed.
     */
    public static BinaryReferencesIndex loadBinaryReferencesIndex(ReaderAtEnd reader) throws IOException, InvalidBinaryReferencesIndexException {
        switch (readMagic(reader)) {
            case BinaryReferencesIndexLoaderV1.MAGIC:
                return BinaryReferencesIndexLoaderV1.loadBinaryReferencesIndex(reader);
            case BinaryReferencesIndexLoaderV2.MAGIC:
                return BinaryReferencesIndexLoaderV2.loadBinaryReferencesIndex(reader);
            default:
                throw new InvalidBinaryReferencesIndexException("Unrecognized magic number");
        }
    }

    private static int readMagic(ReaderAtEnd reader) throws IOException {
        return reader.readAtEnd(Integer.BYTES, Integer.BYTES).getInt();
    }

}
