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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingTermVectorsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;

/**
 * Lucene Codec aimed to reduce index size as much as possible by enabling highest possible compression on term vectors and stored fields.
 * Updated for Lucene 5.5.x - uses Lucene54Codec as base and only overrides stored fields and term vectors formats.
 */
public class CompressingCodec extends FilterCodec {

    private static final int CHUNK_SIZE = 1024;
    private static final int MAX_DOCS_PER_CHUNK = 128;
    private static final int BLOCK_SIZE = 1024;
    private static final String SEGMENT_SUFFIX = "ctv";

    // In Lucene 5.x, CompressingTermVectorsFormat requires 5 parameters
    private final TermVectorsFormat vectorsFormat = new CompressingTermVectorsFormat("Lucene50",
            SEGMENT_SUFFIX, CompressionMode.HIGH_COMPRESSION, CHUNK_SIZE, BLOCK_SIZE);
    // In Lucene 5.x, CompressingStoredFieldsFormat requires 6 parameters:
    // formatName, segmentSuffix, compressionMode, chunkSize, maxDocsPerChunk, blockSize
    private final StoredFieldsFormat fieldsFormat = new CompressingStoredFieldsFormat("Lucene50",
            SEGMENT_SUFFIX, CompressionMode.HIGH_COMPRESSION, CHUNK_SIZE, MAX_DOCS_PER_CHUNK, BLOCK_SIZE);

    public CompressingCodec() {
        super("compressingCodec", new Lucene54Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return fieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }
}
