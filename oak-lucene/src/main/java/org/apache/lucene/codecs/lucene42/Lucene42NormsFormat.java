/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.codecs.lucene42;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Lucene 4.2 score normalization format.
 * <p>
 * NOTE: this uses the same format as {@link Lucene42DocValuesFormat} Numeric DocValues, but with
 * different file extensions, and passing {@link PackedInts#FASTEST} for uncompressed encoding:
 * trading off space for performance.
 * <p>
 * Files:
 * <ul>
 *   <li><tt>.nvd</tt>: DocValues data</li>
 *   <li><tt>.nvm</tt>: DocValues metadata</li>
 * </ul>
 *
 * @see Lucene42DocValuesFormat
 */
public class Lucene42NormsFormat extends NormsFormat {

    final float acceptableOverheadRatio;

    /**
     * Calls {@link #Lucene42NormsFormat(float) Lucene42DocValuesFormat(PackedInts.FASTEST)}
     */
    public Lucene42NormsFormat() {
        // note: we choose FASTEST here (otherwise our norms are half as big but 15% slower than previous lucene)
        this(PackedInts.FASTEST);
    }

    /**
     * Creates a new Lucene42DocValuesFormat with the specified
     * <code>acceptableOverheadRatio</code> for NumericDocValues.
     *
     * @param acceptableOverheadRatio compression parameter for numerics. Currently this is only
     *                                used when the number of unique values is small.
     * @lucene.experimental
     */
    public Lucene42NormsFormat(float acceptableOverheadRatio) {
        this.acceptableOverheadRatio = acceptableOverheadRatio;
    }

    @Override
    public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
        return new Lucene42NormsConsumer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC,
            METADATA_EXTENSION, acceptableOverheadRatio);
    }

    @Override
    public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
        return new Lucene42DocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC,
            METADATA_EXTENSION);
    }

    private static final String DATA_CODEC = "Lucene41NormsData";
    private static final String DATA_EXTENSION = "nvd";
    private static final String METADATA_CODEC = "Lucene41NormsMetadata";
    private static final String METADATA_EXTENSION = "nvm";
}
