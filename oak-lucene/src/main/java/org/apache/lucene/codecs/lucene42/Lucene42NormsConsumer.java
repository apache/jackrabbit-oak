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
import java.util.HashMap;
import java.util.HashSet;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.FormatAndBits;

/**
 * Writer for {@link Lucene42NormsFormat}
 */
class Lucene42NormsConsumer extends DocValuesConsumer {

    static final int VERSION_START = 0;
    static final int VERSION_GCD_COMPRESSION = 1;
    static final int VERSION_CURRENT = VERSION_GCD_COMPRESSION;

    static final byte NUMBER = 0;

    static final int BLOCK_SIZE = 4096;

    static final byte DELTA_COMPRESSED = 0;
    static final byte TABLE_COMPRESSED = 1;
    static final byte UNCOMPRESSED = 2;
    static final byte GCD_COMPRESSED = 3;

    final IndexOutput data, meta;
    final int maxDoc;
    final float acceptableOverheadRatio;

    Lucene42NormsConsumer(SegmentWriteState state, String dataCodec, String dataExtension,
        String metaCodec, String metaExtension, float acceptableOverheadRatio) throws IOException {
        this.acceptableOverheadRatio = acceptableOverheadRatio;
        maxDoc = state.segmentInfo.getDocCount();
        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name,
                state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeHeader(data, dataCodec, VERSION_CURRENT);
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name,
                state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeHeader(meta, metaCodec, VERSION_CURRENT);
            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException {
        meta.writeVInt(field.number);
        meta.writeByte(NUMBER);
        meta.writeLong(data.getFilePointer());
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;
        long gcd = 0;
        // TODO: more efficient?
        HashSet<Long> uniqueValues = null;
        if (true) {
            uniqueValues = new HashSet<Long>();

            long count = 0;
            for (Number nv : values) {
                assert nv != null;
                final long v = nv.longValue();

                if (gcd != 1) {
                    if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) {
                        // in that case v - minValue might overflow and make the GCD computation return
                        // wrong results. Since these extreme values are unlikely, we just discard
                        // GCD computation for them
                        gcd = 1;
                    } else if (count != 0) { // minValue needs to be set first
                        gcd = MathUtil.gcd(gcd, v - minValue);
                    }
                }

                minValue = Math.min(minValue, v);
                maxValue = Math.max(maxValue, v);

                if (uniqueValues != null) {
                    if (uniqueValues.add(v)) {
                        if (uniqueValues.size() > 256) {
                            uniqueValues = null;
                        }
                    }
                }

                ++count;
            }
            assert count == maxDoc;
        }

        if (uniqueValues != null) {
            // small number of unique values
            final int bitsPerValue = PackedInts.bitsRequired(uniqueValues.size() - 1);
            FormatAndBits formatAndBits = PackedInts.fastestFormatAndBits(maxDoc, bitsPerValue,
                acceptableOverheadRatio);
            if (formatAndBits.bitsPerValue == 8 && minValue >= Byte.MIN_VALUE
                && maxValue <= Byte.MAX_VALUE) {
                meta.writeByte(UNCOMPRESSED); // uncompressed
                for (Number nv : values) {
                    data.writeByte(nv == null ? 0 : (byte) nv.longValue());
                }
            } else {
                meta.writeByte(TABLE_COMPRESSED); // table-compressed
                Long[] decode = uniqueValues.toArray(new Long[uniqueValues.size()]);
                final HashMap<Long, Integer> encode = new HashMap<Long, Integer>();
                data.writeVInt(decode.length);
                for (int i = 0; i < decode.length; i++) {
                    data.writeLong(decode[i]);
                    encode.put(decode[i], i);
                }

                meta.writeVInt(PackedInts.VERSION_CURRENT);
                data.writeVInt(formatAndBits.format.getId());
                data.writeVInt(formatAndBits.bitsPerValue);

                final PackedInts.Writer writer = PackedInts.getWriterNoHeader(data,
                    formatAndBits.format, maxDoc, formatAndBits.bitsPerValue,
                    PackedInts.DEFAULT_BUFFER_SIZE);
                for (Number nv : values) {
                    writer.add(encode.get(nv == null ? 0 : nv.longValue()));
                }
                writer.finish();
            }
        } else if (gcd != 0 && gcd != 1) {
            meta.writeByte(GCD_COMPRESSED);
            meta.writeVInt(PackedInts.VERSION_CURRENT);
            data.writeLong(minValue);
            data.writeLong(gcd);
            data.writeVInt(BLOCK_SIZE);

            final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
            for (Number nv : values) {
                long value = nv == null ? 0 : nv.longValue();
                writer.add((value - minValue) / gcd);
            }
            writer.finish();
        } else {
            meta.writeByte(DELTA_COMPRESSED); // delta-compressed

            meta.writeVInt(PackedInts.VERSION_CURRENT);
            data.writeVInt(BLOCK_SIZE);

            final BlockPackedWriter writer = new BlockPackedWriter(data, BLOCK_SIZE);
            for (Number nv : values) {
                writer.add(nv == null ? 0 : nv.longValue());
            }
            writer.finish();
        }
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeVInt(-1); // write EOF marker
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, final Iterable<BytesRef> values)
        throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedField(FieldInfo field, Iterable<BytesRef> values,
        Iterable<Number> docToOrd) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSortedSetField(FieldInfo field, Iterable<BytesRef> values,
        final Iterable<Number> docToOrdCount, final Iterable<Number> ords) throws IOException {
        throw new UnsupportedOperationException();
    }
}
