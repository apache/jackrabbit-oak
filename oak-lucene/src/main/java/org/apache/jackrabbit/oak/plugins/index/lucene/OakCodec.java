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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.backward_codecs.lucene90.Lucene90Codec;
import org.apache.lucene.backward_codecs.lucene90.Lucene90FieldInfosFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90LiveDocsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90NormsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90SegmentInfoFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90TermVectorsFormat;

/**
 * Oak specific {@link org.apache.lucene.codecs.Codec}.
 *
 * It simply mimics {@link Lucene90Codec} but
 * with uncompressed {@link StoredFieldsFormat}.
 */
public class OakCodec extends FilterCodec {

    private static final String OAK_CODEC_NAME = "Lucene90";
    private final TermVectorsFormat vectorsFormat = new Lucene90TermVectorsFormat();
    private final FieldInfosFormat fieldInfosFormat = new Lucene90FieldInfosFormat();
    private final SegmentInfoFormat segmentInfosFormat = new Lucene90SegmentInfoFormat();
    private final LiveDocsFormat liveDocsFormat = new Lucene90LiveDocsFormat();
    private final PostingsFormat defaultFormat = PostingsFormat.forName(OAK_CODEC_NAME);
    private final DocValuesFormat defaultDVFormat = DocValuesFormat.forName(OAK_CODEC_NAME);
    private final NormsFormat normsFormat = new Lucene90NormsFormat();
    private final StoredFieldsFormat fieldsFormat = new Lucene90StoredFieldsFormat();

    public OakCodec() {
        super("oakCodec", new Lucene90Codec());
    }

    @Override
    public PostingsFormat postingsFormat() {
        return defaultFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return defaultDVFormat;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return fieldsFormat;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return vectorsFormat;
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public NormsFormat normsFormat() {
        return normsFormat;
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }
}
