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

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;

/**
 * Oak specific {@link org.apache.lucene.codecs.Codec} for Lucene 5.x.
 *
 * This codec is registered as "oakCodec5" and is based on {@link Lucene54Codec},
 * the default codec for Lucene 5.5.x. It should be used for all new indexes.
 *
 * <p>For reading indexes created with the old OakCodec (Lucene 4.x formats),
 * use {@link OakCodec} which maintains backward compatibility.</p>
 *
 * <p>In Lucene 5.x, the codec architecture was simplified and many of the
 * individual format classes were consolidated. This codec delegates to
 * the default Lucene54Codec which provides:</p>
 * <ul>
 *   <li>Lucene50StoredFieldsFormat (with compression)</li>
 *   <li>Lucene50TermVectorsFormat</li>
 *   <li>Lucene50PostingsFormat</li>
 *   <li>Lucene54DocValuesFormat</li>
 *   <li>Lucene53NormsFormat</li>
 *   <li>Lucene50LiveDocsFormat</li>
 *   <li>Lucene50CompoundFormat</li>
 *   <li>Lucene50SegmentInfoFormat</li>
 *   <li>Lucene60FieldInfosFormat</li>
 * </ul>
 */
public class OakCodec5 extends FilterCodec {

    public static final String NAME = "oakCodec5";

    public OakCodec5() {
        super(NAME, new Lucene54Codec());
    }
}

