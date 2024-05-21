/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.index;

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

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Holder class for common parameters used during read.
 *
 * @lucene.experimental
 */
public class SegmentReadState {

    /**
     * {@link Directory} where this segment is read from.
     */
    public final Directory directory;

    /**
     * {@link SegmentInfo} describing this segment.
     */
    public final SegmentInfo segmentInfo;

    /**
     * {@link FieldInfos} describing all fields in this segment.
     */
    public final FieldInfos fieldInfos;

    /**
     * {@link IOContext} to pass to {@link Directory#openInput(String, IOContext)}.
     */
    public final IOContext context;

    /**
     * The {@code termInfosIndexDivisor} to use, if appropriate (not all {@link PostingsFormat}s
     * support it; in particular the current default does not).
     *
     * <p>  NOTE: if this is &lt; 0, that means "defer terms index
     * load until needed".  But if the codec must load the terms index on init (preflex is the only
     * once currently that must do so), then it should negate this value to get the app's terms
     * divisor
     */
    public int termsIndexDivisor;

    /**
     * Unique suffix for any postings files read for this segment.  {@link PerFieldPostingsFormat}
     * sets this for each of the postings formats it wraps.  If you create a new
     * {@link PostingsFormat} then any files you write/read must be derived using this suffix (use
     * {@link IndexFileNames#segmentFileName(String, String, String)}).
     */
    public final String segmentSuffix;

    /**
     * Create a {@code SegmentReadState}.
     */
    public SegmentReadState(Directory dir, SegmentInfo info,
        FieldInfos fieldInfos, IOContext context, int termsIndexDivisor) {
        this(dir, info, fieldInfos, context, termsIndexDivisor, "");
    }

    /**
     * Create a {@code SegmentReadState}.
     */
    public SegmentReadState(Directory dir,
        SegmentInfo info,
        FieldInfos fieldInfos,
        IOContext context,
        int termsIndexDivisor,
        String segmentSuffix) {
        this.directory = dir;
        this.segmentInfo = info;
        this.fieldInfos = fieldInfos;
        this.context = context;
        this.termsIndexDivisor = termsIndexDivisor;
        this.segmentSuffix = segmentSuffix;
    }

    /**
     * Create a {@code SegmentReadState}.
     */
    public SegmentReadState(SegmentReadState other,
        String newSegmentSuffix) {
        this.directory = other.directory;
        this.segmentInfo = other.segmentInfo;
        this.fieldInfos = other.fieldInfos;
        this.context = other.context;
        this.termsIndexDivisor = other.termsIndexDivisor;
        this.segmentSuffix = newSegmentSuffix;
    }
}
