/*
 * COPIED FROM APACHE LUCENE 4.7.2
 *
 * Git URL: git@github.com:apache/lucene.git, tag: releases/lucene-solr/4.7.2, path: lucene/core/src/java
 *
 * (see https://issues.apache.org/jira/browse/OAK-10786 for details)
 */

package org.apache.lucene.codecs;

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

import org.apache.lucene.index.SegmentInfo;

/**
 * Expert: Controls the format of the {@link SegmentInfo} (segment metadata file).
 * <p>
 *
 * @lucene.experimental
 * @see SegmentInfo
 */
public abstract class SegmentInfoFormat {

    /**
     * Sole constructor. (For invocation by subclass constructors, typically implicit.)
     */
    protected SegmentInfoFormat() {
    }

    /**
     * Returns the {@link SegmentInfoReader} for reading {@link SegmentInfo} instances.
     */
    public abstract SegmentInfoReader getSegmentInfoReader();

    /**
     * Returns the {@link SegmentInfoWriter} for writing {@link SegmentInfo} instances.
     */
    public abstract SegmentInfoWriter getSegmentInfoWriter();
}
