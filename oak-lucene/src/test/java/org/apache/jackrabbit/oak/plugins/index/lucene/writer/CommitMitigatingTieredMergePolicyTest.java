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
package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Tests for {@link CommitMitigatingTieredMergePolicy}
 */
public class CommitMitigatingTieredMergePolicyTest {

    @Test
    public void testMergeWithNoSegments() throws Exception {
        CommitMitigatingTieredMergePolicy mergePolicy = new CommitMitigatingTieredMergePolicy();

        // In Lucene 5.x, findMerges requires an IndexWriter parameter
        Directory directory = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new SimpleAnalyzer());
        config.setMergePolicy(mergePolicy);
        IndexWriter writer = new IndexWriter(directory, config);

        try {
            SegmentInfos infos = new SegmentInfos();
            MergePolicy.MergeSpecification merges = mergePolicy.findMerges(MergeTrigger.SEGMENT_FLUSH, infos, writer);
            assertNull(merges);
        } finally {
            writer.close();
            directory.close();
        }
    }

}
