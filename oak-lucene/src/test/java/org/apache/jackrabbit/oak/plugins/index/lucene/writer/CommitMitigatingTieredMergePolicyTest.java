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

import java.io.IOException;
import java.util.Set;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergePolicy.MergeContext;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.InfoStream;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Tests for {@link CommitMitigatingTieredMergePolicy}
 */
public class CommitMitigatingTieredMergePolicyTest {

    @Test
    public void testMergeWithNoSegments() throws Exception {
        
        CommitMitigatingTieredMergePolicy mergePolicy = new CommitMitigatingTieredMergePolicy();
        
        // TODO: We should verify this is the right way to do the tests 
        SegmentInfos infos = new SegmentInfos(LuceneIndexConstants.VERSION.major);
        MergeContext mergeContext = new MergeContext() {
            @Override
            public int numDeletesToMerge(SegmentCommitInfo segmentCommitInfo) throws IOException {
                return 0;
            }

            @Override
            public int numDeletedDocs(SegmentCommitInfo segmentCommitInfo) {
                return 0;
            }

            @Override
            public InfoStream getInfoStream() {
                return null;
            }

            @Override
            public Set<SegmentCommitInfo> getMergingSegments() {
                return null;
            }
        };
        MergePolicy.MergeSpecification merges = mergePolicy.findMerges(MergeTrigger.SEGMENT_FLUSH, infos, mergeContext);
        assertNull(merges);
    }

}