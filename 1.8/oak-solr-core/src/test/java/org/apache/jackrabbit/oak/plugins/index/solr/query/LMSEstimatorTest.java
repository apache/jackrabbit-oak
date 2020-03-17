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
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link org.apache.jackrabbit.oak.plugins.index.solr.query.LMSEstimator}
 */
public class LMSEstimatorTest {

    @Test
    public void testUpdate() throws Exception {
        LMSEstimator lmsEstimator = new LMSEstimator();
        Filter filter = mock(Filter.class);
        SolrDocumentList docs = mock(SolrDocumentList.class);
        lmsEstimator.update(filter, docs);
    }

    @Test
    public void testMultipleUpdates() throws Exception {
        LMSEstimator lmsEstimator = new LMSEstimator();
        Filter filter = mock(Filter.class);
        FullTextExpression fte = new FullTextTerm("foo", "bar", false, false, "");
        when(filter.getFullTextConstraint()).thenReturn(fte);
        SolrDocumentList docs = new SolrDocumentList();
        lmsEstimator.update(filter, docs);

        long actualCount = 10;
        docs.setNumFound(actualCount);

        long estimate = lmsEstimator.estimate(filter);
        assertEquals(estimate, lmsEstimator.estimate(filter));
        long diff = actualCount - estimate;

        // update causes weights adjustment
        lmsEstimator.update(filter, docs);
        long estimate2 = lmsEstimator.estimate(filter);
        assertEquals(estimate2, lmsEstimator.estimate(filter));
        long diff2 = actualCount - estimate2;
        assertTrue(diff2 < diff); // new estimate is more accurate than previous one

        // update doesn't cause weight adjustments therefore estimates stays unchanged
        lmsEstimator.update(filter, docs);
        long estimate3 = lmsEstimator.estimate(filter);
        assertEquals(estimate3, lmsEstimator.estimate(filter));
        long diff3 = actualCount - estimate3;
        assertTrue(diff3 < diff2);
    }

    @Test
    public void testEstimate() throws Exception {
        LMSEstimator lmsEstimator = new LMSEstimator();
        Filter filter = mock(Filter.class);
        long estimate = lmsEstimator.estimate(filter);
        assertEquals(0L, estimate);
    }
}