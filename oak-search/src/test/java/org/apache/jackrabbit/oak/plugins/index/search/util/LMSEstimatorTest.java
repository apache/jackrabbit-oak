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
package org.apache.jackrabbit.oak.plugins.index.search.util;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.query.SQL2ParserTest;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LMSEstimator}
 */
public class LMSEstimatorTest {

    private static final SQL2Parser p = SQL2ParserTest.createTestSQL2Parser();

    @Test
    public void testUpdate() throws Exception {
        LMSEstimator lmsEstimator = new LMSEstimator();
        Filter filter = mock(Filter.class);
        long numDocs = 100L;
        lmsEstimator.update(filter, numDocs);
    }

    @Test
    public void testMultipleUpdates() throws Exception {
        LMSEstimator lmsEstimator = new LMSEstimator();
        Filter filter = mock(Filter.class);
        FullTextExpression fte = new FullTextTerm("foo", "bar", false, false, "");
        when(filter.getFullTextConstraint()).thenReturn(fte);
        lmsEstimator.update(filter, 0);

        long actualCount = 10;

        long estimate = lmsEstimator.estimate(filter);
        assertEquals(estimate, lmsEstimator.estimate(filter));
        long diff = actualCount - estimate;

        // update causes weights adjustment
        lmsEstimator.update(filter, actualCount);
        long estimate2 = lmsEstimator.estimate(filter);
        assertEquals(estimate2, lmsEstimator.estimate(filter));
        long diff2 = actualCount - estimate2;
        assertTrue(diff2 < diff); // new estimate is more accurate than previous one

        // update doesn't cause weight adjustments therefore estimates stays unchanged
        lmsEstimator.update(filter, actualCount);
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

    @Test
    public void testConvergence() throws Exception {
        LMSEstimator lmsEstimator = new LMSEstimator();
        long mse = getMSE(lmsEstimator);
        int epochs = 15;
        for (int i = 1; i <= epochs; i++) {
            train(lmsEstimator);
            long currentMSE = getMSE(lmsEstimator);
            assertTrue(currentMSE <= mse);
            mse = currentMSE;
        }
    }

    private long getMSE(LMSEstimator lmsEstimator) throws Exception {
        int n = 0;
        long mse = 0;
        for (String line : IOUtils.readLines(getClass().getResourceAsStream("/lms-data.tsv"))) {
            String[] entries = line.split("\t");
            long numDocs = Long.parseLong(entries[1]);
            QueryImpl q = (QueryImpl) p.parse(entries[2]);
            Filter filter = q.getSource().createFilter(true);
            long estimate = lmsEstimator.estimate(filter);
            mse += Math.pow(numDocs - estimate, 2);
            n++;
        }
        mse /= n;
        return mse;
    }

    private void train(LMSEstimator lmsEstimator) throws Exception {
        List<String> strings = IOUtils.readLines(getClass().getResourceAsStream("/lms-data.tsv"));
        for (String line : strings) {
            String[] entries = line.split("\t");
            long numDocs = Long.parseLong(entries[1]);
            QueryImpl q = (QueryImpl) p.parse(entries[2]);
            Filter filter = q.getSource().createFilter(true);
            lmsEstimator.update(filter, numDocs);
        }
    }

}