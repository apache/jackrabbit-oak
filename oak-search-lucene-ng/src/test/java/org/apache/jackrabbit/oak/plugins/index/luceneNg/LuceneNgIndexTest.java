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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class LuceneNgIndexTest {

    /**
     * LuceneNgIndex is an AdvancedQueryIndex: cost is computed by the inherited
     * FulltextIndexPlanner via getPlans(...), and the simple getCost(Filter, NodeState) overload
     * is unsupported (throws, inherited from the base — the same contract as
     * LucenePropertyIndex/ElasticIndex). Actual cost-model behavior (a plan is/isn't offered, with
     * a real estimated entry count) is exercised end-to-end through the query engine in
     * LuceneNgIndexComparisonTest.
     */
    @Test
    public void getCostSimpleOverloadIsUnsupported() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgIndex index = new LuceneNgIndex(tracker, "/oak:index/test");

        Filter filter = mock(Filter.class);
        assertThrows(UnsupportedOperationException.class,
                () -> index.getCost(filter, InitialContentHelper.INITIAL_CONTENT));
    }
}
