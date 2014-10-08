/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.analysis.Analyzer;

/**
 * A LuceneIndexProvider that return a LuceneIndex with a really low cost, so
 * that it tries to guarantee its usage in the queries
 * 
 */
public class LowCostLuceneIndexProvider extends LuceneIndexProvider {

    @Override
    protected LuceneIndex newLuceneIndex() {
        return new LowCostLuceneIndex(tracker, analyzer, aggregator);
    }

    private static class LowCostLuceneIndex extends LuceneIndex {

        public LowCostLuceneIndex(IndexTracker tracker, Analyzer analyzer, NodeAggregator aggregator) {
            super(tracker, analyzer, aggregator);
        }

        @Override
        public List<IndexPlan> getPlans(Filter filter, List<OrderEntry> sortOrder, NodeState rootState) {
            return Collections.singletonList(planBuilder(filter)
                    .setCostPerExecution(1e-3)
                    .build());
        }
    }
}
