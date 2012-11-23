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

import org.apache.jackrabbit.oak.plugins.index.IndexDefinition;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A LuceneIndexProvider that return a LuceneIndex with a really low cost, so
 * that it tries to guarantee its usage in the queries
 * 
 */
public class LowCostLuceneIndexProvider extends LuceneIndexProvider {

    @Override
    protected LuceneIndex newLuceneIndex(IndexDefinition child) {
        return new LowCostLuceneIndex(child);
    }

    private static class LowCostLuceneIndex extends LuceneIndex {

        public LowCostLuceneIndex(IndexDefinition indexDefinition) {
            super(indexDefinition);
        }

        @Override
        public double getCost(Filter filter, NodeState root) {
            return 1e-3;
        }
    }
}
