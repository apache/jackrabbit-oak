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
package org.apache.jackrabbit.oak.plugins.index.aggregate;

import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.FulltextQueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A virtual full-text that can aggregate nodes based on aggregate definitions.
 * Internally, it uses another full-text index.
 */
public class AggregateIndex implements FulltextQueryIndex {
    
    private final FulltextQueryIndex baseIndex;
    
    public AggregateIndex(FulltextQueryIndex baseIndex) {
        this.baseIndex = baseIndex;
    }

    @Override
    public double getCost(Filter filter, NodeState rootState) {
        // TODO dummy implementation
        if (baseIndex == null) {
            return Double.POSITIVE_INFINITY;
        }
        return baseIndex.getCost(filter, rootState);
    }

    @Override
    public Cursor query(Filter filter, NodeState rootState) {
        // TODO dummy implementation
        return baseIndex.query(filter, rootState);
    }

    @Override
    public String getPlan(Filter filter, NodeState rootState) {
        // TODO dummy implementation
        if (baseIndex == null) {
            return "no plan";
        }
        return "aggregate " + baseIndex.getPlan(filter, rootState);
    }

    @Override
    public String getIndexName() {
        // TODO dummy implementation
        if (baseIndex == null) {
            return "aggregat";
        }
        return "aggregate." + baseIndex.getIndexName();
    }

}
