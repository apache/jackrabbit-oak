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
package org.apache.jackrabbit.oak.query.plan;

import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;

/**
 * An execution plan for one selector in a query. The conditions of the given
 * selectors are compiled into a filter, and the execution plan for the selector
 * is to use a certain query index, which will result in an estimated cost to
 * use that index to retrieve nodes for this index.
 */
public class SelectorExecutionPlan implements ExecutionPlan {
    
    private final SelectorImpl selector;
    private final double estimatedCost;
    private final QueryIndex index;
    private final IndexPlan plan;

    public SelectorExecutionPlan(SelectorImpl selector, QueryIndex index, IndexPlan plan, double estimatedCost) {
        this.selector = selector;
        this.index = index;
        this.estimatedCost = estimatedCost;
        this.plan = plan;
    }
    
    @Override
    public double getEstimatedCost() {
        return estimatedCost;
    }

    public SelectorImpl getSelector() {
        return selector;
    }

    public QueryIndex getIndex() {
        return index;
    }
    
    public IndexPlan getIndexPlan() {
        return plan;
    }

}
