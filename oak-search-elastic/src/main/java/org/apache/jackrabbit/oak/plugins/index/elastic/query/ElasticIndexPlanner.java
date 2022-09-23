/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ElasticIndexPlanner extends FulltextIndexPlanner {

    public ElasticIndexPlanner(IndexNode indexNode, String indexPath, Filter filter, List<QueryIndex.OrderEntry> sortOrder) {
        super(indexNode, indexPath, filter, sortOrder);
    }

    @Override
    protected List<QueryIndex.OrderEntry> createSortOrder(IndexDefinition.IndexingRule rule) {
        if (sortOrder == null) {
            return Collections.emptyList();
        }

        List<QueryIndex.OrderEntry> orderEntries = new ArrayList<>(sortOrder.size());
        for (QueryIndex.OrderEntry o : sortOrder) {
            String propName = o.getPropertyName();
            PropertyDefinition pd = rule.getConfig(propName);
            if (pd != null
                    && o.getPropertyType() != null
                    && !o.getPropertyType().isArray()) {
                orderEntries.add(o); // can manage any order desc/asc
            } else if (JcrConstants.JCR_SCORE.equals(propName)) {
                // Supports jcr:score in both directions
                orderEntries.add(o);
            } else if (JcrConstants.JCR_PATH.equals(propName)) {
                // support for path ordering in both directions
                orderEntries.add(o);
            }
            for (PropertyDefinition functionIndex : rule.getFunctionRestrictions()) {
                if (functionIndex.ordered && o.getPropertyName().equals(functionIndex.function)) {
                    // can manage any order desc/asc
                    orderEntries.add(o);
                }
            }
        }

        //TODO Should we return order entries only when all order clauses are satisfied
        return orderEntries;
    }
}
