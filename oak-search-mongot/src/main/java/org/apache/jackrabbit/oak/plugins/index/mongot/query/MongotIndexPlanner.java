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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexNode;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;

public final class MongotIndexPlanner extends FulltextIndexPlanner {

    private final Filter filter;
    private final MongotIndexDefinition definition;

    public MongotIndexPlanner(IndexNode indexNode, String indexPath, Filter filter,
                             List<QueryIndex.OrderEntry> sortOrder) {
        super(indexNode, indexPath, filter, sortOrder);
        this.filter = filter;
        this.definition = (MongotIndexDefinition) indexNode.getDefinition();
    }

    @Override
    public QueryIndex.IndexPlan getPlan() {
        MongotQueryRequest request;
        try {
            request = MongotQueryRequest.from(filter, definition);
        } catch (IllegalArgumentException e) {
            return null;
        }
        if (!request.excerptColumns().isEmpty() && filter.getFullTextConstraint() == null) {
            return null;
        }
        if (filter.getFullTextConstraint() != null
                && !MongotQueryTranslator.translateFullText(filter.getFullTextConstraint()).isSupported()) {
            return null;
        }
        if (!MongotQueryTranslator.translateFilter(filter).isSupported()) {
            return null;
        }
        return super.getPlan();
    }

    @Override
    protected List<QueryIndex.OrderEntry> createSortOrder(
            IndexDefinition.IndexingRule rule) {
        List<QueryIndex.OrderEntry> supported = super.createSortOrder(rule);
        if (sortOrder == null) {
            return supported;
        }
        List<QueryIndex.OrderEntry> result = new java.util.ArrayList<>();
        for (QueryIndex.OrderEntry requested : sortOrder) {
            boolean explicitlyOrderable = supported.stream().anyMatch(candidate ->
                    candidate.getPropertyName().equals(requested.getPropertyName()));
            if (explicitlyOrderable || isMongotSortable(rule, requested)) {
                result.add(requested);
            } else {
                break;
            }
        }
        return result;
    }

    private static boolean isMongotSortable(IndexDefinition.IndexingRule rule,
                                           QueryIndex.OrderEntry entry) {
        return !entry.getPropertyName().startsWith(FieldNames.FUNCTION_PREFIX)
                && entry.getPropertyType() != null
                && !entry.getPropertyType().isArray()
                && rule.getConfig(entry.getPropertyName()) != null
                && rule.getConfig(entry.getPropertyName()).propertyIndexEnabled();
    }
}
