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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.util;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public final class ElasticsearchAggregationBuilderUtil {

    private ElasticsearchAggregationBuilderUtil() {
    }

    public static List<TermsAggregationBuilder> getAggregators(QueryIndex.IndexPlan plan, int numberOfFacets) {
        List<TermsAggregationBuilder> termsAggregationBuilders = new LinkedList<>();
        Collection<Filter.PropertyRestriction> propertyRestrictions = plan.getFilter().getPropertyRestrictions();
        for (Filter.PropertyRestriction propertyRestriction : propertyRestrictions) {
            String name = propertyRestriction.propertyName;
            if (QueryConstants.REP_FACET.equals(name)) {
                String value = propertyRestriction.first.getValue(Type.STRING);
                String facetProp = FulltextIndex.parseFacetField(value);
                termsAggregationBuilders.add(AggregationBuilders.terms(facetProp).field(keywordFieldName(facetProp)).size(numberOfFacets));
            }
        }
        return termsAggregationBuilders;
    }

    private static String keywordFieldName(String propName) {
        return propName + "." + "keyword";
    }
}
