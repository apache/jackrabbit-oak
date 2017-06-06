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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.MultiFacets;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FacetHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(FacetHelper.class);

    /**
     * IndexPaln Attribute name which refers to the name of the fields that should be used for facets.
     */
    public static final String ATTR_FACET_FIELDS = "oak.facet.fields";

    private FacetHelper() {
    }

    public static FacetsConfig getFacetsConfig(NodeBuilder definition) {
        return new NodeStateFacetsConfig(definition);
    }

    public static Facets getFacets(IndexSearcher searcher, Query query, TopDocs docs, QueryIndex.IndexPlan plan, boolean secure) throws IOException {
        Facets facets = null;
        @SuppressWarnings("unchecked")
        List<String> facetFields = (List<String>) plan.getAttribute(ATTR_FACET_FIELDS);
        if (facetFields != null && facetFields.size() > 0) {
            Map<String, Facets> facetsMap = new HashMap<String, Facets>();

            for (String facetField : facetFields) {
                FacetsCollector facetsCollector = new FacetsCollector();
                try {
                    DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(
                            searcher.getIndexReader(), FieldNames.createFacetFieldName(facetField));
                        FacetsCollector.search(searcher, query, 10, facetsCollector);
                    facetsMap.put(facetField, secure ?
                            new FilteredSortedSetDocValuesFacetCounts(state, facetsCollector, plan.getFilter(), docs) :
                            new SortedSetDocValuesFacetCounts(state, facetsCollector));

                } catch (IllegalArgumentException iae) {
                    LOGGER.warn("facets for {} not yet indexed", facetField);
                }
            }
            if (facetsMap.size() > 0) {
                facets = new MultiFacets(facetsMap);
            }

        }
        return facets;
    }


    public static String parseFacetField(String columnName) {
        return columnName.substring(QueryConstants.REP_FACET.length() + 1, columnName.length() - 1);
    }
}
