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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.MoreLikeThisHelperUtil;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;

final class MongotQueryRequest {

    private static final String SUGGEST_PREFIX = "suggest?term=";
    private static final String SPELLCHECK_PREFIX = "spellcheck?term=";
    private static final String MORE_LIKE_THIS_PREFIX = "mlt?";

    enum Mode {
        NORMAL,
        SUGGEST,
        SPELLCHECK
    }

    private final Mode mode;
    private final String term;
    private final String similarityPath;
    private final String nativeQuery;
    private final List<String> excerptColumns;
    private final List<String> facetFields;

    private MongotQueryRequest(Mode mode, String term, String similarityPath, String nativeQuery,
                              List<String> excerptColumns, List<String> facetFields) {
        this.mode = mode;
        this.term = term;
        this.similarityPath = similarityPath;
        this.nativeQuery = nativeQuery;
        this.excerptColumns = List.copyOf(excerptColumns);
        this.facetFields = List.copyOf(facetFields);
    }

    static MongotQueryRequest from(Filter filter, MongotIndexDefinition definition) {
        List<String> excerptColumns = new ArrayList<>();
        List<String> facetFields = new ArrayList<>();
        for (Filter.PropertyRestriction restriction : filter.getPropertyRestrictions()) {
            if (QueryConstants.REP_EXCERPT.equals(restriction.propertyName)) {
                excerptColumns.add(restriction.first.getValue(Type.STRING));
            } else if (QueryConstants.REP_FACET.equals(restriction.propertyName)) {
                facetFields.add(FulltextIndex.parseFacetField(
                        restriction.first.getValue(Type.STRING)));
            }
        }

        Filter.PropertyRestriction nativeRestriction =
                filter.getPropertyRestriction(definition.getFunctionName());
        if (nativeRestriction == null) {
            return new MongotQueryRequest(Mode.NORMAL, null, null, null,
                    excerptColumns, facetFields);
        }

        String nativeQuery = nativeRestriction.first.getValue(Type.STRING);
        if (nativeQuery.startsWith(SUGGEST_PREFIX)) {
            return new MongotQueryRequest(Mode.SUGGEST,
                    term(nativeQuery, SUGGEST_PREFIX), null, null, excerptColumns, facetFields);
        }
        if (nativeQuery.startsWith(SPELLCHECK_PREFIX)) {
            return new MongotQueryRequest(Mode.SPELLCHECK,
                    term(nativeQuery, SPELLCHECK_PREFIX), null, null, excerptColumns, facetFields);
        }
        if (nativeQuery.startsWith(MORE_LIKE_THIS_PREFIX)) {
            String path = MoreLikeThisHelperUtil.getParamMapFromMltQuery(
                    nativeQuery.substring(MORE_LIKE_THIS_PREFIX.length()))
                    .get(MoreLikeThisHelperUtil.MLT_STREAM_BODY);
            if (path == null || path.isBlank()) {
                throw new IllegalArgumentException(
                        "Mongot similarity query is missing stream.body");
            }
            return new MongotQueryRequest(Mode.NORMAL, null, path, null,
                    excerptColumns, facetFields);
        }
        if (nativeQuery.isBlank()) {
            throw new IllegalArgumentException("Mongot native query is empty");
        }
        return new MongotQueryRequest(Mode.NORMAL, null, null, nativeQuery,
                excerptColumns, facetFields);
    }

    private static String term(String query, String prefix) {
        String term = query.substring(prefix.length());
        if (term.isBlank()) {
            throw new IllegalArgumentException("search term is empty");
        }
        return term;
    }

    Mode mode() {
        return mode;
    }

    String term() {
        return term;
    }

    String similarityPath() {
        return similarityPath;
    }

    String nativeQuery() {
        return nativeQuery;
    }

    List<String> excerptColumns() {
        return excerptColumns;
    }

    List<String> facetFields() {
        return facetFields;
    }
}
