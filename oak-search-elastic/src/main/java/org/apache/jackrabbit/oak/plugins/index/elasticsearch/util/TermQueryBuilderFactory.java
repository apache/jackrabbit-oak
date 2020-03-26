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

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.search.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.index.search.FieldNames.PATH_DEPTH;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class TermQueryBuilderFactory {
    /**
     * Private constructor.
     */
    private TermQueryBuilderFactory() {
    }

    private static ExistsQueryBuilder newExistsQuery(String field) {
        return QueryBuilders.existsQuery(field);
    }

    private static TermQueryBuilder newFulltextQuery(String ft) {
        return termQuery(FieldNames.FULLTEXT, ft);
    }

    private static TermQueryBuilder newFulltextQuery(String ft, String field) {
        if (field == null || "*".equals(field)) {
            return newFulltextQuery(ft);
        }
        return termQuery(field, ft);
    }

    public static PrefixQueryBuilder newPrefixQuery(String field, @NotNull String value) {
        return prefixQuery(keywordFieldName(field), value);
    }

    public static WildcardQueryBuilder newWildcardQuery(String field, @NotNull String value) {
        return wildcardQuery(keywordFieldName(field), value);
    }

    public static TermQueryBuilder newPathQuery(String path) {
        return termQuery(PATH, preparePath(path));
    }

    public static PrefixQueryBuilder newPrefixPathQuery(String path) {
        return prefixQuery(PATH, preparePath(path));
    }

    public static WildcardQueryBuilder newWildcardPathQuery(@NotNull String value) {
        return wildcardQuery(PATH, value);
    }

    public static TermQueryBuilder newAncestorQuery(String path) {
        return termQuery(FieldNames.ANCESTORS, preparePath(path));
    }

    public static TermQueryBuilder newDepthQuery(String path, FulltextIndexPlanner.PlanResult planResult) {
        int depth = PathUtils.getDepth(path) + planResult.getParentDepth() + 1;
        return QueryBuilders.termQuery(PATH_DEPTH, depth);
    }

    public static TermQueryBuilder newNodeTypeQuery(String type) {
        return termQuery(keywordFieldName(JCR_PRIMARYTYPE), type);
    }

    public static TermQueryBuilder newMixinTypeQuery(String type) {
        return termQuery(keywordFieldName(JCR_MIXINTYPES), type);
    }

    public static TermQueryBuilder newNotNullPropQuery(String propName) {
        return termQuery(FieldNames.NOT_NULL_PROPS, propName);
    }

    public static TermQueryBuilder newNullPropQuery(String propName) {
        return termQuery(FieldNames.NULL_PROPS, propName);
    }

    private static <R> RangeQueryBuilder newRangeQuery(String field,
                                                       R first, R last, boolean firstIncluding, boolean lastIncluding) {
        return QueryBuilders.rangeQuery(field)
                .from(first).to(last)
                .includeLower(firstIncluding).includeUpper(lastIncluding);
    }

    private static <R> BoolQueryBuilder newInQuery(String field, List<R> values) {
        BoolQueryBuilder bq = boolQuery();
        for (R value : values) {
            bq.should(newRangeQuery(field, value, value, true, true));
        }
        return bq;
    }

    public static <R> QueryBuilder newPropertyRestrictionQuery(String propertyName, boolean isString,
                                                               Filter.PropertyRestriction pr,
                                                               Function<PropertyValue, R> propToObj) {
        if (isString) {
            propertyName = keywordFieldName(propertyName);
        }

        R first = pr.first != null ? propToObj.apply(pr.first) : null;
        R last = pr.last != null ? propToObj.apply(pr.last) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                && pr.lastIncluding) {
            // [property]=[value]
            return termQuery(propertyName, first);
        } else if (pr.first != null && pr.last != null) {
            return newRangeQuery(propertyName, first, last,
                    pr.firstIncluding, pr.lastIncluding);
        } else if (pr.first != null && pr.last == null) {
            // '>' & '>=' use cases
            return newRangeQuery(propertyName, first, null, pr.firstIncluding, true);
        } else if (pr.last != null && !pr.last.equals(pr.first)) {
            // '<' & '<='
            return newRangeQuery(propertyName, null, last, true, pr.lastIncluding);
        } else if (pr.list != null) {
            return newInQuery(propertyName, pr.list.stream()
                    .map(propToObj)
                    .collect(Collectors.toList()));
        } else if (pr.isNotNullRestriction()) {
            // not null. For date lower bound of zero can be used
            return newExistsQuery(propertyName);
        } else {
            return null;
        }
    }

    private static String preparePath(String path) {
        if (!"/".equals(path) && !path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }

    // As per https://www.elastic.co/blog/strings-are-dead-long-live-strings
    private static String keywordFieldName(String propName) {
        return propName + "." + "keyword";
    }
}