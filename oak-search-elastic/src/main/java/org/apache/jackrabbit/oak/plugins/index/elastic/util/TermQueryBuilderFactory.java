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
package org.apache.jackrabbit.oak.plugins.index.elastic.util;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.jetbrains.annotations.NotNull;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.json.JsonData;

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

    public static PrefixQueryBuilder newPrefixQuery(String field, @NotNull String value) {
        return prefixQuery(field, value);
    }

    public static Query newPrefixQuery2(String field, @NotNull String value) {
        return Query.of(q -> q.prefix(p -> p.field(field).value(value)));
    }

    public static WildcardQueryBuilder newWildcardQuery(String field, @NotNull String value) {
        return wildcardQuery(field, value);
    }

    public static Query newWildcardQuery2(String field, @NotNull String value) {
        return Query.of(q -> q.wildcard(w -> w.field(field).value(value)));
    }

    public static TermQueryBuilder newPathQuery(String path) {
        return termQuery(PATH, preparePath(path));
    }

    public static Query newPathQuery2(String path) {
        return Query.of(q -> q.term(t -> t.field(PATH).value(FieldValue.of(preparePath(path)))));
    }

    public static PrefixQueryBuilder newPrefixPathQuery(String path) {
        return prefixQuery(PATH, preparePath(path));
    }

    public static Query newPrefixPathQuery2(String path) {
        return Query.of(q -> q.prefix(p -> p.field(PATH).value(path)));
    }

    public static WildcardQueryBuilder newWildcardPathQuery(@NotNull String value) {
        return wildcardQuery(PATH, value);
    }

    public static Query newWildcardPathQuery2(@NotNull String value) {
        return Query.of(q -> q.wildcard(w -> w.field(PATH).value(value)));
    }

    public static TermQueryBuilder newAncestorQuery(String path) {
        return termQuery(FieldNames.ANCESTORS, preparePath(path));
    }

    public static Query newAncestorQuery2(String path) {
        return Query.of(q -> q.term(t -> t.field(FieldNames.ANCESTORS).value(FieldValue.of(preparePath(path)))));
    }

    public static TermQueryBuilder newDepthQuery(String path, FulltextIndexPlanner.PlanResult planResult) {
        int depth = PathUtils.getDepth(path) + planResult.getParentDepth() + 1;
        return QueryBuilders.termQuery(PATH_DEPTH, depth);
    }

    public static Query newDepthQuery2(String path, FulltextIndexPlanner.PlanResult planResult) {
        int depth = PathUtils.getDepth(path) + planResult.getParentDepth() + 1;
        return Query.of(q -> q.term(t -> t.field(PATH_DEPTH).value(FieldValue.of(depth))));
    }

    public static TermQueryBuilder newNodeTypeQuery(String type) {
        return termQuery(JCR_PRIMARYTYPE, type);
    }

    public static TermQueryBuilder newMixinTypeQuery(String type) {
        return termQuery(JCR_MIXINTYPES, type);
    }

    private static <R> RangeQueryBuilder newRangeQuery(String field, R first, R last, boolean firstIncluding,
            boolean lastIncluding) {
        return QueryBuilders.rangeQuery(field).from(first).to(last).includeLower(firstIncluding)
                .includeUpper(lastIncluding);
    }
    private static <R> Query newRangeQuery2(String field, R first, R last, boolean firstIncluding,
            boolean lastIncluding) {
        RangeQuery.Builder builder = new RangeQuery.Builder()
                .field(field);
        if(first!=null) {
            if(firstIncluding) {
                builder.lte(JsonData.of(first));
            }else {
                builder.lt(JsonData.of(first));
            }
        }
        if(last!=null) {
            if(lastIncluding) {
                builder.gte(JsonData.of(last));
            }else {
                builder.gt(JsonData.of(last));
            }
        }
        return Query.of(q->q
                .range(builder.build()));
    }

    private static <R> Query newInQuery(String field, List<R> values) {
        BoolQuery.Builder bq = new BoolQuery.Builder();
        for (R value : values) {
            bq.should(newRangeQuery2(field, value, value, true, true));
        }
        return Query.of(q->q
                .bool(bq.build()));
    }

    public static <R> Query newPropertyRestrictionQuery(String propertyName, Filter.PropertyRestriction pr,
            Function<PropertyValue, R> propToObj) {

        R first = pr.first != null ? propToObj.apply(pr.first) : null;
        R last = pr.last != null ? propToObj.apply(pr.last) : null;
        R not = pr.not != null ? propToObj.apply(pr.not) : null;
        if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding && pr.lastIncluding) {
            // [property]=[value]
            return Query.of(q->q
                    .term(t->t
                            .field(propertyName)
                            .value(FieldValue.of(first.toString()))));
        } else if (pr.first != null && pr.last != null) {
            return newRangeQuery2(propertyName, first, last, pr.firstIncluding, pr.lastIncluding);
        } else if (pr.first != null) {
            // '>' & '>=' use cases
            return newRangeQuery2(propertyName, first, null, pr.firstIncluding, true);
        } else if (pr.last != null) {
            // '<' & '<='
            return newRangeQuery2(propertyName, null, last, true, pr.lastIncluding);
        } else if (pr.list != null) {
            return newInQuery(propertyName, pr.list.stream().map(propToObj).collect(Collectors.toList()));
        } else if (pr.isNot && pr.not != null) {
            // MUST_NOT [property]=[value]
            return Query.of(q->q
                    .bool(b->b
                            .mustNot(mn->mn
                                    .term(t->t
                                            .field(propertyName)
                                            .value(FieldValue.of(not.toString()))))));
            // This helps with the NOT equal to condition for given property
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
}