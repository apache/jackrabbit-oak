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

    public static Query newPrefixQuery(String field, @NotNull String value) {
        return Query.of(q -> q
                .prefix(p -> p
                        .field(field)
                        .value(value)));
    }

    public static Query newWildcardQuery(String field, @NotNull String value) {
        return Query.of(q -> q
                .wildcard(w -> w
                        .field(field)
                        .value(value)));
    }

    public static Query newPathQuery(String path) {
        return Query.of(q -> q
                .term(t -> t
                        .field(PATH)
                        .value(v->v
                                .stringValue(preparePath(path)))));
    }

    public static Query newPrefixPathQuery(String path) {
        return Query.of(q -> q
                .prefix(p -> p
                        .field(PATH)
                        .value(path)));
    }

    public static Query newWildcardPathQuery(@NotNull String value) {
        return Query.of(q -> q
                .wildcard(w -> w
                        .field(PATH)
                        .value(value)));
    }

    public static Query newAncestorQuery(String path) {
        return Query.of(q -> q
                .term(t -> t
                        .field(FieldNames.ANCESTORS)
                        .value(v->v
                                .stringValue(preparePath(path)))));
    }

    public static Query newDepthQuery(String path, FulltextIndexPlanner.PlanResult planResult) {
        int depth = PathUtils.getDepth(path) + planResult.getParentDepth() + 1;
        return Query.of(q -> q
                .term(t -> t
                        .field(PATH_DEPTH)
                        .value(v->v
                                .longValue(depth))));
    }

    public static Query newNodeTypeQuery(String type) {
        return Query.of(q -> q
                .term(t -> t
                        .field(JCR_PRIMARYTYPE)
                        .value(v->v
                                .stringValue(type))));
    }
    
    public static Query newMixinTypeQuery(String type) {
        return Query.of(q -> q
                .term(t -> t
                        .field(JCR_MIXINTYPES)
                        .value(v->v
                                .stringValue(type))));
    }
    
    //TODO Angela check newRangeQuery is doing what is expected. See original code bellow:
//    private static <R> RangeQueryBuilder newRangeQuery(String field, R first, R last, boolean firstIncluding,
//            boolean lastIncluding) {
//        return QueryBuilders.rangeQuery(field).from(first).to(last).includeLower(firstIncluding)
//                .includeUpper(lastIncluding);
//    }
    
    private static <R> Query newRangeQuery(String field, R first, R last, boolean firstIncluding,
            boolean lastIncluding) {
        RangeQuery.Builder rqBuilder = new RangeQuery.Builder()
                .field(field);
        if(first!=null) {
            if(firstIncluding) {
                rqBuilder.lte(JsonData.of(first));
            }else {
                rqBuilder.lt(JsonData.of(first));
            }
        }
        if(last!=null) {
            if(lastIncluding) {
                rqBuilder.gte(JsonData.of(last));
            }else {
                rqBuilder.gt(JsonData.of(last));
            }
        }
        return Query.of(q->q
                .range(rqBuilder.build()));
    }

    private static <R> Query newInQuery(String field, List<R> values) {
        BoolQuery.Builder bqBuilder = new BoolQuery.Builder();
        for (R value : values) {
            bqBuilder.should(newRangeQuery(field, value, value, true, true));
        }
        return Query.of(q->q
                .bool(bqBuilder.build()));
    }

    // TODO Angela check newPropertyREstrictionQuery. It may fail because of the use of .toString.
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
            return newRangeQuery(propertyName, first, last, pr.firstIncluding, pr.lastIncluding);
        } else if (pr.first != null) {
            // '>' & '>=' use cases
            return newRangeQuery(propertyName, first, null, pr.firstIncluding, true);
        } else if (pr.last != null) {
            // '<' & '<='
            return newRangeQuery(propertyName, null, last, true, pr.lastIncluding);
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