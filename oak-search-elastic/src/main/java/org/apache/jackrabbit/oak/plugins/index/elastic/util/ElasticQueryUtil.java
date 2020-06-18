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

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndexPlanner;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newAncestorQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newDepthQuery;
import static org.apache.jackrabbit.oak.plugins.index.elastic.util.TermQueryBuilderFactory.newPathQuery;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public final class ElasticQueryUtil {

    /**
     * Perform additional wraps on the list of queries to allow, for example, the NOT CONTAINS to
     * play properly when sent to lucene.
     *
     * @param qs the list of queries. Cannot be null.
     * @return the request facade
     */
    @NotNull
    public static QueryBuilder performAdditionalWraps(@NotNull List<QueryBuilder> qs) {
        if (qs.size() == 1) {
            // we don't need to worry about all-negatives in a bool query as
            // BoolQueryBuilder.adjustPureNegative is on by default anyway
            return qs.get(0);
        }
        BoolQueryBuilder bq = new BoolQueryBuilder();
        // TODO: while I've attempted to translate oak-lucene code to corresponding ES one but I am
        // unable to make sense of this code
        for (QueryBuilder q : qs) {
            boolean unwrapped = false;
            if (q instanceof BoolQueryBuilder) {
                unwrapped = unwrapMustNot((BoolQueryBuilder) q, bq);
            }

            if (!unwrapped) {
                bq.must(q);
            }
        }
        return bq;
    }

    /**
     * unwraps any NOT clauses from the provided boolean query into another boolean query.
     *
     * @param input  the query to be analysed for the existence of NOT clauses. Cannot be null.
     * @param output the query where the unwrapped NOTs will be saved into. Cannot be null.
     * @return true if there where at least one unwrapped NOT. false otherwise.
     */
    private static boolean unwrapMustNot(@NotNull BoolQueryBuilder input, @NotNull BoolQueryBuilder output) {
        boolean unwrapped = false;
        for (QueryBuilder mustNot : input.mustNot()) {
            output.mustNot(mustNot);
            unwrapped = true;
        }
        if (unwrapped) {
            // if we have unwrapped "must not" conditions,
            // then we need to unwrap "must" conditions as well
            for (QueryBuilder must : input.must()) {
                output.must(must);
            }
        }

        return unwrapped;
    }

    /**
     * Get path restrictions from plan and create elastic's queryBuilder objects with these restrictions.
     *
     * @param plan
     * @param planResult
     * @param filter
     * @return List of QueryBuilder with pathRestrictions
     */
    public static List<QueryBuilder> getPathRestrictionQuery(QueryIndex.IndexPlan plan,
                                                             FulltextIndexPlanner.PlanResult planResult, Filter filter) {
        final BiPredicate<Iterable<String>, String> any = (iterable, value) ->
                StreamSupport.stream(iterable.spliterator(), false).anyMatch(value::equals);
        List<QueryBuilder> qs = new LinkedList<>();

        String path = FulltextIndex.getPathRestriction(plan);
        switch (filter.getPathRestriction()) {
            case ALL_CHILDREN:
                if (!"/".equals(path)) {
                    qs.add(newAncestorQuery(path));
                }
                break;
            case DIRECT_CHILDREN:
                BoolQueryBuilder bq = boolQuery();
                bq.must(newAncestorQuery(path));
                bq.must(newDepthQuery(path, planResult));
                qs.add(bq);
                break;
            case EXACT:
                // For transformed paths, we can only add path restriction if absolute path to property can be
                // deduced
                if (planResult.isPathTransformed()) {
                    String parentPathSegment = planResult.getParentPathSegment();
                    if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                        qs.add(newPathQuery(path + parentPathSegment));
                    }
                } else {
                    qs.add(newPathQuery(path));
                }
                break;
            case PARENT:
                if (denotesRoot(path)) {
                    // there's no parent of the root node
                    // we add a path that can not possibly occur because there
                    // is no way to say "match no documents" in Lucene
                    qs.add(newPathQuery("///"));
                } else {
                    // For transformed paths, we can only add path restriction if absolute path to property can be
                    // deduced
                    if (planResult.isPathTransformed()) {
                        String parentPathSegment = planResult.getParentPathSegment();
                        if (!any.test(PathUtils.elements(parentPathSegment), "*")) {
                            qs.add(newPathQuery(getParentPath(path) + parentPathSegment));
                        }
                    } else {
                        qs.add(newPathQuery(getParentPath(path)));
                    }
                }
                break;
            case NO_RESTRICTION:
                break;
        }
        return qs;
    }

}
