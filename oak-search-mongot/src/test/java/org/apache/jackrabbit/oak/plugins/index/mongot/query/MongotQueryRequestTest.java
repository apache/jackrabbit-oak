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

import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.util.MongotIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotQueryRequestTest {

    @Test
    public void collectsFacetAndExcerptResultShapes() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty(QueryConstants.REP_EXCERPT, Operator.EQUAL,
                PropertyValues.newString("rep:excerpt()"));
        filter.restrictProperty(QueryConstants.REP_EXCERPT, Operator.EQUAL,
                PropertyValues.newString("rep:excerpt(jcr:title)"));
        filter.restrictProperty(QueryConstants.REP_FACET, Operator.EQUAL,
                PropertyValues.newString("rep:facet(status)"));

        MongotQueryRequest request = MongotQueryRequest.from(filter, definition());

        assertEquals(MongotQueryRequest.Mode.NORMAL, request.mode());
        assertEquals(List.of("rep:excerpt()", "rep:excerpt(jcr:title)"),
                request.excerptColumns());
        assertEquals(List.of("status"), request.facetFields());
        assertEquals(null, request.term());
    }

    @Test
    public void separatesSuggestionAndSpellcheckTerms() {
        FilterImpl suggestion = FilterImpl.newTestInstance();
        suggestion.restrictProperty("native*lucene", Operator.EQUAL,
                PropertyValues.newString("suggest?term=mon"));
        FilterImpl spellcheck = FilterImpl.newTestInstance();
        spellcheck.restrictProperty("native*lucene", Operator.EQUAL,
                PropertyValues.newString("spellcheck?term=mongdb"));

        MongotQueryRequest suggestRequest = MongotQueryRequest.from(suggestion, definition());
        MongotQueryRequest spellcheckRequest = MongotQueryRequest.from(spellcheck, definition());

        assertEquals(MongotQueryRequest.Mode.SUGGEST, suggestRequest.mode());
        assertEquals("mon", suggestRequest.term());
        assertEquals(MongotQueryRequest.Mode.SPELLCHECK, spellcheckRequest.mode());
        assertEquals("mongdb", spellcheckRequest.term());
    }

    @Test
    public void extractsSimilarityReferencePath() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("native*lucene", Operator.EQUAL,
                PropertyValues.newString(
                        "mlt?mlt.fl=:path&mlt.mindf=0&stream.body=/content/reference"));

        MongotQueryRequest request = MongotQueryRequest.from(filter, definition());

        assertEquals(MongotQueryRequest.Mode.NORMAL, request.mode());
        assertEquals("/content/reference", request.similarityPath());
    }

    @Test
    public void preservesNativeLuceneQueryString() {
        FilterImpl filter = FilterImpl.newTestInstance();
        filter.restrictProperty("native*lucene", Operator.EQUAL,
                PropertyValues.newString("title:foo -title:bar"));

        MongotQueryRequest request = MongotQueryRequest.from(filter, definition());

        assertEquals(MongotQueryRequest.Mode.NORMAL, request.mode());
        assertEquals("title:foo -title:bar", request.nativeQuery());
        assertEquals(null, request.similarityPath());
    }

    private static MongotIndexDefinition definition() {
        MongotIndexDefinitionBuilder builder = new MongotIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("jcr:title")
                .analyzed().useInSuggest().useInSpellcheck();
        return new MongotIndexDefinition(EmptyNodeState.EMPTY_NODE, builder.build(),
                "/oak:index/request");
    }
}
