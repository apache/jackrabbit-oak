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
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.mongot.index.MongoFieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotResultAdapterTest {

    @Test
    public void convertsSearchHighlightFragmentsToOakExcerpts() {
        String titlePath = MongoFieldNames.ANALYZED + "."
                + MongoFieldNames.encodeProperty("jcr:title");
        Document result = new Document(MongotResultAdapter.HIGHLIGHTS, List.of(
                highlight(MongoFieldNames.FULLTEXT,
                        text("MongoDB "), hit("connector"), text(" guide")),
                highlight(titlePath, hit("MongoDB"), text(" field guide"))));

        Map<String, String> excerpts = MongotResultAdapter.excerpts(result,
                List.of("rep:excerpt()", "rep:excerpt(jcr:title)", "rep:excerpt(missing)"));

        assertEquals(Map.of(
                "rep:excerpt()", "MongoDB <strong>connector</strong> guide",
                "rep:excerpt(jcr:title)", "<strong>MongoDB</strong> field guide"), excerpts);
    }

    @Test
    public void countsEachFacetLabelOncePerDocumentAndOrdersDeterministically() throws Exception {
        String status = MongoFieldNames.encodeProperty("status");
        List<Document> results = List.of(
                resultWithFacets(status, "Published", "Featured", "Published"),
                resultWithFacets(status, "Published"),
                resultWithFacets(status, "Draft"));

        FulltextIndex.FacetProvider provider = MongotResultAdapter.facets(results);

        assertFacets(provider.getFacets(2, "rep:facet(status)"),
                List.of("Published", "Draft"), List.of(2, 1));
        assertFacets(provider.getFacets(10, "rep:facet(status)"),
                List.of("Published", "Draft", "Featured"), List.of(2, 1, 1));
    }

    @Test
    public void returnsDistinctMatchingSuggestionValuesByBestScore() {
        List<Document> results = List.of(
                new Document(MongoFieldNames.SUGGEST,
                        List.of("MongoDB connector guide", "unrelated value")).append("_score", 3.0d),
                new Document(MongoFieldNames.SUGGEST,
                        List.of("MongoDB connector guide", "MongoDB archive")).append("_score", 5.0d));

        List<FulltextIndex.FulltextResultRow> rows = MongotResultAdapter.suggestions(results, "mongo");

        assertEquals(List.of("MongoDB archive", "MongoDB connector guide"),
                rows.stream().map(row -> row.suggestion).toList());
        assertEquals(5.0d, rows.get(1).score, 0.0d);
    }

    @Test
    public void derivesSpellCorrectionFromFuzzyMatchedValues() {
        List<Document> results = List.of(
                new Document(MongoFieldNames.SPELLCHECK,
                        List.of("MongoDB native connector", "unrelated")),
                new Document(MongoFieldNames.SPELLCHECK,
                        List.of("MongoDB archive")));

        List<FulltextIndex.FulltextResultRow> rows = MongotResultAdapter.spellchecks(results, "mongdb");

        assertEquals(List.of("mongodb"), rows.stream().map(row -> row.suggestion).toList());
    }

    @Test
    public void correctsEachTokenInAMisspelledPhrase() {
        List<Document> results = List.of(
                new Document(MongoFieldNames.SPELLCHECK,
                        List.of("voting in ontario", "visiting ontario")),
                new Document(MongoFieldNames.SPELLCHECK,
                        List.of("voting in ontario")));

        List<FulltextIndex.FulltextResultRow> rows = MongotResultAdapter.spellchecks(
                results, "votin in ontari");

        assertEquals(List.of("voting in ontario"),
                rows.stream().map(row -> row.suggestion).toList());
    }

    private static Document resultWithFacets(String property, String... values) {
        return new Document(MongoFieldNames.FACET,
                new Document(property, List.of(values)));
    }

    private static void assertFacets(List<FulltextIndex.Facet> actual,
                                     List<String> labels, List<Integer> counts) {
        assertEquals(labels, actual.stream().map(FulltextIndex.Facet::getLabel).toList());
        assertEquals(counts, actual.stream().map(FulltextIndex.Facet::getCount).toList());
    }

    private static Document highlight(String path, Document... texts) {
        return new Document("path", path).append("texts", List.of(texts));
    }

    private static Document hit(String value) {
        return new Document("type", "hit").append("value", value);
    }

    private static Document text(String value) {
        return new Document("type", "text").append("value", value);
    }
}
