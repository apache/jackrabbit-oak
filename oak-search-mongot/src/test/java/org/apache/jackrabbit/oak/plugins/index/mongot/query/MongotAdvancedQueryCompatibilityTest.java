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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongotAdvancedQueryCompatibilityTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    private static MongotTestRepositoryBuilder.Fixture repository;

    @BeforeClass
    public static void createRepository() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        IndexDefinitionBuilder definition = builder.definition();
        configure(definition.indexRule("nt:base"));
        configure(definition.indexRule("nt:unstructured"));
        definition.aggregateRule("nt:unstructured", "jcr:content");
        definition.getBuilderTree().addChild(FulltextIndexConstants.ANALYZERS)
                .addChild(FulltextIndexConstants.ANL_DEFAULT)
                .setProperty(FulltextIndexConstants.ANL_CLASS,
                        "org.apache.lucene.analysis.en.EnglishAnalyzer");

        NodeBuilder content = builder.root().child("content");
        article(content, "article1", "MongoDB native connector field guide building", "Published",
                "Regional indexing summary", "aggregate payload walrus");
        article(content, "article2", "MongoDB connector archive", "Draft",
                "Global indexing summary", "other payload");
        repository = builder.build();
    }

    private static void configure(IndexDefinitionBuilder.IndexRule rule) {
        rule.property("jcr:title").propertyIndex().analyzed().nodeScopeIndex().useInExcerpt()
                .useInSuggest().useInSpellcheck();
        rule.property("status").propertyIndex().facets();
        rule.property("details/summary").propertyIndex().analyzed();
        rule.property("lowerStatus").function("lower([status])").propertyIndex();
        rule.property("text").propertyIndex().analyzed().nodeScopeIndex();
    }

    @AfterClass
    public static void closeRepository() throws Exception {
        if (repository != null) {
            repository.close();
        }
    }

    @Test
    public void phraseWildcardFuzzyAndBoostUseMongotSearch() throws Exception {
        String select = "select [jcr:path] from [nt:unstructured] as s where ";
        assertEquals(List.of("/content/article1"), repository.paths(select
                + "contains(s.[jcr:title], '\"native connector\"')", "JCR-SQL2"));
        assertEquals(List.of("/content/article1", "/content/article2"), repository.paths(select
                + "contains(s.[jcr:title], 'conn*') order by [jcr:path]", "JCR-SQL2"));
        assertEquals(List.of("/content/article1", "/content/article2"), repository.paths(select
                + "contains(s.[jcr:title], 'mongdb~1') order by [jcr:path]", "JCR-SQL2"));
        assertEquals(List.of("/content/article1", "/content/article2"), repository.paths(select
                + "contains(s.[jcr:title], 'native^5 OR archive') order by [jcr:score] desc", "JCR-SQL2"));

        assertMongotPlan(select + "contains(s.[jcr:title], 'mongdb~1')");
    }

    @Test
    public void oakBuiltInAnalyzerConfiguresMongotSearch() throws Exception {
        String query = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'build')";
        assertEquals(List.of("/content/article1"), repository.paths(query, "JCR-SQL2"));
        assertMongotPlan(query);
    }

    @Test
    public void relativePropertyAggregateAndFunctionIndexUseMongot() throws Exception {
        String select = "select [jcr:path] from [nt:unstructured] as s where ";
        assertEquals(List.of("/content/article1"), repository.paths(select
                + "contains(s.[details/summary], 'regional')", "JCR-SQL2"));
        assertEquals(List.of("/content/article1"), repository.paths(select
                + "contains(s.*, 'walrus') and issamenode(s, '/content/article1')", "JCR-SQL2"));
        String functionQuery = select + "lower(s.[status]) = 'published'";
        String functionPlan = plan(functionQuery);
        assertEquals(functionPlan, List.of("/content/article1"),
                repository.paths(functionQuery, "JCR-SQL2"));
        assertTrue(functionPlan, functionPlan.contains("mongot:"));
    }

    @Test
    public void facetsAndExcerptsUseMongotResults() throws Exception {
        String base = " from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'mongodb')";
        String query = "select [jcr:path], [rep:facet(status)], [rep:excerpt(.)]"
                + base + " order by [jcr:path]";
        assertMongotPlan(query);

        Result result = repository.query(query, "JCR-SQL2");
        List<ResultRow> rows = new ArrayList<>();
        result.getRows().forEach(rows::add);

        assertEquals(2, rows.size());
        assertEquals("{\"Draft\":1,\"Published\":1}",
                rows.get(0).getValue("rep:facet(status)").getValue(Type.STRING));
        assertTrue(rows.get(0).getValue("rep:excerpt(.)").getValue(Type.STRING),
                rows.get(0).getValue("rep:excerpt(.)").getValue(Type.STRING)
                        .contains("<strong>MongoDB</strong>"));
        assertTrue(rows.get(1).getValue("rep:excerpt(.)").getValue(Type.STRING),
                rows.get(1).getValue("rep:excerpt(.)").getValue(Type.STRING)
                        .contains("<strong>MongoDB</strong>"));
    }

    @Test
    public void suggestionsAndSpellcheckUseMongotVirtualRows() throws Exception {
        String suggest = "select [rep:suggest()] from [nt:base] where suggest('mongo')";
        String spellcheck = "select [rep:spellcheck()] from [nt:base] where spellcheck('mongdb')";

        assertMongotPlan(suggest);
        assertMongotPlan(spellcheck);
        assertEquals(Set.of(
                        "MongoDB native connector field guide building",
                        "MongoDB connector archive"),
                Set.copyOf(values(repository.query(suggest, "JCR-SQL2"), "rep:suggest()")));
        assertEquals(List.of("mongodb"),
                values(repository.query(spellcheck, "JCR-SQL2"), "rep:spellcheck()"));
    }

    @Test
    public void unsupportedNativeExpressionIsDeclinedBeforeMongotSelection() throws Exception {
        String nativePlan = plan(
                "select [jcr:path] from [nt:base] where native('mongodb', 'text:mongodb')");

        assertFalse(nativePlan, nativePlan.contains("mongot:"));
    }

    private static void article(NodeBuilder content, String name, String title, String status,
                                String summary, String aggregateText) {
        NodeBuilder article = content.child(name)
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", title)
                .setProperty("status", status);
        article.child("details")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("summary", summary);
        article.child("jcr:content")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("text", aggregateText);
    }

    private static void assertMongotPlan(String query) throws Exception {
        String plan = plan(query);
        assertTrue(plan, plan.contains("mongot:"));
    }

    private static String plan(String query) throws Exception {
        return repository.query("explain " + query, "JCR-SQL2")
                .getRows().iterator().next().getValue("plan").getValue(Type.STRING);
    }

    private static List<String> values(Result result, String column) {
        List<String> values = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            values.add(row.getValue(column).getValue(Type.STRING));
        }
        return values;
    }
}
