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

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongotCoreQueryCompatibilityTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    private static MongotTestRepositoryBuilder.Fixture repository;

    @BeforeClass
    public static void createRepository() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        IndexDefinitionBuilder definition = builder.definition();
        definition.indexRule("nt:base").property("jcr:title")
                .propertyIndex().analyzed().nodeScopeIndex();
        IndexDefinitionBuilder.IndexRule rule = definition.indexRule("nt:unstructured");
        rule.property("jcr:title").propertyIndex().analyzed().nodeScopeIndex();
        rule.property("status").propertyIndex();
        rule.property("price").propertyIndex().ordered(PropertyType.TYPENAME_LONG);
        rule.property("sortKey").propertyIndex().ordered(PropertyType.TYPENAME_STRING);
        rule.property("tag").propertyIndex();
        rule.property("optional").propertyIndex().nullCheckEnabled();
        rule.property("present").propertyIndex().notNullCheckEnabled();
        definition.getBuilderTree().setProperty(MongotIndexDefinition.QUERY_FETCH_SIZES,
                List.of(3L, 10L), Type.LONGS);

        NodeBuilder site = builder.root().child("content").child("site")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME);
        add(site, "a", "MongoDB MongoDB native connector", "published", 10L, null, true, List.of("one"));
        add(site, "b", "MongoDB archived connector", "draft", 20L, "set", true, List.of("two"));
        add(site, "d", "MongoDB field notes", "published", 15L, "set", true, List.of("one", "two"));
        add(site, "c", "Other search backend", "published", 30L, "set", false, List.of("three"));
        add(builder.root().child("outside"), "x", "MongoDB outside", "published", 5L,
                "set", true, List.of("one"));
        String longSortPrefix = "x".repeat(8_191);
        addLongSortValue(site, "long-b", longSortPrefix + "b");
        addLongSortValue(site, "long-a", longSortPrefix + "a");
        for (int i = 0; i < 25; i++) {
            add(site, String.format("page-%02d", i), "batchmarker", "bulk", i,
                    "set", true, List.of("page"));
        }
        repository = builder.build();
    }

    @AfterClass
    public static void closeRepository() throws Exception {
        if (repository != null) {
            repository.close();
        }
    }

    @Test
    public void equalityRangeInLikeAndInequality() throws Exception {
        String prefix = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'mongodb') and isdescendantnode(s, '/content/site') and ";

        assertEquals(List.of("/content/site/a", "/content/site/d"),
                repository.paths(prefix + "s.[status] = 'published' order by s.[price]", "JCR-SQL2"));
        assertEquals(List.of("/content/site/d", "/content/site/b"),
                repository.paths(prefix + "s.[price] >= 12 and s.[price] < 21 order by s.[price]", "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/d", "/content/site/b"),
                repository.paths(prefix + "s.[status] in ('draft', 'published') order by s.[price]", "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/d"),
                repository.paths(prefix + "s.[status] like 'pub%' order by s.[price]", "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/d"),
                repository.paths(prefix + "s.[status] <> 'draft' order by s.[price]", "JCR-SQL2"));
    }

    @Test
    public void nullNotNullAndPathRestrictions() throws Exception {
        String base = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'mongodb') and ";
        assertEquals(List.of("/content/site/a"), repository.paths(base
                + "s.[optional] is null and s.[present] is not null", "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/d", "/content/site/b"),
                repository.paths(base + "ischildnode(s, '/content/site') order by s.[price]", "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/d", "/content/site/b"),
                repository.paths(base + "isdescendantnode(s, '/content/site') order by s.[price]", "JCR-SQL2"));
    }

    @Test
    public void exactSizeOfABareSearchCountsInsideMongotWithoutACountStage() throws Exception {
        String query = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'mongodb')";
        long searchMetaStages = stageCount("$searchMeta");
        long countStages = stageCount("$count");

        assertEquals(4, repository.query(query, "JCR-SQL2")
                .getSize(Result.SizePrecision.EXACT, Long.MAX_VALUE));

        // The count comes from the search metadata, computed inside mongot, rather
        // than from a re-executed pipeline that streams every hit into mongod.
        assertEquals(searchMetaStages + 1, stageCount("$searchMeta"));
        assertEquals(countStages, stageCount("$count"));
    }

    private static long stageCount(String stage) {
        Number count = mongo.getDatabase().runCommand(new Document("serverStatus", 1))
                .get("metrics", Document.class).get("aggStageCounters", Document.class)
                .get(stage, Number.class);
        return count == null ? 0 : count.longValue();
    }

    @Test
    public void orderingLimitOffsetAndResultSize() throws Exception {
        String query = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'mongodb') and isdescendantnode(s, '/content/site') "
                + "order by s.[price] desc";
        assertEquals(List.of("/content/site/b", "/content/site/d", "/content/site/a"),
                repository.paths(query, "JCR-SQL2"));
        assertEquals(List.of("/content/site/d"), repository.paths(query, "JCR-SQL2", 1, 1));
        assertEquals("Exact result size is not advertised by this POC cursor", -1,
                repository.query(query, "JCR-SQL2").getSize());
        assertEquals(3, repository.query(query, "JCR-SQL2")
                .getSize(Result.SizePrecision.EXACT, Long.MAX_VALUE));

        String plan = repository.query("explain " + query, "JCR-SQL2")
                .getRows().iterator().next().getValue("plan").getValue(Type.STRING);
        assertTrue(plan, plan.contains("mongot:"));
    }

    @Test
    public void orderedValuesRemainDistinctBeyondTheMongotTokenLimit() throws Exception {
        String query = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'long-sort-target') order by s.[sortKey]";

        assertEquals(List.of("/content/site/long-a", "/content/site/long-b"),
                repository.paths(query, "JCR-SQL2"));
    }

    @Test
    public void configuredSmallBatchReturnsACompleteLargeResultSet() throws Exception {
        String query = "select [jcr:path] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'batchmarker') order by s.[price]";
        List<String> expected = java.util.stream.IntStream.range(0, 25)
                .mapToObj(i -> String.format("/content/site/page-%02d", i)).toList();

        assertEquals(expected, repository.paths(query, "JCR-SQL2"));
        assertEquals(expected.subList(12, 17),
                repository.paths(query, "JCR-SQL2", 5, 12));
    }

    @Test
    public void propertyOnlyQueryUsesMongotIndexWithoutSearchMetadata() throws Exception {
        String query = "select [jcr:path] from [nt:unstructured] as s where "
                + "s.[status] = 'published' and isdescendantnode(s, '/content/site') "
                + "order by s.[price]";

        assertEquals(List.of("/content/site/a", "/content/site/d", "/content/site/c"),
                repository.paths(query, "JCR-SQL2"));

        String plan = repository.query("explain " + query, "JCR-SQL2")
                .getRows().iterator().next().getValue("plan").getValue(Type.STRING);
        assertTrue(plan, plan.contains("mongot:"));
    }

    @Test
    public void pathAndScoreOrderingComposeWithMongotResults() throws Exception {
        String propertyQuery = "select [jcr:path] from [nt:unstructured] as s where "
                + "s.[status] = 'published' and isdescendantnode(s, '/content/site')";
        assertEquals(List.of("/content/site/a", "/content/site/c", "/content/site/d"),
                repository.paths(propertyQuery + " order by [jcr:path]", "JCR-SQL2"));
        assertEquals(List.of("/content/site/d", "/content/site/c", "/content/site/a"),
                repository.paths(propertyQuery + " order by [jcr:path] desc", "JCR-SQL2"));

        String searchQuery = "select [jcr:path], [jcr:score] from [nt:unstructured] as s where "
                + "contains(s.[jcr:title], 'mongodb') and isdescendantnode(s, '/content/site')";
        List<String> descending = repository.paths(searchQuery + " order by [jcr:score] desc", "JCR-SQL2");
        List<String> ascending = repository.paths(searchQuery + " order by [jcr:score]", "JCR-SQL2");
        assertEquals(Set.copyOf(descending), Set.copyOf(ascending));
        assertMonotonic(scores(repository.query(searchQuery + " order by [jcr:score] desc", "JCR-SQL2")), false);
        assertMonotonic(scores(repository.query(searchQuery + " order by [jcr:score]", "JCR-SQL2")), true);
    }

    @Test
    public void fullTextBooleanExactPathAndUnionComposition() throws Exception {
        String select = "select [jcr:path] from [nt:unstructured] as s where ";
        String site = " and isdescendantnode(s, '/content/site') order by s.[price]";

        assertEquals(List.of("/content/site/a", "/content/site/b"), repository.paths(select
                + "contains(s.[jcr:title], 'mongodb') and contains(s.[jcr:title], 'connector')"
                + site, "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/b"), repository.paths(select
                + "contains(s.[jcr:title], 'native OR archived')" + site, "JCR-SQL2"));
        assertEquals(List.of("/content/site/a", "/content/site/d"), repository.paths(select
                + "contains(s.[jcr:title], 'mongodb -archived')" + site, "JCR-SQL2"));
        assertEquals(List.of("/content/site/a"), repository.paths(select
                + "contains(s.[jcr:title], 'mongodb') and issamenode(s, '/content/site/a')",
                "JCR-SQL2"));

        String union = select + "contains(s.[jcr:title], 'native')"
                + " union " + select + "contains(s.[jcr:title], 'archived')";
        assertEquals(Set.of("/content/site/a", "/content/site/b"),
                Set.copyOf(repository.paths(union, "JCR-SQL2")));
    }

    @Test
    public void nodeTypeMixinAndChildJoinComposition() throws Exception {
        String mixin = "select [jcr:path] from [mix:referenceable] as s where "
                + "contains(s.[jcr:title], 'mongodb')";
        assertEquals(List.of("/content/site/a"), repository.paths(mixin, "JCR-SQL2"));

        String join = "select child.[jcr:path] from [nt:unstructured] as child "
                + "inner join [nt:unstructured] as parent on ischildnode(child, parent) "
                + "where contains(child.[jcr:title], 'connector') "
                + "and issamenode(parent, '/content/site') order by child.[price]";
        assertEquals(List.of("/content/site/a", "/content/site/b"),
                repository.selectorPaths(join, "JCR-SQL2", "child"));
    }

    private static void add(NodeBuilder parent, String name, String title, String status, long price,
                            String optional, boolean present, List<String> tags) {
        NodeBuilder node = parent.child(name)
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", title)
                .setProperty("status", status)
                .setProperty("price", price)
                .setProperty("tag", tags, Type.STRINGS);
        if ("a".equals(name)) {
            node.setProperty(JCR_MIXINTYPES, List.of("mix:referenceable"), Type.NAMES)
                    .setProperty("jcr:uuid", "e87d4bd1-d8a6-4c4a-9ed0-590f4a31772d");
        }
        if (optional != null) {
            node.setProperty("optional", optional);
        }
        if (present) {
            node.setProperty("present", "yes");
        }
    }

    private static void addLongSortValue(NodeBuilder parent, String name, String sortKey) {
        parent.child(name)
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "long-sort-target")
                .setProperty("sortKey", sortKey);
    }

    private static List<Double> scores(Result result) {
        List<Double> scores = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            scores.add(row.getValue("jcr:score").getValue(Type.DOUBLE));
        }
        return scores;
    }

    private static void assertMonotonic(List<Double> values, boolean ascending) {
        for (int i = 1; i < values.size(); i++) {
            assertTrue(values.toString(), ascending
                    ? values.get(i - 1) <= values.get(i)
                    : values.get(i - 1) >= values.get(i));
        }
    }
}
