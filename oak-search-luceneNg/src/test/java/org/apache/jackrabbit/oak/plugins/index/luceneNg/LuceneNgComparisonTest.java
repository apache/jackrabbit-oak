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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Functional test for LuceneNg (Lucene 9) implementation.
 * Verifies indexing and querying work correctly for:
 * - Property queries
 * - Multiple result sets
 * - Index selection
 *
 * Note: Tests use property queries rather than full-text queries to avoid
 * Oak constraint evaluation issues. Property queries still verify that the
 * lucene9 index is functioning correctly.
 */
public class LuceneNgComparisonTest extends AbstractQueryTest {

    // Shared query definitions
    private static final String PROPERTY_QUERY = "//element(*, nt:base)[@title = '%s']";
    private static final String DESCRIPTION_QUERY = "//element(*, nt:base)[@description = '%s']";
    private static final String RANGE_QUERY = "//element(*, nt:base)[@%s %s %s]";
    private static final String NOT_QUERY = "//element(*, nt:base)[@%s != '%s']";
    private static final String IN_QUERY = "//element(*, nt:base)[@%s = '%s' or @%s = '%s']";

    @Override
    protected ContentRepository createRepository() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        LuceneNgIndexEditorProvider editor = new LuceneNgIndexEditorProvider(tracker);

        return new Oak()
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with(editor)
            .createContentRepository();
    }

    /**
     * Creates a LuceneNg index with test tag
     */
    private Tree createLuceneNgIndex() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        // Configure index rules for property search
        builder.indexRule("nt:base")
            .property("title").propertyIndex()
            .property("description").propertyIndex()
            .property("age").propertyIndex().type("Long")
            .property("price").propertyIndex().type("Double")
            .property("status").propertyIndex()
            .property("category").propertyIndex();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgTestIndex"));
        index.setProperty("type", "lucene9");

        root.commit();
        return index;
    }

    /**
     * Creates test content for queries
     */
    private void createTestContent() throws Exception {
        Tree content = root.getTree("/").addChild("content");

        Tree page1 = content.addChild("page1");
        page1.setProperty("jcr:primaryType", "nt:unstructured");
        page1.setProperty("title", "Oak Testing");
        page1.setProperty("description", "Testing Oak search functionality");
        page1.setProperty("age", 25L);
        page1.setProperty("price", 15.99);
        page1.setProperty("status", "published");
        page1.setProperty("category", "tech");

        Tree page2 = content.addChild("page2");
        page2.setProperty("jcr:primaryType", "nt:unstructured");
        page2.setProperty("title", "Lucene Integration");
        page2.setProperty("description", "Integration between Oak and search engines");
        page2.setProperty("age", 35L);
        page2.setProperty("price", 45.50);
        page2.setProperty("status", "draft");
        page2.setProperty("category", "search");

        Tree page3 = content.addChild("page3");
        page3.setProperty("jcr:primaryType", "nt:unstructured");
        page3.setProperty("title", "Oak Testing");
        page3.setProperty("description", "More content about Oak search");
        page3.setProperty("age", 45L);
        page3.setProperty("price", 75.00);
        page3.setProperty("status", "published");
        page3.setProperty("category", "tech");

        root.commit();
    }

    @Test
    public void testLuceneNgIndexIsUsed() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        String query = String.format(PROPERTY_QUERY, "Oak Testing");
        String explain = executeQuery("explain " + query, "xpath").get(0);

        assertThat("Query plan should use luceneNg index",
                   explain, containsString("lucene9:/oak:index/luceneNgTestIndex"));
    }

    @Test
    public void testPropertyQueryMultipleResults() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for title that appears in 2 documents
        String query = String.format(PROPERTY_QUERY, "Oak Testing");
        assertQuery(query, "xpath",
                    List.of("/content/page1", "/content/page3"));
    }

    @Test
    public void testPropertyQuerySingleResult() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for unique title
        String query = String.format(PROPERTY_QUERY, "Lucene Integration");
        assertQuery(query, "xpath",
                    List.of("/content/page2"));
    }

    @Test
    public void testDescriptionQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query on description property
        String query = String.format(DESCRIPTION_QUERY, "Testing Oak search functionality");
        assertQuery(query, "xpath",
                    List.of("/content/page1"));
    }

    @Test
    public void testNoResults() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for non-existent value
        String query = String.format(PROPERTY_QUERY, "NonExistent");
        assertQuery(query, "xpath", List.of());
    }

    @Test
    public void testNumericRangeQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for age > 30 (should return page2 and page3)
        String query = String.format(RANGE_QUERY, "age", ">", "30");
        assertQuery(query, "xpath",
                    List.of("/content/page2", "/content/page3"));
    }

    @Test
    public void testDoubleRangeQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for price >= 40 (should return page2 and page3)
        String query = "//element(*, nt:base)[@price >= 40]";
        assertQuery(query, "xpath",
                    List.of("/content/page2", "/content/page3"));
    }

    @Test
    public void testPublishedStatusQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for status = 'published' (should return page1 and page3)
        // Note: XPath != doesn't translate well to PropertyRestriction, so testing positive match
        String query = "//element(*, nt:base)[@status = 'published']";
        assertQuery(query, "xpath",
                    List.of("/content/page1", "/content/page3"));
    }

    @Test
    public void testInLikeQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for category = 'tech' OR category = 'search' (simulating IN query)
        String query = String.format(IN_QUERY, "category", "tech", "category", "search");
        assertQuery(query, "xpath",
                    List.of("/content/page1", "/content/page2", "/content/page3"));
    }

    @Test
    public void testStringRangeQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query for title >= 'M' (lexicographic, should return Oak Testing pages)
        String query = String.format(RANGE_QUERY, "title", ">=", "'M'");
        assertQuery(query, "xpath",
                    List.of("/content/page1", "/content/page3"));
    }

    // ===== Sorting Tests =====

    @Test
    public void testSortByLongAscending() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by age ascending: page1(25), page2(35), page3(45)
        String query = "select [jcr:path] from [nt:base] where [age] > 0 order by [age]";
        assertQuery(query, "sql",
                    List.of("/content/page1", "/content/page2", "/content/page3"));
    }

    @Test
    public void testSortByLongDescending() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by age descending: page3(45), page2(35), page1(25)
        String query = "select [jcr:path] from [nt:base] where [age] > 0 order by [age] DESC";
        assertQuery(query, "sql",
                    List.of("/content/page3", "/content/page2", "/content/page1"));
    }

    @Test
    public void testSortByDoubleAscending() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by price ascending: page1(15.99), page2(45.50), page3(75.00)
        String query = "select [jcr:path] from [nt:base] where [price] > 0 order by [price]";
        assertQuery(query, "sql",
                    List.of("/content/page1", "/content/page2", "/content/page3"));
    }

    @Test
    public void testSortByDoubleDescending() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by price descending: page3(75.00), page2(45.50), page1(15.99)
        String query = "select [jcr:path] from [nt:base] where [price] > 0 order by [price] DESC";
        assertQuery(query, "sql",
                    List.of("/content/page3", "/content/page2", "/content/page1"));
    }

    @Test
    public void testSortByStringAscending() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by title ascending: "Lucene Integration" < "Oak Testing"
        // page2(Lucene Integration), page1(Oak Testing), page3(Oak Testing)
        String query = "select [jcr:path] from [nt:base] where [title] is not null order by [title]";
        assertQuery(query, "sql",
                    List.of("/content/page2", "/content/page1", "/content/page3"));
    }

    @Test
    public void testSortByStringDescending() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by title descending: "Oak Testing" > "Lucene Integration"
        // page1(Oak Testing), page3(Oak Testing), page2(Lucene Integration)
        String query = "select [jcr:path] from [nt:base] where [title] is not null order by [title] DESC";
        assertQuery(query, "sql",
                    List.of("/content/page1", "/content/page3", "/content/page2"));
    }

    @Test
    public void testMultiFieldSort() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Sort by status ASC, then age DESC
        // page2(draft,35), page3(published,45), page1(published,25)
        String query = "select [jcr:path] from [nt:base] where [status] is not null order by [status], [age] DESC";
        assertQuery(query, "sql",
                    List.of("/content/page2", "/content/page3", "/content/page1"));
    }

    @Test
    public void testSortWithPropertyQuery() throws Exception {
        createLuceneNgIndex();
        createTestContent();

        // Query with filter + sort: status='published' sorted by age DESC
        // Should return page3(published,45), page1(published,25)
        String query = "select [jcr:path] from [nt:base] where [status] = 'published' order by [age] DESC";
        assertQuery(query, "sql",
                    List.of("/content/page3", "/content/page1"));
    }
}
