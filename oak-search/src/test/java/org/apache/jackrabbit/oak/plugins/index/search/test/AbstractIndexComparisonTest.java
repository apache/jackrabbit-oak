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
package org.apache.jackrabbit.oak.plugins.index.search.test;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.List;

/**
 * Abstract base class defining a shared suite of search index test scenarios.
 *
 * <p>Concrete subclasses supply the repository wiring and index creation for a specific
 * search backend (e.g. legacy Lucene, Lucene 9). Running the same scenarios against each
 * backend verifies behavioural parity across implementations.
 *
 * <p>Test data uses fully unique values for all sort-key fields so that ordering assertions
 * are deterministic regardless of the underlying Lucene version or document-id tiebreaking.
 *
 * <h3>Test data</h3>
 * <pre>
 *  page1: title="Oak Testing",       age=25, price=15.99, status=published, category=tech
 *  page2: title="Lucene Integration", age=35, price=45.50, status=draft,    category=search
 *  page3: title="Query DSL",         age=45, price=75.00, status=published, category=tech
 * </pre>
 */
public abstract class AbstractIndexComparisonTest extends AbstractQueryTest {

    /**
     * Creates the search index in the repository.
     * Implementations use their engine-specific index type and builder.
     */
    protected abstract void createSearchIndex() throws Exception;

    /** Suppress the default "unknown"-type index created by AbstractQueryTest.before(). */
    @Override
    protected void createTestIndexNode() throws Exception {
        // no-op: each test creates its index explicitly via createSearchIndex()
    }

    protected void createTestContent() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        addPage(content.addChild("page1"), "Oak Testing",        "Testing Oak search functionality",            25L, 15.99, "published", "tech");
        addPage(content.addChild("page2"), "Lucene Integration", "Integration between Oak and search engines",  35L, 45.50, "draft",     "search");
        addPage(content.addChild("page3"), "Query DSL",          "More content about Oak search",               45L, 75.00, "published", "tech");
        root.commit();
    }

    private static void addPage(Tree page, String title, String description,
                                long age, double price, String status, String category) {
        page.setProperty("title", title);
        page.setProperty("description", description);
        page.setProperty("age", age);
        page.setProperty("price", price);
        page.setProperty("status", status);
        page.setProperty("category", category);
    }

    // ===== Property equality queries =====

    @Test
    public void testPropertyQuerySingleResult() throws Exception {
        createSearchIndex();
        createTestContent();
        assertQuery("//element(*, nt:base)[@title = 'Lucene Integration']", "xpath",
                List.of("/content/page2"));
    }

    @Test
    public void testPropertyQueryMultipleResults() throws Exception {
        createSearchIndex();
        createTestContent();
        // category=tech matches page1 and page3
        assertQuery("//element(*, nt:base)[@category = 'tech']", "xpath",
                List.of("/content/page1", "/content/page3"));
    }

    @Test
    public void testDescriptionQuery() throws Exception {
        createSearchIndex();
        createTestContent();
        assertQuery("//element(*, nt:base)[@description = 'Testing Oak search functionality']", "xpath",
                List.of("/content/page1"));
    }

    @Test
    public void testNoResults() throws Exception {
        createSearchIndex();
        createTestContent();
        assertQuery("//element(*, nt:base)[@title = 'NonExistent']", "xpath", List.of());
    }

    @Test
    public void testStatusEqualityQuery() throws Exception {
        createSearchIndex();
        createTestContent();
        assertQuery("//element(*, nt:base)[@status = 'published']", "xpath",
                List.of("/content/page1", "/content/page3"));
    }

    @Test
    public void testInLikeQuery() throws Exception {
        createSearchIndex();
        createTestContent();
        assertQuery("//element(*, nt:base)[@category = 'tech' or @category = 'search']", "xpath",
                List.of("/content/page1", "/content/page2", "/content/page3"));
    }

    // ===== Range queries =====

    @Test
    public void testNumericRangeQuery() throws Exception {
        createSearchIndex();
        createTestContent();
        // age > 30: page2(35) and page3(45)
        assertQuery("//element(*, nt:base)[@age > 30]", "xpath",
                List.of("/content/page2", "/content/page3"));
    }

    @Test
    public void testDoubleRangeQuery() throws Exception {
        createSearchIndex();
        createTestContent();
        // price >= 40: page2(45.50) and page3(75.00)
        assertQuery("//element(*, nt:base)[@price >= 40]", "xpath",
                List.of("/content/page2", "/content/page3"));
    }

    @Test
    public void testStringRangeQuery() throws Exception {
        createSearchIndex();
        createTestContent();
        // title >= 'M': "Oak Testing"(page1) and "Query DSL"(page3); "Lucene Integration" < 'M'
        assertQuery("//element(*, nt:base)[@title >= 'M']", "xpath",
                List.of("/content/page1", "/content/page3"));
    }

    // ===== Sorting queries =====

    @Test
    public void testSortByLongAscending() throws Exception {
        createSearchIndex();
        createTestContent();
        // age: page1(25), page2(35), page3(45)
        assertQuery("select [jcr:path] from [nt:base] where [age] > 0 order by [age]", "sql",
                List.of("/content/page1", "/content/page2", "/content/page3"), false, true);
    }

    @Test
    public void testSortByLongDescending() throws Exception {
        createSearchIndex();
        createTestContent();
        // age DESC: page3(45), page2(35), page1(25)
        assertQuery("select [jcr:path] from [nt:base] where [age] > 0 order by [age] DESC", "sql",
                List.of("/content/page3", "/content/page2", "/content/page1"), false, true);
    }

    @Test
    public void testSortByDoubleAscending() throws Exception {
        createSearchIndex();
        createTestContent();
        // price ASC: page1(15.99), page2(45.50), page3(75.00)
        assertQuery("select [jcr:path] from [nt:base] where [price] > 0 order by [price]", "sql",
                List.of("/content/page1", "/content/page2", "/content/page3"), false, true);
    }

    @Test
    public void testSortByDoubleDescending() throws Exception {
        createSearchIndex();
        createTestContent();
        // price DESC: page3(75.00), page2(45.50), page1(15.99)
        assertQuery("select [jcr:path] from [nt:base] where [price] > 0 order by [price] DESC", "sql",
                List.of("/content/page3", "/content/page2", "/content/page1"), false, true);
    }

    @Test
    public void testSortByStringAscending() throws Exception {
        createSearchIndex();
        createTestContent();
        // title ASC: "Lucene Integration"(page2), "Oak Testing"(page1), "Query DSL"(page3)
        assertQuery("select [jcr:path] from [nt:base] where [title] is not null order by [title]", "sql",
                List.of("/content/page2", "/content/page1", "/content/page3"), false, true);
    }

    @Test
    public void testSortByStringDescending() throws Exception {
        createSearchIndex();
        createTestContent();
        // title DESC: "Query DSL"(page3), "Oak Testing"(page1), "Lucene Integration"(page2)
        assertQuery("select [jcr:path] from [nt:base] where [title] is not null order by [title] DESC", "sql",
                List.of("/content/page3", "/content/page1", "/content/page2"), false, true);
    }

    @Test
    public void testMultiFieldSort() throws Exception {
        createSearchIndex();
        createTestContent();
        // status ASC then age DESC:
        //   draft: page2(35)
        //   published: page3(45) before page1(25)
        assertQuery("select [jcr:path] from [nt:base] where [status] is not null order by [status], [age] DESC", "sql",
                List.of("/content/page2", "/content/page3", "/content/page1"), false, true);
    }

    @Test
    public void testSortWithPropertyFilter() throws Exception {
        createSearchIndex();
        createTestContent();
        // status='published' order by age DESC: page3(45), page1(25)
        assertQuery("select [jcr:path] from [nt:base] where [status] = 'published' order by [age] DESC", "sql",
                List.of("/content/page3", "/content/page1"), false, true);
    }
}
