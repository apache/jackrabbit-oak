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
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Functional tests for faceting support in LuceneNg (Lucene 9).
 * Verifies that facet counts are collected and returned correctly for:
 * - Basic single-dimension faceting
 * - Multiple facet dimensions in one query
 * - Facets scoped to a filtered result set
 */
public class LuceneNgFacetTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        LuceneNgIndexEditorProvider editor = new LuceneNgIndexEditorProvider(tracker);

        return new Oak()
            .with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with((org.apache.jackrabbit.oak.spi.query.QueryIndexProvider) provider)
            .with((Observer) provider)
            .with(editor)
            .createContentRepository();
    }

    /**
     * Creates a LuceneNg index with category and author as facet-enabled properties.
     */
    private void createFacetIndex() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        builder.indexRule("nt:base")
            .property("text").propertyIndex()
            .property("category").propertyIndex().facets()
            .property("author").propertyIndex().facets();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgFacetIndex"));
        index.setProperty("type", "lucene9");

        root.commit();
    }

    /**
     * Creates 4 test documents:
     * - category: tech(3), science(1)
     * - author: alice(3), bob(1)
     *
     * Layout:
     *   doc1: category=tech,    author=alice
     *   doc2: category=tech,    author=alice
     *   doc3: category=tech,    author=bob
     *   doc4: category=science, author=alice
     */
    private void createTestDocuments() throws Exception {
        Tree content = root.getTree("/").addChild("facetContent");

        Tree doc1 = content.addChild("doc1");
        doc1.setProperty("jcr:primaryType", "nt:unstructured");
        doc1.setProperty("text", "some text");
        doc1.setProperty("category", "tech");
        doc1.setProperty("author", "alice");

        Tree doc2 = content.addChild("doc2");
        doc2.setProperty("jcr:primaryType", "nt:unstructured");
        doc2.setProperty("text", "some text");
        doc2.setProperty("category", "tech");
        doc2.setProperty("author", "alice");

        Tree doc3 = content.addChild("doc3");
        doc3.setProperty("jcr:primaryType", "nt:unstructured");
        doc3.setProperty("text", "some text");
        doc3.setProperty("category", "tech");
        doc3.setProperty("author", "bob");

        Tree doc4 = content.addChild("doc4");
        doc4.setProperty("jcr:primaryType", "nt:unstructured");
        doc4.setProperty("text", "some text");
        doc4.setProperty("category", "science");
        doc4.setProperty("author", "alice");

        root.commit();
    }

    /**
     * Executes a SQL2 query and parses facets from the Oak Result.
     *
     * Facet data is stored on the first result row — FacetResult reads rep:facet(X)
     * column values from that row. The Oak FacetResult constructor accepting
     * String[] columnNames and FacetResultRow is used to bridge from Oak's ResultRow
     * (PropertyValue-based) to FacetResult's interface.
     */
    private FacetResult executeFacetQuery(String query) throws ParseException {
        Result result = executeQuery(query, SQL2, NO_BINDINGS);
        String[] columnNames = result.getColumnNames();

        List<ResultRow> rows = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            rows.add(row);
        }

        if (rows.isEmpty()) {
            return new FacetResult(columnNames);
        }

        ResultRow firstRow = rows.get(0);
        return new FacetResult(columnNames, columnName -> {
            PropertyValue pv = firstRow.getValue(columnName);
            return pv == null ? null : pv.getValue(Type.STRING);
        });
    }

    @Test
    public void testBasicFaceting() throws Exception {
        createFacetIndex();
        createTestDocuments();

        String query = "select [jcr:path], [rep:facet(category)] from [nt:base] where [text] is not null";
        FacetResult facets = executeFacetQuery(query);

        List<FacetResult.Facet> categoryFacets = facets.getFacets("category");
        assertNotNull("Expected category facets to be present", categoryFacets);
        assertEquals("Expected 2 category values", 2, categoryFacets.size());

        int techCount = 0;
        int scienceCount = 0;
        for (FacetResult.Facet facet : categoryFacets) {
            if ("tech".equals(facet.getLabel())) {
                techCount = facet.getCount();
            } else if ("science".equals(facet.getLabel())) {
                scienceCount = facet.getCount();
            }
        }

        assertEquals("Expected 3 docs in category 'tech'", 3, techCount);
        assertEquals("Expected 1 doc in category 'science'", 1, scienceCount);
    }

    @Test
    public void testMultipleFacetDimensions() throws Exception {
        createFacetIndex();
        createTestDocuments();

        String query = "select [jcr:path], [rep:facet(category)], [rep:facet(author)] from [nt:base] where [text] is not null";
        FacetResult facets = executeFacetQuery(query);

        // Verify category dimension
        List<FacetResult.Facet> categoryFacets = facets.getFacets("category");
        assertNotNull("Expected category facets", categoryFacets);
        assertEquals("Expected 2 category values", 2, categoryFacets.size());

        int techCount = 0;
        int scienceCount = 0;
        for (FacetResult.Facet facet : categoryFacets) {
            if ("tech".equals(facet.getLabel())) {
                techCount = facet.getCount();
            } else if ("science".equals(facet.getLabel())) {
                scienceCount = facet.getCount();
            }
        }
        assertEquals("Expected 3 docs in category 'tech'", 3, techCount);
        assertEquals("Expected 1 doc in category 'science'", 1, scienceCount);

        // Verify author dimension
        List<FacetResult.Facet> authorFacets = facets.getFacets("author");
        assertNotNull("Expected author facets", authorFacets);
        assertEquals("Expected 2 author values", 2, authorFacets.size());

        int aliceCount = 0;
        int bobCount = 0;
        for (FacetResult.Facet facet : authorFacets) {
            if ("alice".equals(facet.getLabel())) {
                aliceCount = facet.getCount();
            } else if ("bob".equals(facet.getLabel())) {
                bobCount = facet.getCount();
            }
        }
        assertEquals("Expected 3 docs by author 'alice'", 3, aliceCount);
        assertEquals("Expected 1 doc by author 'bob'", 1, bobCount);
    }

    @Test
    public void testFacetWithFilter() throws Exception {
        createFacetIndex();
        createTestDocuments();

        // Filter to category=tech only: doc1(alice), doc2(alice), doc3(bob)
        String query = "select [jcr:path], [rep:facet(author)] from [nt:base] where [category] = 'tech'";
        FacetResult facets = executeFacetQuery(query);

        List<FacetResult.Facet> authorFacets = facets.getFacets("author");
        assertNotNull("Expected author facets for tech category filter", authorFacets);
        assertEquals("Expected 2 author values for tech docs", 2, authorFacets.size());

        int aliceCount = 0;
        int bobCount = 0;
        for (FacetResult.Facet facet : authorFacets) {
            if ("alice".equals(facet.getLabel())) {
                aliceCount = facet.getCount();
            } else if ("bob".equals(facet.getLabel())) {
                bobCount = facet.getCount();
            }
        }
        assertEquals("Expected 2 tech docs by author 'alice'", 2, aliceCount);
        assertEquals("Expected 1 tech doc by author 'bob'", 1, bobCount);
    }
}
