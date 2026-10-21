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
import org.apache.jackrabbit.oak.plugins.index.search.test.AbstractIndexComparisonTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Runs the shared {@link AbstractIndexComparisonTest} scenarios against the LuceneNg (Lucene 9) backend.
 */
public class LuceneNgIndexComparisonTest extends AbstractIndexComparisonTest {

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

    @Override
    protected void createSearchIndex() throws Exception {
        IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();

        builder.indexRule("nt:base")
            .property("title").propertyIndex().ordered()
            .property("description").propertyIndex()
            .property("age").propertyIndex().type("Long").ordered()
            .property("price").propertyIndex().type("Double").ordered()
            .property("status").propertyIndex().ordered()
            .property("category").propertyIndex();

        Tree index = builder.build(root.getTree("/").getChild("oak:index").addChild("luceneNgTestIndex"));
        index.setProperty("type", "lucene9");
        root.commit();
    }

    @Test
    public void testLuceneNgIndexIsUsed() throws Exception {
        createSearchIndex();
        createTestContent();
        String explain = executeQuery("explain //element(*, nt:base)[@title = 'Oak Testing']", "xpath").get(0);
        assertThat("Query plan should use lucene:...@v9 for Granite-style parsers",
                explain, containsString("lucene:luceneNgTestIndex@v9"));
        assertThat("Query plan should still expose lucene9 engine tag",
                explain, containsString("lucene9:luceneNgTestIndex"));
        assertThat("Query plan should use luceneQuery label like FulltextIndex.getPlanDescription",
                explain, containsString("luceneQuery:"));
        assertThat("Query plan should carry index definition path for tooling",
                explain, containsString("indexDefinition: /oak:index/luceneNgTestIndex"));
    }
}
