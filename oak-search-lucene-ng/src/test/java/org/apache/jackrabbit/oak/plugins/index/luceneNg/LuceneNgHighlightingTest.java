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
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for highlighting functionality in Lucene 9 indexes.
 */
public class LuceneNgHighlightingTest extends AbstractQueryTest {

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        LuceneNgIndexEditorProvider editorProvider = new LuceneNgIndexEditorProvider(tracker);

        return new Oak()
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with(editorProvider)
                .createContentRepository();
    }

    @Test
    public void testHighlightMatchingTerms() throws Exception {
        // Create index with fulltext enabled
        Tree index = root.getTree("/").addChild("oak:index").addChild("testIdx");
        index.setProperty("jcr:primaryType", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        index.setProperty(IndexConstants.TYPE_PROPERTY_NAME, LuceneNgIndexConstants.TYPE_LUCENE9);
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        // Enable fulltext indexing
        Tree rules = index.addChild(FulltextIndexConstants.INDEX_RULES);
        Tree ntBase = rules.addChild("nt:base");
        ntBase.setProperty("indexNodeName", false);
        Tree props = ntBase.addChild(FulltextIndexConstants.PROP_NODE);
        Tree textProp = props.addChild("text");
        textProp.setProperty(FulltextIndexConstants.PROP_NAME, "text");
        textProp.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        textProp.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        textProp.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, true); // Enable highlighting

        root.commit();

        // Index content
        Tree content = root.getTree("/").addChild("content");
        Tree page1 = content.addChild("page1");
        page1.setProperty("text", "The quick brown fox jumps over the lazy dog");
        Tree page2 = content.addChild("page2");
        page2.setProperty("text", "Apache Jackrabbit Oak is a scalable content repository");
        root.commit();

        // Query with highlighting - search for "brown fox"
        String query = "select [rep:excerpt] from [nt:base] where contains(*, 'brown')";
        Result result = executeQuery(query, "JCR-SQL2", Collections.<String, PropertyValue>emptyMap());

        // Should find page1
        boolean foundPage1 = false;
        for (ResultRow row : result.getRows()) {
            if (row.getPath().equals("/content/page1")) {
                foundPage1 = true;
                // Check that excerpt column exists
                String excerpt = row.getValue("rep:excerpt").getValue(Type.STRING);
                assertNotNull("Excerpt should not be null", excerpt);
                // Excerpt should contain the matching term
                assertTrue("Excerpt should contain 'brown'", excerpt.contains("brown"));
                assertTrue("Excerpt should contain highlighting markers",
                    excerpt.contains("<") && excerpt.contains(">"));
            }
        }

        assertTrue("Should have found page1", foundPage1);
    }
}
