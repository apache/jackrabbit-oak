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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * End-to-end proof that index-time aggregation works: an aggregate rule folds a child node's
 * fulltext content into its parent's {@code :fulltext} field at index time (via
 * {@code LuceneNgDocumentMaker.indexAggregateValue}), so a fulltext query matches the
 * <em>parent</em> for text that exists only on the child.
 */
public class LuceneNgIndexEditorAggregationTest extends AbstractQueryTest {

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
    protected ContentRepository createRepository() {
        LuceneNgIndexTracker tracker = new LuceneNgIndexTracker();
        LuceneNgQueryIndexProvider provider = new LuceneNgQueryIndexProvider(tracker);
        LuceneNgIndexEditorProvider editor = new LuceneNgIndexEditorProvider(tracker);
        return new Oak()
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with(editor)
                .createContentRepository();
    }

    @Test
    public void parentFulltextIncludesAggregatedChildContent() throws Exception {
        // Fulltext index on "text" for nt:base, plus an aggregate rule pulling every child node
        // ("*") of an nt:base node into that node's node-scope fulltext.
        Tree index = root.getTree("/").addChild("oak:index").addChild("luceneNgAggIndex");
        index.setProperty("jcr:primaryType", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        index.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "lucene9");
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);

        Tree props = index.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE);
        Tree textProp = props.addChild("text");
        textProp.setProperty(FulltextIndexConstants.PROP_NAME, "text");
        textProp.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        textProp.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);

        Tree include = index.addChild(FulltextIndexConstants.AGGREGATES)
                .addChild("nt:base").addChild("include0");
        include.setProperty(FulltextIndexConstants.AGG_PATH, "*");
        root.commit();

        // Parent has NO "text" of its own; only its child carries the searched term.
        Tree parent = root.getTree("/").addChild("content").addChild("parent");
        Tree child = parent.addChild("child");
        child.setProperty("text", "findme here");
        root.commit();

        List<String> result = executeQuery(
                "select [jcr:path] from [nt:base] where contains(*, 'findme')", "JCR-SQL2");

        // THE proof: the parent — which has no "text" property of its own — is returned for a
        // fulltext query on the child's term, only because the aggregate rule folded the child's
        // "text" into the parent's :fulltext at index time (LuceneNgDocumentMaker.indexAggregateValue).
        assertTrue("index-time aggregation must fold the child's text into the parent, so the parent "
                        + "matches a fulltext query for the child's term; got " + result,
                result.contains("/content/parent"));
        // The child itself also matches directly, since it carries the term.
        assertTrue("the child node carrying the term must also match; got " + result,
                result.contains("/content/parent/child"));
    }
}
