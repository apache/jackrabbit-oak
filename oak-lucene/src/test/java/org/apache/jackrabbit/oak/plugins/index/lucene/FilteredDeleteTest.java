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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexEditor;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * OAK-12193 behavioral regression tests for the filtered-delete path in
 * {@link FulltextIndexEditor#childNodeDeleted}. Asserts on the cumulative
 * {@code AsyncIndexStats.updates} counter to verify that deleteDocuments
 * is routed only when the removed subtree contains a node matching the
 * index's declaringNodeTypes. Uses an in-memory node store for speed.
 */
public class FilteredDeleteTest {

    private static final int NUM_INDEXES = 3;
    private static final int FAN_OUT = 100;
    private static final int LEAF_COUNT = FAN_OUT * FAN_OUT;

    private Root root;
    private AsyncIndexUpdate asyncIndexUpdate;

    @Before
    public void before() throws Exception {
        // reset toggle to its default (enabled) before each test
        FulltextIndexEditor.FT_OAK_12193_DISABLE.set(false);
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
    }

    @After
    public void after() {
        FulltextIndexEditor.FT_OAK_12193_DISABLE.set(false);
    }

    private ContentRepository createRepository() {
        NodeStore nodeStore = new MemoryNodeStore();
        MemoryBlobStore blobStore = new MemoryBlobStore();
        blobStore.setBlockSizeMin(48);

        LuceneIndexEditorProvider luceneIndexEditorProvider = new LuceneIndexEditorProvider();
        LuceneIndexProvider provider = new LuceneIndexProvider();
        luceneIndexEditorProvider.setBlobStore(blobStore);

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore,
                CompositeIndexEditorProvider.compose(
                        luceneIndexEditorProvider,
                        new NodeCounterEditorProvider()));
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(luceneIndexEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    /**
     * Fix enabled (default). Indexes declare {@code nt:file} but content is
     * {@code nt:unstructured} — no node in the deleted subtree can match any
     * rule, so {@code deleteDocuments} must be skipped for every leaf delete.
     */
    @Test
    public void skipsDeletesWhenSubtreeHasNoMatchingNodes() throws Exception {
        createIndexes("nt:file");
        populateContentAndIndex();
        deleteLeavesIndividually();
        asyncIndexUpdate.run();

        long updates = deleteCycleUpdates();
        // Legacy bug path would produce LEAF_COUNT * NUM_INDEXES (30,000) here; the fix
        // produces at most a small bookkeeping count from unrelated editors in the pipeline.
        assertTrue("Expected filtered-delete path to skip deleteDocuments (got " + updates + ")",
                updates < LEAF_COUNT);
    }

    /**
     * Fix enabled (default). Indexes declare {@code nt:unstructured} and the
     * content matches — every leaf delete must be routed to every index's
     * writer exactly as before the fix.
     */
    @Test
    public void routesDeletesWhenSubtreeHasMatchingNodes() throws Exception {
        createIndexes("nt:unstructured");
        populateContentAndIndex();
        deleteLeavesIndividually();
        asyncIndexUpdate.run();

        long updates = deleteCycleUpdates();
        assertTrue("Every leaf delete should route to every index when content matches (got " + updates + ")",
                updates >= (long) LEAF_COUNT * NUM_INDEXES);
    }

    /**
     * Fix disabled (legacy behavior). Indexes declare {@code nt:file} and
     * content is {@code nt:unstructured} (no match), yet every leaf delete
     * must still be routed — demonstrating that the toggle gates the new
     * filtered-delete path and the legacy path is preserved.
     */
    @Test
    public void toggleDisabledRoutesAllDeletesEvenWhenNoMatch() throws Exception {
        FulltextIndexEditor.FT_OAK_12193_DISABLE.set(true);

        createIndexes("nt:file");
        populateContentAndIndex();
        deleteLeavesIndividually();
        asyncIndexUpdate.run();

        long updates = deleteCycleUpdates();
        assertTrue("Legacy behavior: every leaf delete routes to every index (got " + updates + ")",
                updates >= (long) LEAF_COUNT * NUM_INDEXES);
    }

    /**
     * {@code AsyncIndexStats.updates} is reset at the start of each async cycle,
     * so the value read after the final run reflects only the delete-heavy cycle.
     */
    private long deleteCycleUpdates() {
        return ((IndexStatsMBean) asyncIndexUpdate.getIndexStats()).getUpdates();
    }

    private void createIndexes(String declaringNodeType) throws Exception {
        for (int i = 0; i < NUM_INDEXES; i++) {
            LuceneIndexDefinitionBuilder idxb = new LuceneIndexDefinitionBuilder();
            idxb.async("async");
            idxb.includedPaths("/content");
            idxb.indexRule(declaringNodeType)
                    .property("jcr:title").propertyIndex();
            idxb.build(root.getTree("/oak:index").addChild("idx" + i));
        }
        root.commit();
    }

    private void populateContentAndIndex() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        content.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
        root.commit();

        for (int i = 0; i < FAN_OUT; i++) {
            Tree child = root.getTree("/content").addChild("child" + i);
            child.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
            for (int j = 0; j < FAN_OUT; j++) {
                Tree grandchild = child.addChild("child" + j);
                grandchild.setProperty("jcr:primaryType", "nt:unstructured", Type.NAME);
                grandchild.setProperty("jcr:title", "v-" + i + "-" + j);
            }
            root.commit();
        }
        asyncIndexUpdate.run();
    }

    private void deleteLeavesIndividually() throws Exception {
        int deleted = 0;
        for (int i = 0; i < FAN_OUT; i++) {
            for (int j = 0; j < FAN_OUT; j++) {
                root.getTree("/content/child" + i + "/child" + j).remove();
                deleted++;
                if (deleted % 1000 == 0) {
                    root.commit();
                }
            }
        }
        root.commit();
    }
}
