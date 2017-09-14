/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.Collections;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.secondary.DelegatingDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreCache;
import org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreObserver;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.createChild;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.asDocumentState;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlingTest.newNode;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BundledDocumentDifferTest {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private DocumentNodeStore store;
    private String journalDisabledProp;
    private BundledDocumentDiffer differ;
    private MemoryNodeStore secondary = new MemoryNodeStore();


    @Before
    public void setUpBundlor() throws CommitFailedException {
        journalDisabledProp = System.getProperty(SYS_PROP_DISABLE_JOURNAL);
        System.setProperty(SYS_PROP_DISABLE_JOURNAL, "true");
        store = builderProvider
                .newBuilder()
                .memoryCacheSize(0)
                .getNodeStore();
        NodeState registryState = BundledTypesRegistry.builder()
                .forType("app:Asset")
                .include("jcr:content")
                .include("jcr:content/metadata")
                .include("jcr:content/renditions")
                .include("jcr:content/renditions/**")
                .build();

        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        builder.getChildNode("jcr:system")
                .getChildNode(DOCUMENT_NODE_STORE)
                .getChildNode(BUNDLOR)
                .setChildNode("app:Asset", registryState.getChildNode("app:Asset"));
        merge(store, builder);
        differ = new BundledDocumentDiffer(store);
        store.runBackgroundOperations();
    }

    @After
    public void resetJournalUsage(){
        if (journalDisabledProp != null) {
            System.setProperty(SYS_PROP_DISABLE_JOURNAL, journalDisabledProp);
        } else {
            System.clearProperty(SYS_PROP_DISABLE_JOURNAL);
        }
    }

    @Test
    public void testDiff() throws Exception {
        NodeBuilder builder = createContentStructure();
        NodeState r1 = merge(store, builder);

        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content").setProperty("foo", "bar");
        NodeState r2 = merge(store, builder);

        JsopWriter w = new JsopBuilder();
        String path = "/test";
        assertTrue(differ.diff(dns(r1, path), dns(r2, path), w));
        assertTrue(w.toString().isEmpty());

        w = new JsopBuilder();
        path = "/test/book.jpg";
        assertFalse(differ.diff(dns(r1, path), dns(r2, path), w));
        assertEquals("^\"jcr:content\":{}", w.toString());

        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/foo");
        NodeState r3 = merge(store, builder);

        w = new JsopBuilder();
        path = "/test/book.jpg";
        //As there is a non bundled child differ should return true to continue diffing
        assertTrue(differ.diff(dns(r1, path), dns(r3, path), w));
        assertEquals("^\"jcr:content\":{}", w.toString());
    }

    @Test
    public void diffWithSecondary() throws Exception{
        configureSecondary();
        NodeBuilder builder = createContentStructure();
        NodeState r1 = merge(store, builder);
        NodeState rs1 = DelegatingDocumentNodeState.wrap(secondary.getRoot(), store);

        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content").setProperty("foo", "bar");
        NodeState r2 = merge(store, builder);

        JsopWriter w = new JsopBuilder();
        String path = "/test/book.jpg";
        assertFalse(differ.diff(adns(rs1, path), adns(r2, path), w));
        assertEquals("^\"jcr:content\":{}", w.toString());
    }

    @Test
    public void diffFewChildren() throws Exception{
        NodeBuilder builder = createContentStructure();
        NodeState r1 = merge(store, builder);

        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content").setProperty("foo", "bar");
        childBuilder(builder, "/test/book.jpg/jcr:content/renditions/newChild2").setProperty("foo", "bar");
        childBuilder(builder, "/test/book.jpg/newChild1");
        NodeState r2 = merge(store, builder);

        String path = "/test/book.jpg";
        CollectingDiff diff = new CollectingDiff();
        adns(r2, path).compareAgainstBaseState(adns(r1, path), diff);

        assertThat(diff.changes.get("added"), hasItem("newChild1"));
        assertThat(diff.changes.get("changed"), hasItem("jcr:content"));

        diff = new CollectingDiff();
        path = "/test/book.jpg/jcr:content/renditions";
        adns(r2, path).compareAgainstBaseState(adns(r1, path), diff);

        System.out.println(diff);
        assertThat(diff.changes.get("added"), hasItem("newChild2"));

    }

    @Test
    public void jsopDiff() throws Exception{
        JsopWriter w = new JsopBuilder();
        differ.diffChildren(of("a", "b"), of("b", "c"), w);

        //removed a
        //changed b
        //added b
        assertEquals("-\"a\"^\"b\":{}+\"c\":{}", w.toString());
    }

    private NodeBuilder createContentStructure() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder appNB = newNode("app:Asset");
        createChild(appNB,
                "jcr:content",
                "jcr:content/comments", //not bundled
                "jcr:content/metadata",
                "jcr:content/metadata/xmp", //not bundled
                "jcr:content/renditions", //includes all
                "jcr:content/renditions/original",
                "jcr:content/renditions/original/jcr:content"
        );

        builder.child("test").setChildNode("book.jpg", appNB.getNodeState());
        return builder;
    }

    private static class CollectingDiff extends DefaultNodeStateDiff {
        private ListMultimap<String, String> changes = ArrayListMultimap.create();

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            changes.get("added").add(name);
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            changes.get("changed").add(name);
            return super.childNodeChanged(name, before, after);
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            changes.get("deleted").add(name);
            return super.childNodeDeleted(name, before);
        }

        @Override
        public String toString() {
            return changes.toString();
        }
    }

    private DocumentNodeState dns(NodeState root, String path){
        return asDocumentState(NodeStateUtils.getNode(root, path));
    }

    private AbstractDocumentNodeState adns(NodeState root, String path){
        return (AbstractDocumentNodeState) NodeStateUtils.getNode(root, path);
    }

    private SecondaryStoreCache configureSecondary(){
        SecondaryStoreBuilder builder = createBuilder(new PathFilter(of("/"), Collections.<String>emptyList()));
        builder.metaPropNames(DocumentNodeStore.META_PROP_NAMES);
        SecondaryStoreCache cache = builder.buildCache();
        SecondaryStoreObserver observer = builder.buildObserver(cache);
        store.addObserver(observer);
        return cache;
    }

    private SecondaryStoreBuilder createBuilder(PathFilter pathFilter) {
        return new SecondaryStoreBuilder(secondary).pathFilter(pathFilter);
    }

}