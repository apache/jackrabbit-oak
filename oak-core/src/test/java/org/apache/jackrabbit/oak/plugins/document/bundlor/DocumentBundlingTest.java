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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.PathRev;
import org.apache.jackrabbit.oak.plugins.document.RandomStream;
import org.apache.jackrabbit.oak.plugins.document.TestNodeObserver;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.copyOf;
import static java.lang.String.format;
import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.asDocumentState;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.createChild;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_BUNDLED_CHILD;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DocumentBundlingTest {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private DocumentNodeStore store;
    private RecordingDocumentStore ds = new RecordingDocumentStore();
    private String journalDisabledProp;

    @Before
    public void setUpBundlor() throws CommitFailedException {
        store = builderProvider
                .newBuilder()
                .setDocumentStore(ds)
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
        builder.getChildNode("jcr:system")
                .getChildNode(DOCUMENT_NODE_STORE)
                .getChildNode(BUNDLOR)
                .setChildNode("app:Asset", registryState.getChildNode("app:Asset"));
        merge(builder);
        store.runBackgroundOperations();
        journalDisabledProp = System.getProperty(SYS_PROP_DISABLE_JOURNAL);
    }

    @After
    public void resetSysProps(){
        if (journalDisabledProp != null) {
            System.setProperty(SYS_PROP_DISABLE_JOURNAL, journalDisabledProp);
        } else {
            System.clearProperty(SYS_PROP_DISABLE_JOURNAL);
        }
    }

    @Test
    public void saveAndReadNtFile() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());

        merge(builder);

        NodeState root = store.getRoot();
        NodeState fileNodeState = root.getChildNode("test");
        assertTrue(fileNodeState.getChildNode("book.jpg").exists());
        NodeState contentNode = fileNodeState.getChildNode("book.jpg").getChildNode("jcr:content");
        assertTrue(contentNode.exists());
        assertEquals("jcr:content", getBundlingPath(contentNode));

        assertEquals(1, contentNode.getPropertyCount());
        assertEquals(1, Iterables.size(contentNode.getProperties()));

        assertNull(getNodeDocument("/test/book.jpg/jcr:content"));
        assertNotNull(getNodeDocument("/test/book.jpg"));
        assertTrue(hasNodeProperty("/test/book.jpg", concat("jcr:content", DocumentBundlor.META_PROP_BUNDLING_PATH)));

        AssertingDiff.assertEquals(fileNode.getNodeState(), fileNodeState.getChildNode("book.jpg"));

        DocumentNodeState dns = asDocumentState(fileNodeState.getChildNode("book.jpg"));
        AssertingDiff.assertEquals(fileNode.getNodeState(), dns.withRootRevision(dns.getRootRevision(), true));
        AssertingDiff.assertEquals(fileNode.getNodeState(), dns.fromExternalChange());
    }

    @Test
    public void memory() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder bundledFileNode = newNode("nt:file");
        bundledFileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", bundledFileNode.getNodeState());

        //Create a non bundled NodeState structure nt:File vs nt:file
        NodeBuilder nonBundledFileNode = newNode("nt:File");
        nonBundledFileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book2.jpg", nonBundledFileNode.getNodeState());
        merge(builder);

        NodeState root = store.getRoot();
        DocumentNodeState bundledFile = asDocumentState(getNode(root, "/test/book.jpg"));
        DocumentNodeState nonBundledFile = asDocumentState(getNode(root, "/test/book2.jpg"));
        DocumentNodeState nonBundledContent = asDocumentState(getNode(root, "/test/book2.jpg/jcr:content"));

        int nonBundledMem = nonBundledFile.getMemory() + nonBundledContent.getMemory();
        int bundledMem = bundledFile.getMemory();

        assertEquals(1386, bundledMem);
        assertThat(bundledMem, is(greaterThan(nonBundledMem)));
    }

    @Test
    public void memoryWithBinary() throws Exception {
        Blob blob = store.createBlob(new RandomStream(1024, 17));
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder bundledFileNode = newNode("nt:file");
        bundledFileNode.child("jcr:content").setProperty("jcr:data", blob);
        builder.child("test").setChildNode("book.jpg", bundledFileNode.getNodeState());

        //Create a non bundled NodeState structure nt:File vs nt:file
        NodeBuilder nonBundledFileNode = newNode("nt:File");
        nonBundledFileNode.child("jcr:content").setProperty("jcr:data", blob);
        builder.child("test").setChildNode("book2.jpg", nonBundledFileNode.getNodeState());
        merge(builder);

        NodeState root = store.getRoot();
        DocumentNodeState bundledFile = asDocumentState(getNode(root, "/test/book.jpg"));
        DocumentNodeState nonBundledFile = asDocumentState(getNode(root, "/test/book2.jpg"));
        DocumentNodeState nonBundledContent = asDocumentState(getNode(root, "/test/book2.jpg/jcr:content"));

        int nonBundledMem = nonBundledFile.getMemory() + nonBundledContent.getMemory();
        int bundledMem = bundledFile.getMemory();

        assertThat(bundledMem, is(greaterThan(nonBundledMem)));
    }

    @Test
    public void bundlorUtilsIsBundledMethods() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());

        merge(builder);
        NodeState fileNodeState =  NodeStateUtils.getNode(store.getRoot(), "/test/book.jpg");
        assertTrue(BundlorUtils.isBundledNode(fileNodeState));
        assertFalse(BundlorUtils.isBundledChild(fileNodeState));
        assertTrue(BundlorUtils.isBundlingRoot(fileNodeState));

        NodeState contentNode =  NodeStateUtils.getNode(store.getRoot(), "/test/book.jpg/jcr:content");
        assertTrue(BundlorUtils.isBundledNode(contentNode));
        assertTrue(BundlorUtils.isBundledChild(contentNode));
        assertFalse(BundlorUtils.isBundlingRoot(contentNode));
    }

    @Test
    public void bundledParent() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder appNB = newNode("app:Asset");
        createChild(appNB,
                "jcr:content", //Bundled
                "jcr:content/comments" //Not bundled. Parent bundled
        );
        dump(appNB.getNodeState());
        builder.child("test").setChildNode("book.jpg", appNB.getNodeState());

        merge(builder);
    }

    @Test
    public void bundlingPath() throws Exception{
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

        merge(builder);
        DocumentNodeState appNode = (DocumentNodeState)getNode(store.getRoot(), "test/book.jpg");

        assertEquals("jcr:content", getBundlingPath(appNode.getChildNode("jcr:content")));
        assertEquals("jcr:content/metadata",
                getBundlingPath(appNode.getChildNode("jcr:content").getChildNode("metadata")));
        assertEquals("jcr:content/renditions/original",
                getBundlingPath(appNode.getChildNode("jcr:content").getChildNode("renditions").getChildNode("original")));

        List<String> bundledPaths = new ArrayList<>();
        for (DocumentNodeState bs : appNode.getAllBundledNodesStates()) {
            bundledPaths.add(bs.getPath());
        }
        assertThat(bundledPaths, containsInAnyOrder(
                "/test/book.jpg/jcr:content",
                "/test/book.jpg/jcr:content/metadata",
                "/test/book.jpg/jcr:content/renditions",
                "/test/book.jpg/jcr:content/renditions/original",
                "/test/book.jpg/jcr:content/renditions/original/jcr:content")
        );
    }
    
    @Test
    public void queryChildren() throws Exception{
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

        merge(builder);
        NodeState appNode = getNode(store.getRoot(), "test/book.jpg");

        ds.reset();

        int childCount = Iterables.size(appNode.getChildNodeEntries());
        assertEquals(1, childCount);
        assertEquals(0, ds.queryPaths.size());

        assertThat(childNames(appNode, "jcr:content"), hasItems("comments", "metadata", "renditions"));
        assertEquals(3, getNode(appNode, "jcr:content").getChildNodeCount(100));

        assertThat(childNames(appNode, "jcr:content/metadata"), hasItems("xmp"));
        assertEquals(1, getNode(appNode, "jcr:content/metadata").getChildNodeCount(100));

        ds.reset();
        //For bundled case no query should be fired
        assertThat(childNames(appNode, "jcr:content/renditions"), hasItems("original"));
        assertEquals(1, getNode(appNode, "jcr:content/renditions").getChildNodeCount(100));
        assertEquals(0, ds.queryPaths.size());

        assertThat(childNames(appNode, "jcr:content/renditions/original"), hasItems("jcr:content"));
        assertEquals(1, getNode(appNode, "jcr:content/renditions/original").getChildNodeCount(100));
        assertEquals(0, ds.queryPaths.size());

        AssertingDiff.assertEquals(appNB.getNodeState(), appNode);
    }

    @Test
    public void hasChildren() throws Exception{
        createTestNode("/test/book.jpg", createChild(newNode("app:Asset"),
                "jcr:content",
                "jcr:content/comments", //not bundled
                "jcr:content/metadata"
        ).getNodeState());

        ds.reset();

        assertEquals(0, Iterables.size(getLatestNode("test/book.jpg/jcr:content/metadata").getChildNodeNames()));
        assertEquals(0, ds.queryPaths.size());

        //Case 1 - Bundled root but no bundled child
        //Case 2 - Bundled root but non bundled child
        //Case 3 - Bundled root but  bundled child
        //Case 3 - Bundled node but  no bundled child
        //Case 3 - Bundled leaf node but child can be present in non bundled form
        //Case 3 - Bundled leaf node but all child bundled
    }

    @Test
    public void hasChildren_BundledRoot_NoChild() throws Exception{
        createTestNode("/test/book.jpg", createChild(newNode("app:Asset")).getNodeState());

        ds.reset();

        assertEquals(0, Iterables.size(getLatestNode("test/book.jpg").getChildNodeNames()));
        assertEquals(0, getLatestNode("test/book.jpg").getChildNodeCount(100));
        assertEquals(0, ds.queryPaths.size());

        NodeBuilder builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content");
        merge(builder);

        ds.reset();
        assertEquals(1, Iterables.size(getLatestNode("test/book.jpg").getChildNodeNames()));
        assertEquals(1, getLatestNode("test/book.jpg").getChildNodeCount(100));
        assertEquals(0, ds.queryPaths.size());
        assertTrue(hasNodeProperty("/test/book.jpg", META_PROP_BUNDLED_CHILD));
        assertFalse(hasNodeProperty("/test/book.jpg", "_children"));
    }

    @Test
    public void hasChildren_BundledRoot_BundledChild() throws Exception{
        createTestNode("/test/book.jpg", createChild(newNode("app:Asset"), "jcr:content").getNodeState());

        ds.reset();

        assertEquals(1, Iterables.size(getLatestNode("test/book.jpg").getChildNodeNames()));
        assertEquals(0, ds.queryPaths.size());

        NodeBuilder builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/fooContent");
        merge(builder);

        ds.reset();
        assertEquals(2, Iterables.size(getLatestNode("test/book.jpg").getChildNodeNames()));
        assertEquals(1, ds.queryPaths.size());

        assertTrue(hasNodeProperty("/test/book.jpg", META_PROP_BUNDLED_CHILD));
        assertTrue(hasNodeProperty("/test/book.jpg", "_children"));
    }


    @Test
    public void hasChildren_BundledRoot_NonBundledChild() throws Exception{
        createTestNode("/test/book.jpg", createChild(newNode("app:Asset"), "fooContent").getNodeState());

        ds.reset();

        assertEquals(1, Iterables.size(getLatestNode("test/book.jpg").getChildNodeNames()));
        assertEquals(1, ds.queryPaths.size());

        assertFalse(hasNodeProperty("/test/book.jpg", META_PROP_BUNDLED_CHILD));
        assertTrue(hasNodeProperty("/test/book.jpg", "_children"));
    }

    @Test
    public void hasChildren_BundledNode_NoChild() throws Exception{
        createTestNode("/test/book.jpg", createChild(newNode("app:Asset"),
                "jcr:content"
        ).getNodeState());

        ds.reset();

        assertEquals(0, Iterables.size(getLatestNode("test/book.jpg/jcr:content").getChildNodeNames()));
        assertEquals(0, ds.queryPaths.size());

        NodeBuilder builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/metadata");
        merge(builder);

        ds.reset();
        assertEquals(1, Iterables.size(getLatestNode("test/book.jpg/jcr:content").getChildNodeNames()));
        assertEquals(0, ds.queryPaths.size());
    }

    @Test
    public void hasChildren_BundledLeafNode_NoChild() throws Exception{
        createTestNode("/test/book.jpg", createChild(newNode("app:Asset"),
                "jcr:content/metadata"
        ).getNodeState());

        ds.reset();

        assertEquals(0, Iterables.size(getLatestNode("test/book.jpg/jcr:content/metadata").getChildNodeNames()));
        assertEquals(0, ds.queryPaths.size());

        NodeBuilder builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/metadata/xmp");
        merge(builder);

        ds.reset();
        assertEquals(1, Iterables.size(getLatestNode("test/book.jpg/jcr:content/metadata").getChildNodeNames()));
        assertEquals(1, ds.queryPaths.size());
    }

    @Test
    public void addBundledNodePostInitialCreation() throws Exception{
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

        merge(builder);

        builder = store.getRoot().builder();
        NodeBuilder renditions = childBuilder(builder, "/test/book.jpg/jcr:content/renditions");
        renditions.child("small").child("jcr:content");
        NodeState appNode_v2 = childBuilder(builder, "/test/book.jpg").getNodeState();
        merge(builder);

        assertThat(childNames(getLatestNode("/test/book.jpg"), "jcr:content/renditions"),
                hasItems("original", "small"));
        assertTrue(AssertingDiff.assertEquals(getLatestNode("/test/book.jpg"), appNode_v2));
    }

    @Test
    public void modifyBundledChild() throws Exception{
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

        merge(builder);

        //Modify bundled property
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content").setProperty("foo", "bar");
        merge(builder);

        NodeState state = childBuilder(builder, "/test/book.jpg").getNodeState();
        assertEquals("bar", getLatestNode("/test/book.jpg/jcr:content").getString("foo"));
        assertTrue(AssertingDiff.assertEquals(state, getLatestNode("/test/book.jpg")));

        //Modify deep bundled property
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/renditions").setProperty("foo", "bar");
        merge(builder);

        state = childBuilder(builder, "/test/book.jpg").getNodeState();
        assertEquals("bar", getLatestNode("/test/book.jpg/jcr:content/renditions").getString("foo"));
        assertTrue(AssertingDiff.assertEquals(state, getLatestNode("/test/book.jpg")));

        //Modify deep unbundled property - jcr:content/comments/@foo
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/comments").setProperty("foo", "bar");
        merge(builder);

        state = childBuilder(builder, "/test/book.jpg").getNodeState();
        assertEquals("bar", getLatestNode("/test/book.jpg/jcr:content/comments").getString("foo"));
        assertTrue(AssertingDiff.assertEquals(state, getLatestNode("/test/book.jpg")));
    }

    @Test
    public void deleteBundledNode() throws Exception{
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

        childBuilder(appNB, "jcr:content/metadata").setProperty("foo", "bar");
        childBuilder(appNB, "jcr:content/comments").setProperty("foo", "bar");
        builder.child("test").setChildNode("book.jpg", appNB.getNodeState());

        merge(builder);

        //Delete a bundled node jcr:content/metadata
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/metadata").remove();
        NodeState appNode_v2 = childBuilder(builder, "/test/book.jpg").getNodeState();
        merge(builder);

        assertTrue(AssertingDiff.assertEquals(appNode_v2, getLatestNode("/test/book.jpg")));


        //Delete unbundled child jcr:content/comments
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/comments").remove();
        NodeState appNode_v3 = childBuilder(builder, "/test/book.jpg").getNodeState();
        merge(builder);

        assertTrue(AssertingDiff.assertEquals(appNode_v3, getLatestNode("/test/book.jpg")));

    }

    @Test
    public void binaryFlagSet() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder appNB = newNode("app:Asset");
        createChild(appNB,
                "jcr:content",
                "jcr:content/renditions", //includes all
                "jcr:content/renditions/original",
                "jcr:content/renditions/original/jcr:content"
        );


        builder.child("test").setChildNode("book.jpg", appNB.getNodeState());
        merge(builder);

        assertFalse(getNodeDocument("/test/book.jpg").hasBinary());

        builder = store.getRoot().builder();
        childBuilder(builder, "test/book.jpg/jcr:content/renditions/original/jcr:content").setProperty("foo", "bar".getBytes());
        merge(builder);
        assertTrue(getNodeDocument("/test/book.jpg").hasBinary());
    }

    @Test
    public void jsonSerialization() throws Exception{
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

        merge(builder);
        DocumentNodeState appNode = (DocumentNodeState) getNode(store.getRoot(), "test/book.jpg");
        String json = appNode.asString();
        NodeState appNode2 = DocumentNodeState.fromString(store, json);
        AssertingDiff.assertEquals(appNode, appNode2);
    }

    @Test
    public void documentNodeStateBundledMethods() throws Exception{
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

        merge(builder);
        DocumentNodeState appNode = (DocumentNodeState) getNode(store.getRoot(), "test/book.jpg");
        assertTrue(appNode.hasOnlyBundledChildren());
        assertEquals(ImmutableSet.of("jcr:content"), appNode.getBundledChildNodeNames());
        assertEquals(ImmutableSet.of("metadata", "renditions"),
                asDocumentState(appNode.getChildNode("jcr:content")).getBundledChildNodeNames());

        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/foo");
        merge(builder);

        DocumentNodeState appNode2 = (DocumentNodeState) getNode(store.getRoot(), "test/book.jpg");
        assertFalse(appNode2.hasOnlyBundledChildren());
    }

    @Test
    public void bundledDocsShouldNotBePartOfBackgroundUpdate() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());

        merge(builder);
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content/vlt:definition").setProperty("foo", "bar");
        merge(builder);

        //2 for /test, /test/book.jpg and /test/book.jpg/jcr:content should be there
        //TODO If UnsavedModification is made public we can assert on path itself
        assertEquals(2, store.getPendingWriteCount());
    }

    @Test
    public void bundledNodeAndNodeCache() throws Exception{
        store.dispose();
        store = builderProvider
                .newBuilder()
                .setDocumentStore(ds)
                .memoryCacheSize(ONE_MB * 10)
                .getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());

        //Make some modifications under the bundled node
        //This would cause an entry for bundled node in Commit modified set
        childBuilder(builder, "/test/book.jpg/jcr:content/vlt:definition").setProperty("foo", "bar");

        TestNodeObserver o = new TestNodeObserver("test");
        store.addObserver(o);
        merge(builder);
        assertEquals(4, o.added.size());

    }

    @Test
    public void bundledNodeAndNodeChildrenCache() throws Exception{
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

        merge(builder);

        Set<PathRev> cachedPaths = store.getNodeChildrenCache().asMap().keySet();
        for (PathRev pr : cachedPaths){
            assertFalse(pr.getPath().contains("jcr:content/renditions"));
        }
    }

    @Test
    public void recreatedBundledNode() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        builder = store.getRoot().builder();
        builder.child("test").child("book.jpg").remove();

        fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo2");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);
    }

    @Test
    public void recreatedBundledNode2() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        builder = store.getRoot().builder();
        builder.child("a");
        //In this case we recreate the node in CommitHook
        store.merge(builder, new EditorHook(new EditorProvider() {
            @Override
            public Editor getRootEditor(NodeState before, NodeState after,
                                        NodeBuilder builder, CommitInfo info) throws CommitFailedException {
                return new BookRecreatingEditor(builder);
            }
        }), CommitInfo.EMPTY);
    }

    private static class BookRecreatingEditor extends DefaultEditor {
        final NodeBuilder builder;

        private BookRecreatingEditor(NodeBuilder builder) {
            this.builder = builder;
        }

        @Override
        public void enter(NodeState before, NodeState after) throws CommitFailedException {
            builder.child("test").remove();

            NodeBuilder book = builder.child("test").child("book.jpg");
            book.setProperty(JCR_PRIMARYTYPE, "nt:file");
            book.child("jcr:content");
        }
    }

    @Test
    public void deleteAndRecreatedBundledNode() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        builder = store.getRoot().builder();
        builder.child("test").child("book.jpg").remove();
        merge(builder);

        builder = store.getRoot().builder();
        fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo2");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);
    }

    @Test
    public void bundlingPatternChangedAndBundledNodeRecreated() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        assertTrue(hasNodeProperty("/test/book.jpg", concat("jcr:content", DocumentBundlor.META_PROP_BUNDLING_PATH)));

        //Delete the bundled node structure
        builder = store.getRoot().builder();
        builder.child("test").child("book.jpg").remove();
        merge(builder);

        //Change bundling pattern
        builder = store.getRoot().builder();
        NodeBuilder ntfile = childBuilder(builder, BundlingConfigHandler.CONFIG_PATH+"/nt:file");
        ntfile.setProperty("pattern", Arrays.asList("jcr:content", "metadata"), Type.STRINGS);
        merge(builder);

        //Create a new node with same path
        builder = store.getRoot().builder();
        fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        fileNode.child("metadata").setProperty("jcr:data", "foo");
        fileNode.child("comments").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        //New pattern should be effective
        assertTrue(hasNodeProperty("/test/book.jpg", concat("metadata", DocumentBundlor.META_PROP_BUNDLING_PATH)));
        assertFalse(hasNodeProperty("/test/book.jpg", concat("comments", DocumentBundlor.META_PROP_BUNDLING_PATH)));
    }

    @Test
    public void bundlingPatternRemovedAndBundledNodeRecreated() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        assertTrue(hasNodeProperty("/test/book.jpg", concat("jcr:content", DocumentBundlor.META_PROP_BUNDLING_PATH)));

        //Delete the bundled node structure
        builder = store.getRoot().builder();
        builder.child("test").child("book.jpg").remove();
        merge(builder);

        //Change bundling pattern
        builder = store.getRoot().builder();
        NodeBuilder ntfile = childBuilder(builder, BundlingConfigHandler.CONFIG_PATH+"/nt:file");
        ntfile.remove();
        merge(builder);

        //Create a new node with same path
        builder = store.getRoot().builder();
        fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        fileNode.child("metadata").setProperty("jcr:data", "foo");
        fileNode.child("comments").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        //New pattern should be effective
        assertNotNull(getNodeDocument("/test/book.jpg"));
        assertFalse(hasNodeProperty("/test/book.jpg", concat("metadata", DocumentBundlor.META_PROP_BUNDLING_PATH)));
        assertFalse(hasNodeProperty("/test/book.jpg", concat("comments", DocumentBundlor.META_PROP_BUNDLING_PATH)));
    }

    @Test
    public void journalDiffAndBundling() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());

        NodeState r1 = merge(builder);
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content").setProperty("fooContent", "bar");
        childBuilder(builder, "/test/book.jpg").setProperty("fooBook", "bar");
        NodeState r2 = merge(builder);

        final List<String> addedPropertyNames = Lists.newArrayList();
        r2.compareAgainstBaseState(r1, new DefaultNodeStateDiff(){

            @Override
            public boolean propertyAdded(PropertyState after) {
                addedPropertyNames.add(after.getName());
                return super.propertyAdded(after);
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                return after.compareAgainstBaseState(before, this);
            }
        });

        assertTrue("No change reported for /test/book.jpg", addedPropertyNames.contains("fooBook"));
        assertTrue("No change reported for /test/book.jpg/jcr:content", addedPropertyNames.contains("fooContent"));
    }

    @Test
    public void bundledNodeAndDiffFew() throws Exception{
        store.dispose();
        disableJournalUse();
        store = builderProvider
                .newBuilder()
                .setDocumentStore(ds)
                .memoryCacheSize(0)
                .getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        //Make some modifications under the bundled node
        //This would cause an entry for bundled node in Commit modified set
        builder = store.getRoot().builder();
        childBuilder(builder, "/test/book.jpg/jcr:content").setProperty("foo", "bar");

        TestNodeObserver o = new TestNodeObserver("test");
        store.addObserver(o);
        merge(builder);
        assertEquals(1, o.changed.size());
    }

    private static void disableJournalUse() {
        System.setProperty(SYS_PROP_DISABLE_JOURNAL, "true");
    }

    private void createTestNode(String path, NodeState state) throws CommitFailedException {
        String parentPath = PathUtils.getParentPath(path);
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder parent = childBuilder(builder, parentPath);
        parent.setChildNode(PathUtils.getName(path), state);
        merge(builder);
    }

    private NodeState getLatestNode(String path){
        return getNode(store.getRoot(), path);
    }

    private boolean hasNodeProperty(String path, String propName) {
        NodeDocument doc = getNodeDocument(path);
        return doc.keySet().contains(propName);
    }

    private NodeDocument getNodeDocument(String path) {
        return ds.find(Collection.NODES, Utils.getIdFromPath(path));
    }

    private NodeState merge(NodeBuilder builder) throws CommitFailedException {
        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static String getBundlingPath(NodeState contentNode) {
        PropertyState ps = contentNode.getProperty(DocumentBundlor.META_PROP_BUNDLING_PATH);
        return checkNotNull(ps).getValue(Type.STRING);
    }

    private static void dump(NodeState state){
        System.out.println(NodeStateUtils.toString(state));
    }

    private static List<String> childNames(NodeState state, String path){
        return copyOf(getNode(state, path).getChildNodeNames());
    }

    static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static class RecordingDocumentStore extends MemoryDocumentStore {
        final List<String> queryPaths = new ArrayList<>();
        final List<String> findPaths = new ArrayList<>();

        @Override
        public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
            if (collection == Collection.NODES){
                findPaths.add(Utils.getPathFromId(key));
            }
            return super.find(collection, key);
        }

        @Nonnull
        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
                                                  String indexedProperty, long startValue, int limit) {
            if (collection == Collection.NODES){
                queryPaths.add(Utils.getPathFromId(Utils.getParentIdFromLowerLimit(fromKey)));
            }
            return super.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        }

        public void reset(){
            queryPaths.clear();
            findPaths.clear();
        }
    }

    private static class AssertingDiff implements NodeStateDiff {
        private final Set<String> ignoredProps = ImmutableSet.of(
                DocumentBundlor.META_PROP_PATTERN,
                META_PROP_BUNDLED_CHILD,
                DocumentBundlor.META_PROP_NON_BUNDLED_CHILD
        );

        public static boolean assertEquals(NodeState before, NodeState after) {
            //Do not rely on default compareAgainstBaseState as that works at lastRev level
            //and we need proper equals
            return before.exists() == after.exists()
                    && AbstractNodeState.compareAgainstBaseState(after, before, new AssertingDiff());
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            if (ignore(after)) return true;
            throw new AssertionError("Added property: " + after);
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (ignore(after)) return true;
            throw new AssertionError("Property changed: Before " + before + " , after " + after);
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            if (ignore(before)) return true;
            throw new AssertionError("Deleted property: " + before);
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            throw new AssertionError(format("Added child: %s -  %s", name, after));
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            return AbstractNodeState.compareAgainstBaseState(after, before, new AssertingDiff());
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            throw new AssertionError(format("Deleted child : %s -  %s", name, before));
        }

        private boolean ignore(PropertyState state){
            return ignoredProps.contains(state.getName());
        }
    }
}
