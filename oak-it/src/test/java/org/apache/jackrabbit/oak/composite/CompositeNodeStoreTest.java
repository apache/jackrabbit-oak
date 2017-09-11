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
package org.apache.jackrabbit.oak.composite;

import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.spi.state.ChildNodeEntry.GET_NAME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.util.CountingDiff;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CompositeNodeStoreTest {

    private final NodeStoreKind root;
    private final NodeStoreKind mounts;

    private final List<NodeStoreRegistration> registrations = newArrayList();

    private CompositeNodeStore store;
    private NodeStore globalStore;
    private NodeStore mountedStore;
    private NodeStore deepMountedStore;
    private NodeStore readOnlyStore;
    private MountInfoProvider mip;

    @Parameters(name="Root: {0}, Mounts: {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { NodeStoreKind.MEMORY, NodeStoreKind.MEMORY },
            { NodeStoreKind.SEGMENT, NodeStoreKind.SEGMENT},
            { NodeStoreKind.DOCUMENT_H2, NodeStoreKind.DOCUMENT_H2},
            { NodeStoreKind.DOCUMENT_H2, NodeStoreKind.SEGMENT}
        });
    }

    public CompositeNodeStoreTest(NodeStoreKind root, NodeStoreKind mounts) {
        this.root = root;
        this.mounts = mounts;
    }

    @Before
    public void initStore() throws Exception {
        mip = Mounts.newBuilder()
                .mount("temp", "/tmp")
                .mount("deep", "/libs/mount")
                .mount("empty", "/nowhere")
                .readOnlyMount("readOnly", "/readOnly")
                .build();

        globalStore = register(root.create(null));
        mountedStore = register(mounts.create("temp"));
        deepMountedStore = register(mounts.create("deep"));
        readOnlyStore = register(mounts.create("readOnly"));
        NodeStore emptyStore = register(mounts.create("empty")); // this NodeStore will always be empty

        // create a property on the root node
        NodeBuilder builder = globalStore.getRoot().builder();
        builder.setProperty("prop", "val");
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(globalStore.getRoot().hasProperty("prop"));

        // create a different sub-tree on the root store
        builder = globalStore.getRoot().builder();
        NodeBuilder libsBuilder = builder.child("libs");
        libsBuilder.child("first");
        libsBuilder.child("second");

        // create an empty /apps node with a property
        builder.child("apps").setProperty("prop", "val");

        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertThat(globalStore.getRoot().getChildNodeCount(10), equalTo(2l));

        // create a /tmp child on the mounted store and set a property
        builder = mountedStore.getRoot().builder();
        NodeBuilder tmpBuilder = builder.child("tmp");
        tmpBuilder.setProperty("prop1", "val1");
        tmpBuilder.child("child1").setProperty("prop1", "val1");
        tmpBuilder.child("child2");

        mountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(mountedStore.getRoot().hasChildNode("tmp"));
        assertThat(mountedStore.getRoot().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));

        // populate /libs/mount/third in the deep mount, and include a property

        builder = deepMountedStore.getRoot().builder();
        builder.child("libs").child("mount").child("third").setProperty("mounted", "true");

        deepMountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(deepMountedStore.getRoot().getChildNode("libs").getChildNode("mount").getChildNode("third").hasProperty("mounted"));

        // populate /readonly with a single node
        builder = readOnlyStore.getRoot().builder();
        builder.child("readOnly");

        readOnlyStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // don't use the builder since it would fail due to too many read-write stores
        // but for the purposes of testing the general correctness it's fine
        List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("temp"), mountedStore));
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("deep"), deepMountedStore));
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("empty"), emptyStore));
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("readOnly"), readOnlyStore));
        store = new CompositeNodeStore(mip, globalStore, nonDefaultStores);
    }

    @After
    public void closeRepositories() throws Exception {
        for ( NodeStoreRegistration reg : registrations ) {
            reg.close();
        }
    }

    @Test
    public void rootExists() {
        assertThat("root exists", store.getRoot().exists(), equalTo(true));
    }

    @Test
    public void rootPropertyIsSet() {
        assertThat("root[prop]", store.getRoot().hasProperty("prop"), equalTo(true));
        assertThat("root[prop] = val", store.getRoot().getProperty("prop").getValue(Type.STRING), equalTo("val"));
    }

    @Test
    public void nonMountedChildIsFound() {
        assertThat("root.libs", store.getRoot().hasChildNode("libs"), equalTo(true));
    }

    @Test
    public void nestedMountNodeIsVisible() {
        assertThat("root.libs(childCount)", store.getRoot().getChildNode("libs").getChildNodeCount(10), equalTo(3l));
    }

    @Test
    public void mixedMountsChildNodes() {
        assertThat("root(childCount)", store.getRoot().getChildNodeCount(100), equalTo(4l));
    }

    @Test
    public void mountedChildIsFound() {

        assertThat("root.tmp", store.getRoot().hasChildNode("tmp"), equalTo(true));
    }

    @Test
    public void childrenUnderMountAreFound() {
        assertThat("root.tmp(childCount)", store.getRoot().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));
    }

    @Test
    public void childNodeEntryForMountIsComposite() {
        ChildNodeEntry libsNode = Iterables.find(store.getRoot().getChildNodeEntries(), new Predicate<ChildNodeEntry>() {

            @Override
            public boolean apply(ChildNodeEntry input) {
                return input.getName().equals("libs");
            }
        });

        assertThat("root.libs(childCount)", libsNode.getNodeState().getChildNodeCount(10), equalTo(3l));
    }

    @Test
    public void contentBelongingToAnotherMountIsIgnored() throws Exception {
        // create a /tmp/oops child on the root store
        // these two nodes must be ignored
        NodeBuilder builder = globalStore.getRoot().builder();
        builder.child("tmp").child("oops");

        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(globalStore.getRoot().getChildNode("tmp").hasChildNode("oops"));

        assertFalse(store.getRoot().getChildNode("tmp").hasChildNode("oops"));
    }

    @Test
    public void checkpoint() throws Exception {
        String checkpoint = store.checkpoint(TimeUnit.DAYS.toMillis(1));

        assertNotNull("checkpoint reference is null", checkpoint);

        // create a new child /new in the root store
        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        globalBuilder.child("new");
        globalStore.merge(globalBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create a new child /tmp/new in the mounted store
        NodeBuilder mountedBuilder = mountedStore.getRoot().builder();
        mountedBuilder.getChildNode("tmp").child("new");
        mountedStore.merge(mountedBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create a new child /libs/mount/new in the deeply mounted store
        NodeBuilder deepMountBuilder = deepMountedStore.getRoot().builder();
        deepMountBuilder.getChildNode("libs").getChildNode("mount").child("new");
        deepMountedStore.merge(deepMountBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("store incorrectly exposes child at /new", store.retrieve(checkpoint).hasChildNode("new"));
        assertFalse("store incorrectly exposes child at /tmp/new", store.retrieve(checkpoint).getChildNode("tmp").hasChildNode("new"));
        assertFalse("store incorrectly exposes child at /libs/mount/new", store.retrieve(checkpoint).getChildNode("libs").getChildNode("mount").hasChildNode("new"));
    }

    @Test
    public void checkpointInfo() throws Exception {
        Map<String, String> info = Collections.singletonMap("key", "value");
        String checkpoint = store.checkpoint(TimeUnit.DAYS.toMillis(1), info);

        assertThat(store.checkpointInfo(checkpoint), equalTo(info));
    }

    @Test
    public void release() {
        String checkpoint = store.checkpoint(TimeUnit.DAYS.toMillis(1));

        assertTrue(store.release(checkpoint));
    }

    @Test
    public void existingBlobsInRootStoreAreRetrieved() throws Exception {
        assumeTrue(root.supportsBlobCreation());

        Blob createdBlob = globalStore.createBlob(createLargeBlob());
        Blob retrievedBlob = store.getBlob(createdBlob.getReference());

        assertThat(retrievedBlob.getContentIdentity(), equalTo(createdBlob.getContentIdentity()));
    }


    @Test
    public void existingBlobsInMountedStoreAreRetrieved() throws Exception {

        assumeTrue(mounts.supportsBlobCreation());

        Blob createdBlob = mountedStore.createBlob(createLargeBlob());
        Blob retrievedBlob = store.getBlob(createdBlob.getReference());

        assertThat(retrievedBlob.getContentIdentity(), equalTo(createdBlob.getContentIdentity()));
    }

    @Test
    public void blobCreation() throws Exception {
        assumeTrue(root.supportsBlobCreation());

        Blob createdBlob = store.createBlob(createLargeBlob());
        Blob retrievedBlob = store.getBlob(createdBlob.getReference());

        assertThat(retrievedBlob.getContentIdentity(), equalTo(createdBlob.getContentIdentity()));
    }

    @Test
    public void setPropertyOnRootStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.setProperty("newProp", "newValue");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertThat("Property must be visible in composite store",
                store.getRoot().getProperty("newProp").getValue(Type.STRING), equalTo("newValue"));

        assertThat("Property must be visible in owning (root) store",
                globalStore.getRoot().getProperty("newProp").getValue(Type.STRING), equalTo("newValue"));
    }

    @Test
    public void removePropertyFromRootStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.removeProperty("prop");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Property must be removed from composite store", store.getRoot().hasProperty("prop"));
        assertFalse("Property must be removed from owning (root) store", globalStore.getRoot().hasProperty("prop"));
    }

    @Test
    public void createNodeInRootStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.child("newNode");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Node must be added to composite store", store.getRoot().hasChildNode("newNode"));
        assertTrue("Node must be added to owning (root) store", globalStore.getRoot().hasChildNode("newNode"));
    }

    @Test
    public void createNodeInMountedStore() throws Exception {

        NodeBuilder builder = store.getRoot().builder();

        builder.getChildNode("tmp").child("newNode");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Node must be added to composite store", store.getRoot().getChildNode("tmp").hasChildNode("newNode"));
        assertTrue("Node must be added to owning (mounted) store", mountedStore.getRoot().getChildNode("tmp").hasChildNode("newNode"));
    }

    @Test
    public void removeNodeInRootStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.getChildNode("apps").remove();

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Node must be removed from the composite store", store.getRoot().hasChildNode("apps"));
        assertFalse("Node must be removed from the owning (root) store", globalStore.getRoot().hasChildNode("apps"));
    }


    @Test
    public void removeNodeInMountedStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.getChildNode("tmp").child("newNode");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Node must be added to composite store", store.getRoot().getChildNode("tmp").hasChildNode("newNode"));

        builder = store.getRoot().builder();

        builder.getChildNode("tmp").getChildNode("newNode").remove();

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Node must be removed from the composite store", store.getRoot().getChildNode("tmp").hasChildNode("newNode"));
        assertFalse("Node must be removed from the owning (composite) store", globalStore.getRoot().getChildNode("tmp").hasChildNode("newNode"));
    }

    @Test
    public void builderChildrenCountInRootStore() throws Exception {
        assertThat("root(childCount)", store.getRoot().builder().getChildNodeCount(100), equalTo(4l));
    }

    @Test
    public void builderChildrenCountInMountedStore() {
        assertThat("root.tmp(childCount)", store.getRoot().builder().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));
    }

    @Test
    public void builderChildNodeNamesInRootStore() throws Exception {
        assertChildNodeNames(store.getRoot().builder(), "libs", "apps", "tmp", "readOnly");
    }

    @Test
    public void builderChildNodeNamesInMountedStore() throws Exception {
        assertChildNodeNames(store.getRoot().builder().getChildNode("tmp"), "child1", "child2");
    }

    @Test
    public void builderStateIsUpdatedBeforeMergeinGlobalStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("newChild");

        assertTrue("Newly created node should be visible in the builder's node state", builder.hasChildNode("newChild"));
    }

    @Test
    public void builderStateIsUpdatedBeforeMergeinMountedStore() throws Exception {
        NodeBuilder builder = store.getRoot().getChildNode("tmp").builder();
        builder.child("newChild");

        assertTrue("Newly created node should be visible in the builder's node state", builder.hasChildNode("newChild"));
    }


    @Test
    public void builderHasPropertyNameInRootStore() {
        assertFalse("Node 'nope' does not exist", store.getRoot().builder().hasChildNode("nope"));
        assertTrue("Node 'tmp' should exist (contributed by mount)", store.getRoot().builder().hasChildNode("tmp"));
        assertTrue("Node 'libs' should exist (contributed by root)", store.getRoot().builder().hasChildNode("libs"));
    }

    @Test
    public void builderHasPropertyNameInMountedStore() {
        assertFalse("Node 'nope' does not exist", store.getRoot().builder().getChildNode("tmp").hasChildNode("nope"));
        assertTrue("Node 'child1' should exist", store.getRoot().builder().getChildNode("tmp").hasChildNode("child1"));
    }

    @Test
    public void setChildNodeInRootStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.setChildNode("apps");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Node apps must still exist", store.getRoot().hasChildNode("apps"));
        assertThat("Node apps must not have any properties", store.getRoot().getChildNode("apps").getPropertyCount(), equalTo(0l));
    }


    @Test
    public void setChildNodeInMountStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        builder.getChildNode("tmp").setChildNode("child1");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue("Node child1 must still exist", store.getRoot().getChildNode("tmp").hasChildNode("child1"));
        assertThat("Node child1 must not have any properties", store.getRoot().getChildNode("tmp").getChildNode("child1").getPropertyCount(), equalTo(0l));
    }


    @Test
    public void builderBasedOnRootStoreChildNode() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder appsBuilder = builder.getChildNode("apps");

        appsBuilder.removeProperty("prop");
        appsBuilder.setChildNode("child1");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Node apps must have no properties (composite store)", store.getRoot().getChildNode("apps").hasProperty("prop"));
        assertFalse("Node apps must have no properties (root store)", globalStore.getRoot().getChildNode("apps").hasProperty("prop"));

        assertTrue("Node /apps/child1 must exist (composite store)", store.getRoot().getChildNode("apps").hasChildNode("child1"));
        assertTrue("Node /apps/child1 must exist (root store)", globalStore.getRoot().getChildNode("apps").hasChildNode("child1"));
    }

    @Test
    public void builderBasedOnMountStoreChildNode() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder tmpBuilder = builder.getChildNode("tmp");

        tmpBuilder.removeProperty("prop1");
        tmpBuilder.setChildNode("child3");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Node tmp must have no properties (composite store)", store.getRoot().getChildNode("tmp").hasProperty("prop1"));
        assertFalse("Node tmp must have no properties (mounted store)", mountedStore.getRoot().getChildNode("tmp").hasProperty("prop1"));

        assertTrue("Node /tmp/build3 must exist (composite store)", store.getRoot().getChildNode("tmp").hasChildNode("child3"));
        assertTrue("Node /tmp/child3 must exist (mounted store)", mountedStore.getRoot().getChildNode("tmp").hasChildNode("child3"));

    }

    @Test
    public void freshBuilderForGlobalStore() {
        NodeBuilder builder = store.getRoot().builder();

        assertFalse("builder.isNew", builder.isNew());
        assertFalse("builder.isModified", builder.isModified());
        assertFalse("builder.isReplaced", builder.isReplaced());
    }

    @Test
    public void freshBuilderForMountedStore() {
        NodeBuilder builder = store.getRoot().getChildNode("tmp").builder();

        assertFalse("builder.isNew", builder.isNew());
        assertFalse("builder.isModified", builder.isModified());
        assertFalse("builder.isReplaced", builder.isReplaced());
    }

    @Test
    public void newBuilderForGlobalStore() {
        NodeBuilder builder = store.getRoot().builder();

        builder = builder.child("newChild");

        assertTrue("builder.isNew", builder.isNew());
        assertFalse("builder.isModified", builder.isModified());
        assertFalse("builder.isReplaced", builder.isReplaced());
    }

    @Test
    public void newBuilderForMountedStore() {
        NodeBuilder builder = store.getRoot().getChildNode("tmp").builder();

        builder = builder.child("newChild");

        assertTrue("builder.isNew", builder.isNew());
        assertFalse("builder.isModified", builder.isModified());
        assertFalse("builder.isReplaced", builder.isReplaced());
    }

    @Test
    public void replacedBuilderForGlobalStore() {
        NodeBuilder builder = store.getRoot().builder();

        NodeBuilder libsBuilder = builder.setChildNode("libs");

        assertTrue("libsBuilder.isReplaced", libsBuilder.isReplaced());
        assertTrue("builder.getChild('libs').isReplaced", builder.getChildNode("libs").isReplaced());
    }

    @Test
    public void replacedBuilderForMountedStore() {
        NodeBuilder builder = store.getRoot().getChildNode("tmp").builder();

        builder = builder.setChildNode("child1");

        assertTrue("builder.isReplaced", builder.isReplaced());
    }

    @Test
    public void readChildNodeBasedOnPathFragment() throws Exception {
        NodeBuilder builder = globalStore.getRoot().builder();

        builder.child("multi-holder");

        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = mountedStore.getRoot().builder();

        builder.child("multi-holder").child("oak:mount-temp").setProperty("prop", "val");

        mountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState holderNode = store.getRoot().getChildNode("multi-holder");
        assertTrue("/multi-holder/oak:mount-temp should be visible from the composite store",
                holderNode.hasChildNode("oak:mount-temp"));

        assertChildNodeNames(holderNode, "oak:mount-temp");

        assertThat("/multi-holder/ must have 1 child entry", holderNode.getChildNodeCount(10), equalTo(1l));
    }

    @Test
    public void moveNodeInSameStore() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        NodeBuilder src = builder.child("src");
        NodeBuilder dst = builder.child("dst");

        boolean result = src.moveTo(dst, "src");
        assertTrue("move result should be success", result);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("/src must no longer exist", store.getRoot().hasChildNode("src"));
        assertTrue("/dst/src must exist (composite store)", store.getRoot().getChildNode("dst").hasChildNode("src"));
    }

    @Test
    @Ignore("Test ignored, since only the default store is writeable")
    public void moveNodeBetweenStores() throws Exception {
        NodeBuilder builder = store.getRoot().builder();

        NodeBuilder src = builder.child("src");
        NodeBuilder dst = builder.child("tmp");

        boolean result = src.moveTo(dst, "src");
        assertTrue("move result should be success", result);

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("/src must no longer exist", store.getRoot().hasChildNode("src"));
        assertTrue("/tmp/src must exist (composite store)", store.getRoot().getChildNode("tmp").hasChildNode("src"));

    }

    @Test
    public void resetOnGlobalStore() {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("newChild");

        store.reset(builder);

        assertFalse("Newly added child should no longer be visible after reset", builder.hasChildNode("newChild"));
    }

    @Test
    public void resetOnMountedStore() {
        NodeBuilder rootBuilder = store.getRoot().builder();
        NodeBuilder builder = rootBuilder.getChildNode("tmp");
        builder.child("newChild");

        store.reset(rootBuilder);

        assertFalse("Newly added child should no longer be visible after reset", builder.getChildNode("tmp").hasChildNode("newChild"));
    }

    @Test
    public void oldNodeStateDoesNotRefreshOnGlobalStore() throws Exception {
        NodeState old = store.getRoot();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("newNode");

        assertFalse("old NodeState should not see newly added child node before merge ", old.hasChildNode("newNode"));

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("old NodeState should not see newly added child node after merge ", old.hasChildNode("newNode"));
    }

    @Test
    public void oldNodeStateDoesNotRefreshOnMountedStore() throws Exception {
        NodeState old = store.getRoot();

        NodeBuilder builder = store.getRoot().builder();

        builder.getChildNode("tmp").child("newNode");

        assertFalse("old NodeState should not see newly added child node before merge ", old.getChildNode("tmp").hasChildNode("newNode"));

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("old NodeState should not see newly added child node after merge ", old.getChildNode("tmp").hasChildNode("newNode"));
    }

    // this test ensures that when going from State -> Builder -> State -> Builder the state is properly maintained
    @Test
    public void nestedBuilderFromState() throws Exception {
        NodeState rootState = store.getRoot();
        NodeBuilder rootBuilder = rootState.builder();
        rootBuilder.child("newNode");

        NodeState baseState = rootBuilder.getNodeState();
        NodeBuilder builderFromState = baseState.builder();

        assertTrue(builderFromState.hasChildNode("newNode"));
    }

    @Test
    public void nestedBuilderWithNewPropertyFromState() throws Exception {
        NodeState rootState = store.getRoot();
        NodeBuilder rootBuilder = rootState.builder();
        rootBuilder.setProperty("newProperty", true, Type.BOOLEAN);

        NodeState baseState = rootBuilder.getNodeState();
        assertTrue(baseState.getBoolean("newProperty"));

        NodeBuilder builderFromState = baseState.builder();
        assertTrue(builderFromState.getBoolean("newProperty"));
        assertTrue(builderFromState.getNodeState().getBoolean("newProperty"));
        //assertTrue(builderFromState.getBaseState().getBoolean("newProperty")); // FIXME
    }

    @Test (expected = UnsupportedOperationException.class)
    public void readOnlyMountRejectsChanges() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.getChildNode("readOnly").child("newChild");
    }

    @Test
    public void builderBasedOnCheckpoint() throws CommitFailedException {
        String checkpoint = store.checkpoint(TimeUnit.DAYS.toMillis(1));

        assertNotNull("checkpoint reference is null", checkpoint);

        // create a new child /new in the root store
        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        globalBuilder.child("new");
        globalStore.merge(globalBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create a new child /tmp/new in the mounted store
        NodeBuilder mountedBuilder = mountedStore.getRoot().builder();
        mountedBuilder.getChildNode("tmp").child("new");
        mountedStore.merge(mountedBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create a new child /libs/mount/new in the deeply mounted store
        NodeBuilder deepMountBuilder = deepMountedStore.getRoot().builder();
        deepMountBuilder.getChildNode("libs").getChildNode("mount").child("new");
        deepMountedStore.merge(deepMountBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder rootCheckpointBuilder = store.retrieve(checkpoint).builder();
        assertFalse("store incorrectly exposes child at /new", rootCheckpointBuilder.hasChildNode("new"));
        assertFalse("store incorrectly exposes child at /tmp/new", rootCheckpointBuilder.getChildNode("tmp").hasChildNode("new"));
        assertFalse("store incorrectly exposes child at /libs/mount/new", rootCheckpointBuilder.getChildNode("libs").getChildNode("mount").hasChildNode("new"));
    }

    @Test
    public void duplicatedChildren() throws CommitFailedException {
        // create a new child /new in the root store
        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        globalBuilder.child("new").setProperty("store", "global", Type.STRING);
        globalStore.merge(globalBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create a new child /tmp/new in the mounted store
        NodeBuilder mountedBuilder = mountedStore.getRoot().builder();
        mountedBuilder.child("new").setProperty("store", "mounted", Type.STRING);
        mountedStore.merge(mountedBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // create a new child /libs/mount/new in the deeply mounted store
        NodeBuilder deepMountBuilder = deepMountedStore.getRoot().builder();
        deepMountBuilder.child("new").setProperty("store", "deepMounted", Type.STRING);
        deepMountedStore.merge(deepMountBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        List<ChildNodeEntry> children = newArrayList(filter(store.getRoot().getChildNodeEntries(), compose(Predicates.equalTo("new"), GET_NAME)));
        assertEquals(1, children.size());
        assertEquals("global", children.get(0).getNodeState().getString("store"));

        NodeBuilder rootBuilder = store.getRoot().builder();
        List<String> childNames = newArrayList(filter(rootBuilder.getChildNodeNames(), Predicates.equalTo("new")));
        assertEquals(1, childNames.size());
        assertEquals("global", rootBuilder.getChildNode("new").getString("store"));
    }

    @Test
    public void propertyIndex() throws Exception{
        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        createIndexDefinition(globalBuilder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new PropertyIndexEditorProvider().with(mip)));

        globalStore.merge(globalBuilder, hook, CommitInfo.EMPTY);

        NodeBuilder builder = store.getRoot().builder();
        builder.child("content").setProperty("foo", "bar");
        store.merge(builder, hook, CommitInfo.EMPTY);

        builder = store.getRoot().builder();
        builder.child("content").removeProperty("foo");
        store.merge(builder, hook, CommitInfo.EMPTY);
    }

    private static enum NodeStoreKind {
        MEMORY {
            @Override
            public NodeStoreRegistration create(String name) {
                return new NodeStoreRegistration() {

                    private MemoryNodeStore instance;

                    @Override
                    public NodeStore get() {

                        if (instance != null) {
                            throw new IllegalStateException("instance already created");
                        }

                        instance = new MemoryNodeStore();

                        return instance;
                    }

                    @Override
                    public void close() throws Exception {
                        // does nothing

                    }
                };
            }

            public boolean supportsBlobCreation() {
                return false;
            }
        }, SEGMENT {
            @Override
            public NodeStoreRegistration create(final String name) {
                return new NodeStoreRegistration() {

                    private SegmentNodeStore instance;
                    private FileStore store;
                    private File storePath;
                    private String blobStorePath;

                    @Override
                    public NodeStore get() throws Exception {

                        if (instance != null) {
                            throw new IllegalStateException("instance already created");
                        }

                        // TODO - don't use Unix directory separators
                        String directoryName = name != null ? "segment-" + name : "segment";
                        storePath = new File("target/classes/" + directoryName);

                        String blobStoreDirectoryName = name != null ? "blob-" + name : "blob";
                        blobStorePath = "target/classes/" + blobStoreDirectoryName;

                        BlobStore blobStore = new FileBlobStore(blobStorePath);

                        store = FileStoreBuilder.fileStoreBuilder(storePath).withBlobStore(blobStore).build();
                        instance = SegmentNodeStoreBuilders.builder(store).build();

                        return instance;
                    }

                    @Override
                    public void close() throws Exception {
                        store.close();

                        FileUtils.deleteQuietly(storePath);
                        FileUtils.deleteQuietly(new File(blobStorePath));
                    }
                };
            }
        }, DOCUMENT_H2 {

            // TODO - copied from DocumentRdbFixture

            private DataSource ds;

            @Override
            public NodeStoreRegistration create(final String name) {

                return new NodeStoreRegistration() {

                    private DocumentNodeStore instance;

                    @Override
                    public NodeStore get() throws Exception {
                        RDBOptions options = new RDBOptions().dropTablesOnClose(true);
                        String jdbcUrl = "jdbc:h2:file:./target/classes/document";
                        if ( name != null ) {
                            jdbcUrl += "-" + name;
                        }
                        ds = RDBDataSourceFactory.forJdbcUrl(jdbcUrl, "sa", "");

                        instance = new DocumentMK.Builder().setRDBConnection(ds, options).getNodeStore();

                        return instance;

                    }

                    @Override
                    public void close() throws Exception {
                        instance.dispose();
                        if ( ds instanceof Closeable ) {
                            ((Closeable) ds).close();
                        }
                    }

                };

            }
        };

        public abstract NodeStoreRegistration create(@Nullable String name);

        public boolean supportsBlobCreation() {
            return true;
        }
    }

    private interface NodeStoreRegistration {
        NodeStore get() throws Exception;

        void close() throws Exception;
    }

    private NodeStore register(NodeStoreRegistration reg) throws Exception {
        registrations.add(reg);

        return reg.get();
    }

    // ensure blobs don't get inlined by the SegmentBlobStore
    private ByteArrayInputStream createLargeBlob() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        for (int i = 0; i <= SegmentTestConstants.MEDIUM_LIMIT; i++) {
            out.write('a');
        }

        return new ByteArrayInputStream(out.toByteArray());
    }

    private void assertChildNodeNames(NodeBuilder builder, String... names) {
        Iterable<String> childNodeNames = builder.getChildNodeNames();

        assertNotNull("childNodeNames must not be empty", childNodeNames);
        assertThat("Incorrect number of elements", Iterables.size(childNodeNames), equalTo(names.length));
        assertThat("Mismatched elements", childNodeNames, hasItems(names));
    }

    private void assertChildNodeNames(NodeState state, String... names) {
        Iterable<String> childNodeNames = state.getChildNodeNames();

        assertNotNull("childNodeNames must not be empty", childNodeNames);
        assertThat("Incorrect number of elements", Iterables.size(childNodeNames), equalTo(names.length));
        assertThat("Mismatched elements", childNodeNames, hasItems(names));
    }
}
