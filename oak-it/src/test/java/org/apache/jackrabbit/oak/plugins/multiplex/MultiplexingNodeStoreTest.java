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
package org.apache.jackrabbit.oak.plugins.multiplex;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class MultiplexingNodeStoreTest {
    
    private final NodeStoreKind root;
    private final NodeStoreKind mounts;
    
    private final List<NodeStoreRegistration> registrations = Lists.newArrayList();
    
    private MultiplexingNodeStore store;
    private NodeStore globalStore;
    private NodeStore mountedStore;
    private NodeStore deepMountedStore;

    @Parameters(name="Root: {0}, Mounts: {1}")
    public static Collection<Object[]> data() {
        
        return Arrays.asList(new Object[][] { 
            { NodeStoreKind.MEMORY, NodeStoreKind.MEMORY },
            { NodeStoreKind.SEGMENT, NodeStoreKind.SEGMENT},
            { NodeStoreKind.DOCUMENT_H2, NodeStoreKind.DOCUMENT_H2},
            { NodeStoreKind.DOCUMENT_H2, NodeStoreKind.SEGMENT}
        });
    }
    
    public MultiplexingNodeStoreTest(NodeStoreKind root, NodeStoreKind mounts) {
        
        this.root = root;
        this.mounts = mounts;
    }
    
    @Before
    public void initStore() throws Exception {
        
        MountInfoProvider mip = new SimpleMountInfoProvider.Builder()
                .mount("temp", "/tmp")
                .mount("deep", "/libs/mount")
                .build();
        
        globalStore = register(root.create(null));
        mountedStore = register(mounts.create("temp"));
        deepMountedStore = register(mounts.create("deep"));

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
        
        store = new MultiplexingNodeStore.Builder(mip, globalStore)
                .addMount("temp", mountedStore)
                .addMount("deep", deepMountedStore)
                .build();
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
        
        assertThat("root(childCount)", store.getRoot().getChildNodeCount(100), equalTo(3l));
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
    public void childNodeEntryForMountIsMultiplexed() {
        
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
    public void existingBlobsInRootStoreAreRetrieved() throws Exception, IOException {
        
        assumeTrue(root.supportsBlobCreation());
        
        Blob createdBlob = globalStore.createBlob(createLargeBlob());
        Blob retrievedBlob = store.getBlob(createdBlob.getReference());
        
        assertThat(retrievedBlob.getContentIdentity(), equalTo(createdBlob.getContentIdentity()));
    }
    
    
    @Test
    public void existingBlobsInMountedStoreAreRetrieved() throws Exception, IOException {
        
        assumeTrue(mounts.supportsBlobCreation());
        
        Blob createdBlob = mountedStore.createBlob(createLargeBlob());
        Blob retrievedBlob = store.getBlob(createdBlob.getReference());
        
        assertThat(retrievedBlob.getContentIdentity(), equalTo(createdBlob.getContentIdentity()));
    }    
    
    @Test
    public void blobCreation() throws Exception, IOException {
        
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
        
        assertThat("Property must be visible in multiplexed store", 
                store.getRoot().getProperty("newProp").getValue(Type.STRING), equalTo("newValue"));
        
        assertThat("Property must be visible in owning (root) store", 
                globalStore.getRoot().getProperty("newProp").getValue(Type.STRING), equalTo("newValue"));
    }
    
    @Test
    public void removePropertyFromRootStore() throws Exception {
        
        NodeBuilder builder = store.getRoot().builder();
        
        builder.removeProperty("prop");
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertFalse("Property must be removed from multiplexed store", store.getRoot().hasProperty("prop"));
        assertFalse("Property must be removed from owning (root) store", globalStore.getRoot().hasProperty("prop"));
    }
    
    @Test
    public void createNodeInRootStore() throws Exception {
        
        NodeBuilder builder = store.getRoot().builder();
        
        builder.child("newNode");
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue("Node must be added to multiplexed store", store.getRoot().hasChildNode("newNode"));
        assertTrue("Node must be added to owning (root) store", globalStore.getRoot().hasChildNode("newNode"));        
    }
    
    @Test
    public void createNodeInMountedStore() throws Exception {

        NodeBuilder builder = store.getRoot().builder();
        
        builder.getChildNode("tmp").child("newNode");
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue("Node must be added to multiplexed store", store.getRoot().getChildNode("tmp").hasChildNode("newNode"));
        assertTrue("Node must be added to owning (mounted) store", mountedStore.getRoot().getChildNode("tmp").hasChildNode("newNode"));        
    }
    
    @Test
    public void removeNodeInRootStore() throws Exception {

        NodeBuilder builder = store.getRoot().builder();
        
        builder = store.getRoot().builder();
        
        builder.getChildNode("apps").remove();
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Node must be removed from the multiplexed store", store.getRoot().hasChildNode("apps"));
        assertFalse("Node must be removed from the owning (root) store", globalStore.getRoot().hasChildNode("apps"));
    }
    
    
    @Test
    public void removeNodeInMountedStore() throws Exception {

        NodeBuilder builder = store.getRoot().builder();
        
        builder.getChildNode("tmp").child("newNode");
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue("Node must be added to multiplexed store", store.getRoot().getChildNode("tmp").hasChildNode("newNode"));
        
        builder = store.getRoot().builder();
        
        builder.getChildNode("tmp").getChildNode("newNode").remove();
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse("Node must be removed from the multiplexed store", store.getRoot().getChildNode("tmp").hasChildNode("newNode"));
        assertFalse("Node must be removed from the owning (multiplexed) store", globalStore.getRoot().getChildNode("tmp").hasChildNode("newNode"));
    }
    
    @Test
    public void builderChildrenCountInRootStore() throws Exception {
        
        assertThat("root(childCount)", store.getRoot().builder().getChildNodeCount(100), equalTo(3l));
    }
    
    @Test
    public void builderChildrenCountInMountedStore() {
        
        assertThat("root.tmp(childCount)", store.getRoot().builder().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));
    }
    
    @Test
    public void builderChildNodeNamesInRootStore() throws Exception {
        
        assertChildNodeNames(store.getRoot().builder(), "libs", "apps", "tmp");
    }
    
    @Test
    public void builderChildNodeNamesInMountedStore() throws Exception {
        
        assertChildNodeNames(store.getRoot().builder().getChildNode("tmp"), "child1", "child2");
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
        
        builder = store.getRoot().builder();
        
        builder.setChildNode("apps");
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue("Node apps must still exist", store.getRoot().hasChildNode("apps"));
        assertThat("Node apps must not have any properties", store.getRoot().getChildNode("apps").getPropertyCount(), equalTo(0l));
    }
    
    
    @Test
    public void setChildNodeInMountStore() throws Exception {
        
        NodeBuilder builder = store.getRoot().builder();
        
        builder = store.getRoot().builder();
        
        builder.getChildNode("tmp").setChildNode("child1");
        
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue("Node child1 must still exist", store.getRoot().getChildNode("tmp").hasChildNode("child1"));
        assertThat("Node child1 must not have any properties", store.getRoot().getChildNode("tmp").getChildNode("child1").getPropertyCount(), equalTo(0l));
    }    
    
    @Test
    @Ignore("Not implemented")
    public void readOnlyMountRejectsChanges() {
        
    }
    
    @Test
    @Ignore("Not implemented")
    public void builderBasedOnCheckpoint() {
        
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
        },
        SEGMENT {
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
                        
                        store = FileStore.builder(storePath).withBlobStore(blobStore).build();
                        instance = SegmentNodeStore.builder(store).build();

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
        
        for ( int i = 0 ; i <= Segment.MEDIUM_LIMIT; i++) {
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

}