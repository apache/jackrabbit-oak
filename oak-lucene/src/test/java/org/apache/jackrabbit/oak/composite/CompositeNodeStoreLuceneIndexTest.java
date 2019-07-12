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


import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static  org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLuceneIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings("ConstantConditions")
public class CompositeNodeStoreLuceneIndexTest extends CompositeNodeStoreQueryTestBase{

    private NodeStore readOnlyStoreV1;
    private NodeStore readOnlyStoreV2;

    private CompositeNodeStore storeV1;
    private CompositeNodeStore storeV2;

    private MountInfoProvider mipV1;
    private MountInfoProvider mipV2;

    public CompositeNodeStoreLuceneIndexTest(NodeStoreKind root, NodeStoreKind mounts) {
        super(root, mounts);
    }

    @Override
    @Before
    public void initStore() throws Exception {
        globalStore = register(nodeStoreRoot.create(null));
        readOnlyStoreV1 = register(mounts.create("readOnlyv1"));
        readOnlyStoreV2 = register(mounts.create("readOnlyv2"));

        // populate readOnlyStoreV1 (This will be part of V1 of the app/repository)
        NodeBuilder builder = readOnlyStoreV1.getRoot().builder();
        new InitialContent().initialize(builder);
        builder.child("libs");

        // Now populate readOnlyStoreV2 (This will be part of V2 of the app/repository)
        NodeBuilder builder2 = readOnlyStoreV2.getRoot().builder();
        new InitialContent().initialize(builder);
        builder2.child("libs");


        readOnlyStoreV1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        readOnlyStoreV2.merge(builder2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        initMounts();

    }

    void initMounts() throws LoginException, NoSuchWorkspaceException {

        mipV1 = Mounts.newBuilder().readOnlyMount("readOnlyv1", "/libs").build();
        List<MountedNodeStore> nonDefaultStores = Lists.newArrayList();
        nonDefaultStores.add(new MountedNodeStore(mipV1.getMountByName("readOnlyv1"), readOnlyStoreV1));
        // This will be V1
        storeV1 = new CompositeNodeStore(mipV1, globalStore, nonDefaultStores);


        mipV2 = Mounts.newBuilder().readOnlyMount("readOnlyv2", "/libs").build();
        List<MountedNodeStore> nonDefaultStores2 = Lists.newArrayList();
        nonDefaultStores2.add(new MountedNodeStore(mipV2.getMountByName("readOnlyv2"), readOnlyStoreV2));
        storeV2 = new CompositeNodeStore(mipV2, globalStore, nonDefaultStores2);

        session = createRepository(storeV1, mipV1).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }
    /*
    Given an index defintion exists in application V1
    Remove it in application V2
    Make sure results are returned till all instances of V1 are down and V2 application is up
     */
    @Test
    public void removeIndexDefinition1() throws Exception {

        // Add the V1 store to lb mock for now
        CompositeStoreLB lb = new CompositeStoreLB();
        lb.addStoreToLB(storeV1);

        lb.setActiveNodeStore(storeV1);

        LuceneIndexEditorProvider iep = new LuceneIndexEditorProvider(indexCopier, indexTracker, null, null, mipV1);
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(iep, "async", false));


        // Now Add Index def to both read only (V1) and read-write(global/shared) parts

        NodeBuilder readOnlyBuilderV1 = readOnlyStoreV1.getRoot().builder();
        NodeBuilder index = readOnlyBuilderV1.child(INDEX_DEFINITIONS_NAME);
        index = newLucenePropertyIndexDefinition(index, "luceneTest", ImmutableSet.of("foo"), "async");
        index.setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/luceneTest/@v1");

        // Add some content to read-only v1 repo
        for (int i = 0; i < 3; i++) {
            NodeBuilder b = readOnlyBuilderV1.child("libs").child("node-" + i);
            b.setProperty("foo", "bar");
            b.setProperty("jcr:primaryType", "nt:base", Type.NAME);
        }

        // Add /libs/indexes/luceneTest/@v1 to make this index usable

        readOnlyBuilderV1.child("libs").child("indexes").child("luceneTest").setProperty("v1", true);


        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        index = globalBuilder.child(INDEX_DEFINITIONS_NAME);
        index = newLucenePropertyIndexDefinition(index, "luceneTest", ImmutableSet.of("foo"), "async");
        index.setProperty(IndexConstants.USE_IF_EXISTS, "/libs/indexes/luceneTest/@v1");

        // Add some content to read write now
        NodeBuilder builder;
        builder = storeV1.getRoot().builder();
        for (int i = 0; i < 2; i++) {
            NodeBuilder b = builder.child("content").child("node-" + i);
            b.setProperty("foo", "bar");
            b.setProperty("jcr:primaryType", "nt:base", Type.NAME);
        }

        readOnlyStoreV1.merge(readOnlyBuilderV1, hook, CommitInfo.EMPTY);
        globalStore.merge(globalBuilder, hook, CommitInfo.EMPTY);
        storeV1.merge(builder, hook, CommitInfo.EMPTY);
        root.commit();
        indexTracker.update(readOnlyStoreV1.getRoot());
        indexTracker.update(globalStore.getRoot());

        // Now run a query to see if data is returned from both the stores
        // need to login again to see changes in the read-only area
        session = createRepository(lb.getActiveNodeStore(), mipV1).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        indexTracker.update(lb.getActiveNodeStore().getRoot());

        assertThat(
                executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath", false).toString(),
                containsString("/* lucene:luceneTest(/oak:index/luceneTest) foo:bar"));
        assertEquals("[/content/node-0, /content/node-1, " +
                        "/libs/node-0, /libs/node-1, /libs/node-2]",
                executeQuery("/jcr:root//*[@foo = 'bar']", "xpath").toString());


        // ****This point we have V1 of our app with index properly configured in read only V1 and and global read write****

        // Now we setup the new instance V2

        // storeV2 at this point is configured to use globalStore that has luceneV1 and readOnlyStoreV2 without this index
        // Since storeV2 is ready - adding this to LB

        lb.addStoreToLB(storeV2);
        lb.setActiveNodeStore(storeV2);  // This can easily be dodne without this lb thing - but this is just a try to demonstrate how things would work at a high level

        // login again now using the new instance
        session = createRepository(lb.getActiveNodeStore(), mipV1).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        indexTracker.update(lb.getActiveNodeStore().getRoot());

        // Execute Query and see that the index is now not used .
        assertThat(
                executeQuery("explain /jcr:root//*[@foo = 'bar']", "xpath", false).toString(),
                containsString("/* traverse"));
        assertEquals("[/content/node-0, /content/node-1",
                executeQuery("/jcr:root//*[@foo = 'bar']", "xpath").toString());

        // Now we would effectively remove the V1 instance from lb
        lb.removeStoreFromLB(storeV1);

        // After upgrade -- now we will remove the index from global read-write store as well - just documenting the step - nothing
        // specific to test now .

    }
}
