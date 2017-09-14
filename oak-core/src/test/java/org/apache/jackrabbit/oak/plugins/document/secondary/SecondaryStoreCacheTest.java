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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundledTypesRegistry;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreObserverTest.create;
import static org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreObserverTest.documentState;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SecondaryStoreCacheTest {
    private final List<String> empty = Collections.emptyList();
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore primary;
    private NodeStore secondary;

    @Before
    public void setUp() throws IOException {
        primary = builderProvider.newBuilder().getNodeStore();
        secondary = new MemoryNodeStore();
    }

    @Test
    public void basicTest() throws Exception{
        SecondaryStoreCache cache = createCache(new PathFilter(of("/a"), empty));

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        merge(nb);

        RevisionVector rv1 = new RevisionVector(new Revision(1,0,1));
        RevisionVector rv2 = new RevisionVector(new Revision(1,0,3));
        assertNull(cache.getDocumentNodeState("/a/b", rv1, rv2));
        assertNull(cache.getDocumentNodeState("/x", rv1, rv2));
    }

    @Test
    public void updateAndReadAtReadRev() throws Exception{
        SecondaryStoreCache cache = createCache(new PathFilter(of("/a"), empty));

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        AbstractDocumentNodeState r1 = merge(nb);

        //Update some other part of tree i.e. which does not change lastRev for /a/c
        nb = primary.getRoot().builder();
        create(nb, "/a/e/d");
        AbstractDocumentNodeState r2 = merge(nb);

        //Lookup should work fine
        AbstractDocumentNodeState a_r2 = documentState(r2, "/a/c");
        AbstractDocumentNodeState result
                = cache.getDocumentNodeState("/a/c", r2.getRootRevision(), a_r2.getLastRevision());
        assertTrue(EqualsDiff.equals(a_r2, result));

        //Child docs should only have lastRev and not root rev
        assertTrue(result.hasProperty(DelegatingDocumentNodeState.PROP_LAST_REV));
        assertFalse(result.hasProperty(DelegatingDocumentNodeState.PROP_REVISION));

        //Root doc would have both meta props
        assertTrue(secondary.getRoot().hasProperty(DelegatingDocumentNodeState.PROP_LAST_REV));
        assertTrue(secondary.getRoot().hasProperty(DelegatingDocumentNodeState.PROP_REVISION));

        nb = primary.getRoot().builder();
        nb.child("a").child("c").remove();
        AbstractDocumentNodeState r3 = merge(nb);

        //Now look from older revision
        result = cache.getDocumentNodeState("/a/c", r3.getRootRevision(), a_r2.getLastRevision());

        //now as its not visible from head it would not be visible
        assertNull(result);
    }

    @Test
    public void updateAndReadAtPrevRevision() throws Exception {
        SecondaryStoreCache cache = createCache(new PathFilter(of("/a"), empty));

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c");
        AbstractDocumentNodeState r0 = merge(nb);
        AbstractDocumentNodeState a_c_0 = documentState(primary.getRoot(), "/a/c");

        //Update some other part of tree i.e. which does not change lastRev for /a/c
        nb = primary.getRoot().builder();
        create(nb, "/a/c/d");
        AbstractDocumentNodeState r1 = merge(nb);
        AbstractDocumentNodeState a_c_1 = documentState(primary.getRoot(), "/a/c");

        AbstractDocumentNodeState result
                = cache.getDocumentNodeState("/a/c", r1.getRootRevision(), a_c_1.getLastRevision());
        assertTrue(EqualsDiff.equals(a_c_1, result));

        //Read from older revision
        result = cache.getDocumentNodeState("/a/c", r0.getRootRevision(), a_c_0.getLastRevision());
        assertTrue(EqualsDiff.equals(a_c_0, result));
    }

    @Test
    public void binarySearch() throws Exception{
        SecondaryStoreCache cache = createCache(new PathFilter(of("/a"), empty));

        List<AbstractDocumentNodeState> roots = Lists.newArrayList();
        List<RevisionVector> revs = Lists.newArrayList();
        for (int i = 0; i < 50; i++) {
            NodeBuilder nb = primary.getRoot().builder();
            create(nb, "/a/b"+i);
            AbstractDocumentNodeState r = merge(nb);
            roots.add(r);
            revs.add(r.getRootRevision());
        }

        AbstractDocumentNodeState[] rootsArr = Iterables.toArray(roots, AbstractDocumentNodeState.class);

        Collections.shuffle(revs);
        for (RevisionVector rev : revs){
            AbstractDocumentNodeState result = SecondaryStoreCache.findMatchingRoot(rootsArr, rev);
            assertNotNull(result);
            assertEquals(rev, result.getRootRevision());
        }

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/m");
        AbstractDocumentNodeState r = merge(nb);
        AbstractDocumentNodeState result = SecondaryStoreCache.findMatchingRoot(rootsArr, r.getRootRevision());
        assertNull(result);

    }

    @Test
    public void readWithSecondaryLagging() throws Exception{
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreCache cache = createBuilder(pathFilter).buildCache();
        SecondaryStoreObserver observer = createBuilder(pathFilter).buildObserver(cache);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c");
        AbstractDocumentNodeState r0 = merge(nb);
        AbstractDocumentNodeState a_c_0 = documentState(primary.getRoot(), "/a/c");

        observer.contentChanged(r0, CommitInfo.EMPTY);

        AbstractDocumentNodeState result = cache.getDocumentNodeState("/a/c", r0.getRootRevision(), a_c_0
                .getLastRevision());
        assertTrue(EqualsDiff.equals(a_c_0, result));

        //Make change in some other part of tree i.e. /a/c is unmodified
        nb = primary.getRoot().builder();
        create(nb, "/a/e");
        AbstractDocumentNodeState r1 = merge(nb);

        //Change is yet not pushed to secondary i.e. observer not invoked
        //but lookup with latest root should still work fine if lastRev matches
        result = cache.getDocumentNodeState("/a/c", r1.getRootRevision(), a_c_0
                .getLastRevision());
        assertTrue(EqualsDiff.equals(a_c_0, result));

        //Change which is not pushed would though not be visible
        AbstractDocumentNodeState a_e_1 = documentState(primary.getRoot(), "/a/e");
        result = cache.getDocumentNodeState("/a/e", r1.getRootRevision(), a_e_1
                .getLastRevision());
        assertNull(result);
    }

    @Test
    public void isCached() throws Exception{
        SecondaryStoreCache cache = createCache(new PathFilter(of("/a"), empty));

        assertTrue(cache.isCached("/a"));
        assertTrue(cache.isCached("/a/b"));
        assertFalse(cache.isCached("/x"));
    }

    @Test
    public void bundledNodes() throws Exception{
        SecondaryStoreCache cache = createCache(new PathFilter(of("/"), empty));
        primary.setNodeStateCache(cache);

        NodeBuilder builder = primary.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        merge(builder);

        BundledTypesRegistry registry = BundledTypesRegistry.from(NodeStateUtils.getNode(primary.getRoot(),
                BundlingConfigHandler.CONFIG_PATH));
        assertNotNull("DocumentBundling not found to be enabled for nt:file",
                registry.getBundlor(newNode("nt:file").getNodeState()));

        //1. Create a file node
        builder = primary.getRoot().builder();
        NodeBuilder fileNode = newNode("nt:file");
        fileNode.child("jcr:content").setProperty("jcr:data", "foo");
        builder.child("test").setChildNode("book.jpg", fileNode.getNodeState());
        merge(builder);

        //2. Assert that bundling is working
        assertNull(getNodeDocument("/test/book.jpg/jcr:content"));

        //3. Now update the file node
        builder = primary.getRoot().builder();
        builder.getChildNode("test").getChildNode("book.jpg").getChildNode("jcr:content").setProperty("foo", "bar");
        merge(builder);
    }

    private SecondaryStoreCache createCache(PathFilter pathFilter){
        SecondaryStoreBuilder builder = createBuilder(pathFilter);
        builder.metaPropNames(DocumentNodeStore.META_PROP_NAMES);
        SecondaryStoreCache cache = builder.buildCache();
        SecondaryStoreObserver observer = builder.buildObserver(cache);
        primary.addObserver(observer);

        return cache;
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private NodeDocument getNodeDocument(String path) {
        return primary.getDocumentStore().find(Collection.NODES, Utils.getIdFromPath(path));
    }

    private SecondaryStoreBuilder createBuilder(PathFilter pathFilter) {
        return new SecondaryStoreBuilder(secondary).pathFilter(pathFilter);
    }

    private AbstractDocumentNodeState merge(NodeBuilder nb) throws CommitFailedException {
        return (AbstractDocumentNodeState) primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}