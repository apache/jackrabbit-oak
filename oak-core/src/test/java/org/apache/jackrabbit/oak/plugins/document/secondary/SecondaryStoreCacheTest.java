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
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer.DEFAULT_DIFFER;
import static org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreObserverTest.create;
import static org.apache.jackrabbit.oak.plugins.document.secondary.SecondaryStoreObserverTest.documentState;
import static org.junit.Assert.assertEquals;
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

    private SecondaryStoreCache createCache(PathFilter pathFilter){
        SecondaryStoreCache cache = new SecondaryStoreCache(secondary, pathFilter, DEFAULT_DIFFER);
        SecondaryStoreObserver observer = new SecondaryStoreObserver(secondary, pathFilter, cache,
                DEFAULT_DIFFER, StatisticsProvider.NOOP);
        primary.addObserver(observer);

        return cache;
    }

    private AbstractDocumentNodeState merge(NodeBuilder nb) throws CommitFailedException {
        return (AbstractDocumentNodeState) primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}