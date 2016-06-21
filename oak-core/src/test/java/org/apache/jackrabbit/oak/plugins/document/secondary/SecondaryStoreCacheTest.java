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

import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache.NodeStateCacheEntry;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
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
import static org.junit.Assert.*;

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
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreObserver observer = new SecondaryStoreObserver(secondary, pathFilter, DEFAULT_DIFFER);
        primary.addObserver(observer);

        SecondaryStoreCache cache = new SecondaryStoreCache(secondary, pathFilter, DEFAULT_DIFFER);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        RevisionVector rv1 = new RevisionVector(new Revision(1,0,1));
        RevisionVector rv2 = new RevisionVector(new Revision(1,0,3));
        assertSame(DocumentNodeStateCache.UNKNOWN, cache.getDocumentNodeState("/a/b", rv1, rv2));
        assertSame(DocumentNodeStateCache.UNKNOWN, cache.getDocumentNodeState("/x", rv1, rv2));
    }

    @Test
    public void updateAndReadAtReadRev() throws Exception{
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreObserver observer = new SecondaryStoreObserver(secondary, pathFilter, DEFAULT_DIFFER);
        primary.addObserver(observer);

        SecondaryStoreCache cache = new SecondaryStoreCache(secondary, pathFilter, DEFAULT_DIFFER);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        AbstractDocumentNodeState r1 =
                (AbstractDocumentNodeState) primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Update some other part of tree i.e. which does not change lastRev for /a/c
        nb = primary.getRoot().builder();
        create(nb, "/a/e/d");
        AbstractDocumentNodeState r2 =
                (AbstractDocumentNodeState)primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Lookup should work fine
        AbstractDocumentNodeState a_r2 = documentState(r2, "/a");
        NodeStateCacheEntry result
                = cache.getDocumentNodeState("/a/c", r2.getRootRevision(), a_r2.getLastRevision());
        assertTrue(EqualsDiff.equals(a_r2.getChildNode("c"), result.getState()));

        nb = primary.getRoot().builder();
        nb.child("a").child("c").remove();
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Now look from older revision
        result = cache.getDocumentNodeState("/a/c", r1.getRootRevision(), a_r2.getLastRevision());

        //now as its not visible from head it would not be visible
        assertSame(DocumentNodeStateCache.UNKNOWN, result);
    }

    @Test
    public void updateAndReadAtPrevRevision() throws Exception {
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreCache cache = new SecondaryStoreCache(secondary, pathFilter, DEFAULT_DIFFER);
        SecondaryStoreObserver observer = new SecondaryStoreObserver(secondary, pathFilter, cache,
                DEFAULT_DIFFER, StatisticsProvider.NOOP);
        primary.addObserver(observer);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c");
        AbstractDocumentNodeState r0 =
                (AbstractDocumentNodeState) primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        AbstractDocumentNodeState a_c_0 = documentState(primary.getRoot(), "/a/c");

        //Update some other part of tree i.e. which does not change lastRev for /a/c
        nb = primary.getRoot().builder();
        create(nb, "/a/c/d");
        AbstractDocumentNodeState r1 =
                (AbstractDocumentNodeState)primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        AbstractDocumentNodeState a_c_1 = documentState(primary.getRoot(), "/a/c");

        NodeStateCacheEntry result
                = cache.getDocumentNodeState("/a/c", r1.getRootRevision(), a_c_1.getLastRevision());
        assertTrue(EqualsDiff.equals(a_c_1, result.getState()));

        //Read from older revision
        result
                = cache.getDocumentNodeState("/a/c", r0.getRootRevision(), a_c_0.getLastRevision());
        assertTrue(EqualsDiff.equals(a_c_0, result.getState()));
    }

}