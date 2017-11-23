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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for OAK-2421.
 */
@RunWith(Parameterized.class)
public class OrphanedBranchTest {

    private static final Logger LOG = LoggerFactory.getLogger(OrphanedBranchTest.class);
    private DocumentStoreFixture fixture;
    private DocumentNodeStore store;
    private VersionGarbageCollector gc;

    public OrphanedBranchTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if(mongo.isAvailable()){
            fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        store = new DocumentMK.Builder()
                .setDocumentStore(fixture.createDocumentStore())
                .setAsyncDelay(0)
                .getNodeStore();
        gc = store.getVersionGarbageCollector();
    }

    @After
    public void tearDown() throws Exception {
        store.dispose();
        fixture.dispose();
    }

    @Test
    public void orphanedBranches() throws Exception {
        int numCreated = 0;
        for (;;) {
            NodeBuilder builder = store.getRoot().builder();
            NodeBuilder child = builder.child("foo");
            int numBranches = store.getBranches().size();

            int count = 0;
            // perform changes until a branch is created
            while (store.getBranches().size() == numBranches) {
                child.setProperty("prop", count++);
            }
            // but do not merge!
            numCreated++;

            // commit a change to get new head revision.
            // collisions will only be cleaned up if the head
            // revision is newer.
            builder = store.getRoot().builder();
            builder.child("bar").setProperty("prop", numCreated);
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            numBranches = store.getBranches().size();
            store.runBackgroundOperations();
            // after background ops we must not see more collisions
            // than active branches
            String id = Utils.getIdFromPath("/");
            NodeDocument doc = store.getDocumentStore().find(NODES, id);
            assertNotNull(doc);
            Map<Revision, String> collisions = doc.getLocalMap(COLLISIONS);
            assertTrue("too many collisions: " + collisions.size(),
                    collisions.size() <= numBranches);
            // split ops must remove orphaned changes
            // limit to check is number of branches considered active
            // plus NodeDocument.NUM_REVS_THRESHOLD
            int limit = numBranches + NodeDocument.NUM_REVS_THRESHOLD;
            id = Utils.getIdFromPath("/foo");
            doc = store.getDocumentStore().find(NODES, id);
            assertNotNull(doc);
            Map<Revision, String> map = doc.getLocalMap("prop");
            assertTrue("too many orphaned changes: " + map.size() + " > " + limit,
                    map.size() <= limit);
            map = doc.getLocalCommitRoot();
            assertTrue("too many orphaned commit root entries: " + map.size() + " > " + limit,
                    map.size() <= limit);
            Set<Revision> branchCommits = doc.getLocalBranchCommits();
            assertTrue("too many orphaned branch commit entries: " + branchCommits.size() + " > " + limit,
                    branchCommits.size() <= limit);

            // run garbage collector once in a while for changes on /bar
            if (numCreated % NodeDocument.NUM_REVS_THRESHOLD == 0) {
                gc.gc(1, TimeUnit.SECONDS);
            }

            LOG.info("created {}, still considered active: {}",
                    numCreated, store.getBranches().size());

            // create orphaned branches, until we were able to
            // collect 500 of them (or 100 for MongoDB and other fixtures)
            int collect = fixture instanceof DocumentStoreFixture.MemoryFixture ? 500 : 100;
            if (numCreated - store.getBranches().size() >= collect) {
                break;
            }
        }
    }
    
    // OAK-2442
    @Test
    public void removeUncommittedChange() throws Exception {
        String id = Utils.getIdFromPath("/foo");
        NodeBuilder builder = store.getRoot().builder();
        builder.child("foo");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // force previous document
        int count = 0;
        while (store.getDocumentStore().find(NODES, id).getPreviousRanges().isEmpty()) {
            builder = store.getRoot().builder();
            builder.child("foo").setProperty("test", count++);
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            store.runBackgroundOperations();
        }

        int numBranches = store.getBranches().size();
        count = 0;
        builder = store.getRoot().builder();
        NodeBuilder child = builder.child("foo");
        // perform changes until a branch is created
        while (store.getBranches().size() == numBranches) {
            child.setProperty("prop", count++);
        }
        // perform some other change to update head revision
        builder = store.getRoot().builder();
        builder.child("foo").setProperty("bar", 0);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // force remove branch
        NodeDocument doc = store.getDocumentStore().find(NODES, id);
        assertNotNull(doc);
        assertFalse(doc.getLocalBranchCommits().isEmpty());
        Map<Revision, String> valueMap = doc.getLocalMap("prop");
        assertFalse(valueMap.isEmpty());
        UnmergedBranches branches = store.getBranches();
        Revision branchRev = doc.getLocalMap("prop").firstKey();
        Branch b = branches.getBranch(new RevisionVector(branchRev.asBranchRevision()));
        assertNotNull(b);
        branches.remove(b);
        
        // force another previous document to trigger split
        // this will also remove unmerged changes
        count = 0;
        while (store.getDocumentStore().find(NODES, id).getPreviousRanges().size() == 1) {
            builder = store.getRoot().builder();
            builder.child("foo").setProperty("p", count++);
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            store.runBackgroundOperations();
        }

        doc = store.getDocumentStore().find(NODES, id);
        assertTrue(doc.getLocalBranchCommits().isEmpty());
        doc.getNodeAtRevision(store, store.getHeadRevision(), null);
        
        store.dispose();
    }
}
