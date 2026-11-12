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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link VersionGarbageCollector} retaining cross-cluster child visibility.
 */
@RunWith(Parameterized.class)
public class FullGCRevisionRetentionTest {

    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Parameterized.Parameter
    public boolean verification;

    @Parameterized.Parameter(1)
    public boolean dryRun;

    private Clock clock;
    private DocumentNodeStore first;
    private DocumentNodeStore second;
    private FullGCMode originalMode;

    /** Exercises cleanup with and without verification, and verified dry-run. */
    @Parameterized.Parameters(name = "verification={0}, dryRun={1}")
    public static List<Object[]> parameters() {
        return List.of(new Object[] {true, false}, new Object[] {false, false},
                new Object[] {true, true});
    }

    /** Shares an isolated in-memory backend between two cluster nodes. */
    @Before
    public void setUp() throws Exception {
        originalMode = VersionGarbageCollector.getFullGcMode();
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        first = builderProvider.newBuilder().clock(clock).setClusterId(1)
                .setAsyncDelay(0).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(new MemoryDocumentStore()).getNodeStore();
        second = builderProvider.newBuilder().clock(clock).setClusterId(2)
                .setAsyncDelay(0).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(first.getDocumentStore()).getNodeStore();
    }

    /** Restores process-wide clocks and GC configuration after each scenario. */
    @After
    public void tearDown() throws Exception {
        FieldUtils.writeStaticField(VersionGarbageCollector.class, "fullGcMode", originalMode, true);
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    /** OAK-12531: property-history cleanup must not hide children from another cluster. */
    @Test
    public void preserveChildVisibilityWithoutPersistedLastRev() throws Exception {
        NodeBuilder builder = first.getRoot().builder();
        builder.child("parent");
        builder.child("other");
        first.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = first.getRoot().builder();
        builder.child("parent").setProperty("value", "old").child("firstChild");
        builder.child("other").setProperty("value", "old");
        first.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = first.getRoot().builder();
        builder.child("parent").setProperty("value", "previous");
        builder.child("other").setProperty("value", "previous");
        first.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        first.runBackgroundOperations();
        second.runBackgroundOperations();

        builder = second.getRoot().builder();
        builder.child("parent").setProperty("value", "current").child("secondChild");
        builder.child("other").setProperty("value", "current");
        second.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        second.runBackgroundOperations();
        first.runBackgroundOperations();

        DocumentStore documents = first.getDocumentStore();
        // Historical commit roots can supply cluster revisions absent from persisted _lastRev.
        UpdateOp removeLastRev = new UpdateOp("1:/parent", false);
        removeLastRev.removeMapEntry("_lastRev", new Revision(0, 0, 1));
        Revision uncommitted = new Revision(clock.getTime() + 1, 0, 1);
        NodeDocument.setCommitRoot(removeLastRev, uncommitted, 0);
        removeLastRev.setMapEntry("value", uncommitted, "\"uncommitted\"");
        documents.findAndUpdate(Collection.NODES, removeLastRev);
        NodeDocument before = documents.find(Collection.NODES, "1:/parent");
        Assert.assertNotNull(before);
        RevisionVector readRevision = first.getHeadRevision();
        DocumentNodeState stateBefore = before.getNodeAtRevision(first, readRevision, null);
        Assert.assertNotNull(stateBefore);
        Assert.assertTrue(stateBefore.hasChildNode("firstChild"));
        Assert.assertTrue(stateBefore.hasChildNode("secondChild"));

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(2));
        VersionGarbageCollector gc = new VersionGarbageCollector(first,
                new VersionGCSupport(documents), true, dryRun, verification,
                6, 0, 100, 1000, TimeUnit.HOURS.toMillis(1), 0);
        gc.setFullGCPaths(Set.of("/parent"), Set.of());
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, TimeUnit.HOURS);
        Assert.assertEquals("Verification must accept the parent cleanup", 0, stats.skippedFullGCDocsCount);
        Assert.assertTrue("Obsolete property history must be collected", stats.deletedPropRevsCount > 0);

        NodeDocument after = documents.find(Collection.NODES, "1:/parent");
        Assert.assertNotNull(after);
        DocumentNodeState stateAfter = after.getNodeAtRevision(first, readRevision, null);
        Assert.assertNotNull(stateAfter);
        Assert.assertTrue("The first cluster's child must remain visible", stateAfter.hasChildNode("firstChild"));
        Assert.assertTrue("The second cluster's child must remain visible", stateAfter.hasChildNode("secondChild"));
        Assert.assertEquals(stateBefore.getLastRevision(), stateAfter.getLastRevision());
        Assert.assertTrue(AbstractNodeState.equals(stateBefore, stateAfter));
        Assert.assertEquals(dryRun ? before.getLocalMap("value").size() : 1,
                after.getLocalMap("value").size());
        Assert.assertEquals(dryRun ? before.getLocalCommitRoot().size() : 3,
                after.getLocalCommitRoot().size());
    }
}
