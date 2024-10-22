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
package org.apache.jackrabbit.oak.segment.spi.persistence.split;

import com.microsoft.azure.storage.StorageException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.testutils.NodeStoreTestHarness;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AzureOnTarBaseSplitPersistenceTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public NodeStoreTestHarness.Rule harnesses = new NodeStoreTestHarness.Rule();

    private NodeStoreTestHarness base;

    private NodeStoreTestHarness split;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException, CommitFailedException, URISyntaxException, InvalidKeyException, StorageException {
        base = harnesses.createHarnessWithFolder(TarPersistence::new);
        initializeBaseSetup(base, "1");
        base.getNodeStore().checkpoint(Long.MAX_VALUE);
        base.setReadOnly();

        SegmentNodeStorePersistence azurePersistence = new AzurePersistence(azurite.getContainer("oak-test").getDirectoryReference("oak"));
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(base.getPersistence(), azurePersistence);
        split = harnesses.createHarness(splitPersistence);
    }

    @Test
    public void baseNodesShouldBeAvailable() {
        assertBaseSetup(base, "1");
        assertBaseSetup(split, "1");
    }

    @Test
    public void changesShouldBePersistedInAzureStore() throws CommitFailedException {
        modifyNodeStore(split, "2");

        assertBaseSetup(base, "1");

        assertEquals("v2", split.getNodeState("/foo/bar").getString("version"));
        assertEquals("1.0.0", split.getNodeState("/foo/bar").getString("fullVersion"));
        assertEquals("version_1", split.getNodeState("/foo").getString("fooVersion"));
        assertEquals("version_2", split.getNodeState("/foo").getString("fooOverwriteVersion"));
        assertEquals("version_2", split.getNodeState("/foo").getString("splitVersion"));
        assertFalse(split.getNodeState("/foo/to_be_deleted").exists());
    }


    @Test
    public void rebaseChangesToNewBase() throws CommitFailedException, IOException, InvalidFileStoreVersionException, URISyntaxException, InvalidKeyException, StorageException {

        modifyNodeStore(split, "2");

        final NodeStoreTestHarness newBase = harnesses.createHarnessWithFolder(TarPersistence::new);
        initializeBaseSetup(newBase, "3");
        newBase.setReadOnly();
        assertBaseSetup(newBase, "3");

        SegmentNodeStorePersistence azurePersistence = new AzurePersistence(azurite.getContainer("oak-test").getDirectoryReference("oak-2"));
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(newBase.getPersistence(), azurePersistence);
        final NodeStoreTestHarness newSplit = harnesses.createHarness(splitPersistence);
        // base -> newBase
        // azure -> rebase diff base..split (i.e. what's stored in azure) onto newBase and write them to newSplit
        newSplit.writeAndCommit(builder -> {
            split.getRoot().compareAgainstBaseState(base.getRoot(), new ApplyDiff(builder));
        });

        assertEquals("v2", newSplit.getNodeState("/foo/bar").getString("version"));
        assertEquals("3.0.0", newSplit.getNodeState("/foo/bar").getString("fullVersion"));
        assertEquals("version_3", newSplit.getNodeState("/foo").getString("fooVersion"));
        assertEquals("version_2", newSplit.getNodeState("/foo").getString("fooOverwriteVersion"));
        assertEquals("version_2", newSplit.getNodeState("/foo").getString("splitVersion"));
        assertFalse(split.getNodeState("/foo/to_be_deleted").exists());
    }

    @Test
    public void rebaseChangesAfterGC() throws CommitFailedException, IOException, InvalidFileStoreVersionException, URISyntaxException, InvalidKeyException, StorageException, InterruptedException {

        createGarbage();
        modifyNodeStore(split, "2");
        assertTrue(split.runGC());
        split.startNewTarFile();

        final NodeStoreTestHarness newBase = harnesses.createHarnessWithFolder(TarPersistence::new);
        initializeBaseSetup(newBase, "3");
        newBase.setReadOnly();
        assertBaseSetup(newBase, "3");

        SegmentNodeStorePersistence azurePersistence = new AzurePersistence(azurite.getContainer("oak-test").getDirectoryReference("oak-2"));
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(newBase.getPersistence(), azurePersistence);
        final NodeStoreTestHarness newSplit = harnesses.createHarness(splitPersistence);
        // base -> newBase
        // azure -> rebase diff base..split (i.e. what's stored in azure) onto newBase and write them to newSplit
        newSplit.writeAndCommit(builder -> {
            split.getRoot().compareAgainstBaseState(base.getRoot(), new ApplyDiff(builder));
        });

        // In case we need more advanced conflict resolution, below code can help.
        // However, I think ApplyDiff is equivalent to an OURS resolution strategy.
        // final NodeBuilder builder = newSplit.getRoot().builder();
        // split.getRoot().compareAgainstBaseState(base.getRoot(), new ConflictAnnotatingRebaseDiff(builder));
        // newSplit.merge(builder, ConflictHook.of(DefaultThreeWayConflictHandler.OURS), CommitInfo.EMPTY);

        assertEquals("v2", newSplit.getNodeState("/foo/bar").getString("version"));
        assertEquals("3.0.0", newSplit.getNodeState("/foo/bar").getString("fullVersion"));
        assertEquals("version_3", newSplit.getNodeState("/foo").getString("fooVersion"));
        assertEquals("version_2", newSplit.getNodeState("/foo").getString("fooOverwriteVersion"));
        assertEquals("version_2", newSplit.getNodeState("/foo").getString("splitVersion"));
        assertFalse(split.getNodeState("/foo/to_be_deleted").exists());
    }

    private void createGarbage() throws CommitFailedException, IOException, InvalidFileStoreVersionException {
        for (int i = 0; i < 100; i++) {
            modifyNodeStore(split, "" + i);
            if (i % 50 == 0) {
                split.startNewTarFile();
            }
        }
    }

    private void initializeBaseSetup(NodeStoreTestHarness harness, String version) throws CommitFailedException, IOException, InvalidFileStoreVersionException {
        harness.writeAndCommit(builder -> {
            builder.child("foo").child("bar").setProperty("version", "v" + version);
            builder.child("foo").child("bar").setProperty("fullVersion", version + ".0.0");
            builder.child("foo").setProperty("fooVersion", "version_" + version);
            builder.child("foo").setProperty("fooOverwriteVersion", "version_" + version);
            builder.child("foo").child("to_be_deleted").setProperty("version", "v" + version);
        });
        harness.startNewTarFile();
    }

    private static void assertBaseSetup(NodeStoreTestHarness harness, String version) {
        assertEquals("v" + version, harness.getNodeState("/foo/bar").getString("version"));
        assertEquals(version + ".0.0", harness.getNodeState("/foo/bar").getString("fullVersion"));
        assertEquals("version_" + version, harness.getNodeState("/foo").getString("fooVersion"));
        assertEquals("version_" + version, harness.getNodeState("/foo").getString("fooOverwriteVersion"));
        assertNull(harness.getNodeState("/foo").getString("splitVersion"));
        assertEquals("v" + version, harness.getNodeState("/foo/to_be_deleted").getString("version"));
    }

    private static void modifyNodeStore(NodeStoreTestHarness harness, String version) throws CommitFailedException {
        harness.writeAndCommit(builder -> {
            builder.child("foo").child("bar").setProperty("version", "v" + version);
            builder.child("foo").setProperty("fooOverwriteVersion", "version_" + version);
            builder.child("foo").setProperty("splitVersion", "version_" + version);
            builder.child("foo").child("to_be_deleted").remove();
        });
    }
}
