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

import java.util.Map;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingHandler;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.SET_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_BUNDLED_CHILD;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_BUNDLING_PATH;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_PATTERN;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CommitDiffTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private BundlingHandler bundlingHandler;

    private DocumentNodeStore ns = builderProvider.newBuilder().build();

    @Before
    public void before() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        merge(ns, builder);
        ns.runBackgroundOperations();
        bundlingHandler = ns.getBundlingConfigHandler().newBundlingHandler();
    }

    @Test
    public void addBundlingRoot() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        CommitDiff diff = newCommitDiff(builder);
        NodeBuilder ntFile = newNtFile().builder();
        ntFile.child(JCR_CONTENT).remove();
        diff.childNodeAdded("file", ntFile.getNodeState());
        assertEquals(1, diff.getNumChanges());
        Commit c = builder.build(Revision.newRevision(1));
        Revision r = c.getRevision();
        UpdateOp op = c.getUpdateOperationForNode(Path.fromString("/file"));
        Map<Key, Operation> changes = op.getChanges();
        assertThat(changes.keySet(), hasSize(4));
        assertThat(changes, hasKey(new Key("_deleted", r)));
        assertThat(changes, hasKey(new Key(MODIFIED_IN_SECS, null)));
        assertThat(changes, hasKey(new Key(JCR_PRIMARYTYPE, r)));
        assertThat(changes, hasKey(new Key(META_PROP_PATTERN, r)));
    }

    @Test
    public void addBundledNodeWithRoot() {
        CommitBuilder builder = new CommitBuilder(ns, null);
        CommitDiff diff = newCommitDiff(builder);
        diff.childNodeAdded("file", newNtFile());
        assertEquals(1, diff.getNumChanges());
        Commit c = builder.build(Revision.newRevision(1));
        Revision r = c.getRevision();
        UpdateOp op = c.getUpdateOperationForNode(Path.fromString("/file"));
        Map<Key, Operation> changes = op.getChanges();
        assertThat(changes.keySet(), hasSize(8));
        assertThat(changes, hasKey(new Key("_deleted", r)));
        assertThat(changes, hasKey(new Key(MODIFIED_IN_SECS, null)));
        assertThat(changes, hasKey(new Key(JCR_PRIMARYTYPE, r)));
        assertThat(changes, hasKey(new Key(META_PROP_PATTERN, r)));
        assertThat(changes, hasEntry(new Key(META_PROP_BUNDLED_CHILD, r), new Operation(SET_MAP_ENTRY, "true")));
        // changes for jcr:content child node
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_PRIMARYTYPE, r)));
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_DATA, r)));
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + META_PROP_BUNDLING_PATH, r)));
    }

    @Test
    public void removeBundledNodeWithRoot() throws Exception {
        addTestFile();

        CommitBuilder builder = new CommitBuilder(ns, null);
        CommitDiff diff = newCommitDiff(builder);
        diff.childNodeDeleted("file", ns.getRoot().getChildNode("file"));
        assertEquals(1, diff.getNumChanges());
        Commit c = builder.build(Revision.newRevision(1));
        Revision r = c.getRevision();
        UpdateOp op = c.getUpdateOperationForNode(Path.fromString("/file"));
        Map<Key, Operation> changes = op.getChanges();
        assertThat(changes.keySet(), hasSize(9));
        assertThat(changes, hasKey(new Key("_deleted", r)));
        assertThat(changes, hasKey(new Key("_deletedOnce", null)));
        assertThat(changes, hasKey(new Key(MODIFIED_IN_SECS, null)));
        assertThat(changes, hasKey(new Key(JCR_PRIMARYTYPE, r)));
        assertThat(changes, hasEntry(new Key(META_PROP_PATTERN, r), new Operation(SET_MAP_ENTRY, null)));
        assertThat(changes, hasEntry(new Key(META_PROP_BUNDLED_CHILD, r), new Operation(SET_MAP_ENTRY, null)));
        // changes for jcr:content child node
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_PRIMARYTYPE, r)));
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_DATA, r)));
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + META_PROP_BUNDLING_PATH, r)));
    }

    @Test
    public void removeBundledNode() throws Exception {
        addTestFile();
        NodeBuilder after = ns.getRoot().builder();
        after.child("file").child(JCR_CONTENT).remove();

        CommitBuilder builder = new CommitBuilder(ns, null);
        CommitDiff diff = newCommitDiff(builder);
        after.getNodeState().compareAgainstBaseState(ns.getRoot(), diff);

        assertEquals(1, diff.getNumChanges());
        Commit c = builder.build(Revision.newRevision(1));
        Revision r = c.getRevision();
        UpdateOp op = c.getUpdateOperationForNode(Path.fromString("/file"));
        Map<Key, Operation> changes = op.getChanges();
        assertThat(changes.keySet(), hasSize(4));
        assertThat(changes, hasKey(new Key(MODIFIED_IN_SECS, null)));
        // changes for jcr:content child node
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_PRIMARYTYPE, r)));
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_DATA, r)));
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + META_PROP_BUNDLING_PATH, r)));
    }

    @Test
    public void changeBundledNode() throws Exception {
        addTestFile();
        NodeBuilder after = ns.getRoot().builder();
        after.child("file").child(JCR_CONTENT).setProperty(JCR_DATA, "modified");

        CommitBuilder builder = new CommitBuilder(ns, null);
        CommitDiff diff = newCommitDiff(builder);
        after.getNodeState().compareAgainstBaseState(ns.getRoot(), diff);

        assertEquals(1, diff.getNumChanges());
        Commit c = builder.build(Revision.newRevision(1));
        Revision r = c.getRevision();
        UpdateOp op = c.getUpdateOperationForNode(Path.fromString("/file"));
        Map<Key, Operation> changes = op.getChanges();
        assertThat(changes.keySet(), hasSize(2));
        assertThat(changes, hasKey(new Key(MODIFIED_IN_SECS, null)));
        // changes for jcr:content child node
        assertThat(changes, hasKey(new Key(JCR_CONTENT + "/" + JCR_DATA, r)));
    }

    private void addTestFile() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.setChildNode("file", newNtFile());
        merge(ns, builder);
    }

    private CommitDiff newCommitDiff(CommitBuilder builder) {
        return new CommitDiff(bundlingHandler, builder, ns.getBlobSerializer());
    }

    private static NodeState newNtFile() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, "nt:file", Type.NAME);
        NodeBuilder content = builder.child("jcr:content");
        content.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        content.setProperty(JCR_DATA, "test");
        return builder.getNodeState();
    }
}
