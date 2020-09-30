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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import java.util.Collections;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NestedCugHookTest extends AbstractCugTest {

    protected boolean removeCug(@NotNull String path, boolean doCommit) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        for (AccessControlPolicy policy : acMgr.getPolicies(path)) {
            if (policy instanceof CugPolicy) {
                acMgr.removePolicy(path, policy);
                if (doCommit) {
                    root.commit();
                }
                return true;
            }
        }
        return false;
    }

    @Test
    public void testToString() {
        assertEquals("NestedCugHook", new NestedCugHook().toString());
    }

    @Test
    public void testAddCug() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        root.commit();

        assertNestedCugs(root, getRootProvider(), "/", false, "/content");
        assertNestedCugs(root, getRootProvider(), "/content", true);
    }

    @Test
    public void testAddNestedCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/a", "/content/aa/bb", "/content2");
        assertNestedCugs(root, getRootProvider(), "/content/a", true, "/content/a/b/c");

        // add CUG at /content after having created CUGs in the subtree
        createCug("/content", EveryonePrincipal.getInstance());
        root.commit();
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content", "/content2");
        assertNestedCugs(root, getRootProvider(), "/content", true, "/content/a", "/content/aa/bb");
        assertNestedCugs(root, getRootProvider(), "/content/a", true, "/content/a/b/c");
    }

    @Test
    public void testAddNodeWithCug() throws Exception {
        createCug(SUPPORTED_PATH2, EveryonePrincipal.getInstance());

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH2), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH2, true, path);
    }

    @Test
    public void testAddNodeWithCugManually() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(root, path, getTestGroupPrincipal().getName());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true, path);
    }

    @Test
    public void testAddAtUnsupportedPath() throws Exception {
        String unsupportedPath = UNSUPPORTED_PATH + "/child";

        createCug(root, unsupportedPath, EveryonePrincipal.NAME);
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, unsupportedPath);
    }

    @Test
    public void testAddAtRoot() throws Exception {
        createCug(root, ROOT_PATH, EveryonePrincipal.NAME);
        root.commit();

        assertTrue(root.getTree(ROOT_PATH).hasChild(REP_CUG_POLICY));

        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, true, "/content", "/content2");
    }

    @Test
    public void testRemoveCug() throws Exception {
        // create cugs at /content
        createCug("/content", getTestGroupPrincipal());
        root.commit();

        // remove CUG at /content
        assertTrue(removeCug("/content", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveNestedCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        // remove CUG at /content/a/b/c
        assertTrue(removeCug("/content/a/b/c", true));

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/a", "/content/aa/bb", "/content2");
        assertNestedCugs(root, getRootProvider(), "/content/a", true);
    }

    @Test
    public void testRemoveIntermediateCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        // remove CUG at /content/a
        assertTrue(removeCug("/content/a", true));

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/aa/bb", "/content2", "/content/a/b/c");
        assertFalse(root.getTree("/content/a").hasChild(REP_CUG_POLICY));
    }

    @Test
    public void testRemoveMultipleCug() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        assertTrue(removeCug("/content/a", false));
        assertTrue(removeCug("/content/aa/bb", false));
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2", "/content/a/b/c");
    }

    @Test
    public void testRemoveMultipleCug2() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        assertTrue(removeCug("/content/a", false));
        assertTrue(removeCug("/content/a/b/c", false));
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content/aa/bb", "/content2");
    }

    @Test
    public void testRemoveContentNode() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        root.getTree("/content").remove();
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2");
    }

    @Test
    public void testRemoveContentANode() throws Exception {
        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        setupCugsAndAcls();

        root.getTree("/content/a").remove();
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2", "/content/aa/bb");
    }

    @Test
    public void testRemoveRootCug() throws Exception {
        // add cug at /
        createCug(root, ROOT_PATH, EveryonePrincipal.NAME);
        createCug("/content", getTestGroupPrincipal());
        createCug("/content2", EveryonePrincipal.getInstance());
        root.commit();

        assertTrue(root.getTree(PathUtils.concat(ROOT_PATH, REP_CUG_POLICY)).remove());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content", "/content2");

        assertTrue(removeCug("/content", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, "/content2");

        assertTrue(removeCug("/content2", true));
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false);
    }

    @Test
    public void testRemoveAndReadd() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true, path);

        removeCug(path, false);
        createCug(path, EveryonePrincipal.getInstance());
        root.commit();

        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true, path);
    }

    @Test
    public void testMoveToUnsupportedPath() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        String destPath = PathUtils.concat(UNSUPPORTED_PATH, "moved");
        root.move(path, destPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3, destPath);
    }

    @Test
    public void testMoveToSupportedPath() throws Exception {
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        String destPath = PathUtils.concat(SUPPORTED_PATH, "moved");
        root.move(path, destPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true);
        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3, destPath);
    }

    @Test
    public void testMoveToNested() throws Exception {
        createCug(root, SUPPORTED_PATH2, EveryonePrincipal.NAME);
        createCug(root, SUPPORTED_PATH3, EveryonePrincipal.NAME);

        Tree newTree = TreeUtil.addChild(root.getTree(SUPPORTED_PATH3), "child", NT_OAK_UNSTRUCTURED);
        String path = newTree.getPath();
        createCug(path, getTestGroupPrincipal());
        root.commit();

        String destPath = PathUtils.concat(SUPPORTED_PATH2, "moved");
        root.move(path, destPath);
        root.commit();

        assertNestedCugs(root, getRootProvider(), ROOT_PATH, false, SUPPORTED_PATH3, SUPPORTED_PATH2);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH3, true);
        assertNestedCugs(root, getRootProvider(), SUPPORTED_PATH2, true, destPath);
    }

    @Test
    public void testHiddenChildNodeAdded() throws Exception {
        NestedCugHook nch = new NestedCugHook();

        NodeState before = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        NodeState after = spy(before);

        NodeState child = mock(NodeState.class);
        Iterable newCnes = Collections.singleton(new MemoryChildNodeEntry(":hidden", child));
        Iterable cnes = Iterables.concat(newCnes, before.getChildNodeEntries());
        when(after.getChildNodeEntries()).thenReturn(cnes);
        when(after.getChildNode(":hidden")).thenReturn(child);

        nch.processCommit(before, after, new CommitInfo("sid", null));

        verify(child, never()).getProperty(anyString());
    }

    @Test
    public void testHiddenChildNodeChanged() {
        NestedCugHook nch = new NestedCugHook();

        NodeState nodeState = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        NodeState after = spy(nodeState);
        NodeState before = spy(nodeState);

        NodeState child = mock(NodeState.class);
        Iterable hidden = Collections.singleton(new MemoryChildNodeEntry(":hidden", child));
        Iterable cnes = Iterables.concat(hidden, nodeState.getChildNodeEntries());
        when(before.getChildNodeEntries()).thenReturn(cnes);
        when(before.getChildNode(":hidden")).thenReturn(child);

        NodeState child2 = when(mock(NodeState.class).exists()).thenReturn(true).getMock();
        hidden = Collections.singleton(new MemoryChildNodeEntry(":hidden", child2));
        cnes = Iterables.concat(hidden, nodeState.getChildNodeEntries());
        when(after.getChildNodeEntries()).thenReturn(cnes);
        when(after.getChildNode(":hidden")).thenReturn(child2);

        nch.processCommit(before, after, new CommitInfo("sid", null));

        verify(child, never()).getProperty(anyString());
        verify(child2, never()).getProperty(anyString());
    }

    @Test
    public void testHiddenChildNodeDeleted() {
        NestedCugHook nch = new NestedCugHook();

        NodeState after = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        NodeState before = spy(after);

        NodeState child = mock(NodeState.class);
        Iterable deletedCnes = Collections.singleton(new MemoryChildNodeEntry(":hidden", child));
        Iterable cnes = Iterables.concat(deletedCnes, after.getChildNodeEntries());
        when(before.getChildNodeEntries()).thenReturn(cnes);
        when(before.getChildNode(":hidden")).thenReturn(child);

        nch.processCommit(before, after, new CommitInfo("sid", null));

        verify(child, never()).getProperty(anyString());
    }

    @Test
    public void testRemoveWithInvalidHiddenNestedCugEntry() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content/subtree", EveryonePrincipal.getInstance());
        root.commit();

        // after state no longer contains cug-policy at /content (=> 'deletedChildNode' for CUG node)
        removeCug("/content", false);
        NodeState after = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        // before cug-policy at /content state contains an invalid entry in the HIDDEN_NESTED_CUGS property that
        // cannot be relativized during the reconnect-preparation
        NodeBuilder before = new MemoryNodeBuilder(getTreeProvider().asNodeState(adminSession.getLatestRoot().getTree(PathUtils.ROOT_PATH)));
        NodeBuilder cugNode = before.getChildNode("content").getChildNode("rep:cugPolicy");
        cugNode.setProperty(HIDDEN_NESTED_CUGS, ImmutableList.of("/nested/out/of/hierarchy", "/content/subtree"), Type.STRINGS);

        NestedCugHook nch = new NestedCugHook();
        nch.processCommit(before.getNodeState(), after, new CommitInfo("sid", null));
    }

    @Test
    public void testRemoveWithMissingHiddenNestedCugEntry() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content/subtree", EveryonePrincipal.getInstance());
        root.commit();

        // after state:
        // no longer contains cug-policy at /content/subtree (=> 'deletedChildNode' for that CUG node)
        removeCug("/content/subtree", false);

        // NestedCugHook must not fail if the Diff doesn't manage to find a hidden-nested-cug property listing
        // the nested /content/subtree in the after-state (and thus cannot clean-up the hidden structure, which is already 'cleaned'
        // for that very cug)
        NodeBuilder after = new MemoryNodeBuilder(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        NodeBuilder cugNode = after.getChildNode("content").getChildNode("rep:cugPolicy");
        cugNode.setProperty(HIDDEN_NESTED_CUGS, ImmutableList.of(), Type.STRINGS);

        NodeState before = getTreeProvider().asNodeState(adminSession.getLatestRoot().getTree(PathUtils.ROOT_PATH));

        NestedCugHook nch = new NestedCugHook();
        nch.processCommit(before, after.getNodeState(), new CommitInfo("sid", null));
    }

    @Test
    public void testRemoveWithMissingHiddenNestedCugEntry2() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content/subtree", EveryonePrincipal.getInstance());
        root.commit();

        // after state:
        // no longer contains cug-policy at /content (=> 'deletedChildNode' for that CUG node)
        removeCug("/content/subtree", false);
        removeCug("/content", false);

        NodeState after = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        // NestedCugHook must not fail if the Diff doesn't manage to find a hidden-nested-cug property listing
        // the nested cug of the before-state (and thus cannot clean-up the hidden structure)
        NodeBuilder before = new MemoryNodeBuilder(getTreeProvider().asNodeState(adminSession.getLatestRoot().getTree(PathUtils.ROOT_PATH)));
        NodeBuilder cugNode = before.getChildNode("content").getChildNode("rep:cugPolicy");
        cugNode.setProperty(HIDDEN_NESTED_CUGS, ImmutableList.of(), Type.STRINGS);

        NestedCugHook nch = new NestedCugHook();
        nch.processCommit(before.getNodeState(), after, new CommitInfo("sid", null));
    }

    @Test
    public void testRemoveWithMissingHiddenNestedCugProperty() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content/subtree", EveryonePrincipal.getInstance());
        root.commit();

        // after state:
        // no longer contains cug-policy at /content (=> 'deletedChildNode' for that CUG node)
        removeCug("/content/subtree", false);
        removeCug("/content", false);

        NodeState after = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        // NestedCugHook must not fail if the Diff doesn't manage to find a hidden-nested-cug property listing
        // the nested cug of the before-state (and thus cannot clean-up the hidden structure)
        NodeBuilder before = new MemoryNodeBuilder(getTreeProvider().asNodeState(adminSession.getLatestRoot().getTree(PathUtils.ROOT_PATH)));
        NodeBuilder cugNode = before.getChildNode("content").getChildNode("rep:cugPolicy");
        cugNode.removeProperty(HIDDEN_NESTED_CUGS);

        NestedCugHook nch = new NestedCugHook();
        nch.processCommit(before.getNodeState(), after, new CommitInfo("sid", null));
    }

    @Test
    public void testRemoveWithMissingHiddenNestedCugEntryAtRootNode() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content/subtree", EveryonePrincipal.getInstance());
        root.commit();

        // after state:
        // no longer contains cug-policy at /content (=> 'deletedChildNode' for that CUG node)
        removeCug("/content", false);

        // NestedCugHook must not fail if the Diff doesn't manage to find a hidden-nested-cug property listing
        // the nested /content with the root node (and thus cannot clean-up the hidden structure,
        // which is already 'cleaned' for that very cug)
        NodeBuilder after = new MemoryNodeBuilder(getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH)));
        after.setProperty(HIDDEN_NESTED_CUGS, ImmutableList.of(), Type.STRINGS);
        after.setProperty(HIDDEN_TOP_CUG_CNT, 0);
        NodeState before = getTreeProvider().asNodeState(adminSession.getLatestRoot().getTree(PathUtils.ROOT_PATH));

        NestedCugHook nch = new NestedCugHook();
        nch.processCommit(before, after.getNodeState(), new CommitInfo("sid", null));
    }

    @Test
    public void testRemoveWithMissingNestedCugPolicy() throws Exception {
        createCug("/content", getTestGroupPrincipal());
        createCug("/content/subtree", EveryonePrincipal.getInstance());
        root.commit();

        // after state no longer contains cug-policy at /content (=> 'deletedChildNode' for CUG node)
        // but nested CUG is still present
        removeCug("/content", false);

        // NestedCugHook must not fail the reconnection of the nested-cug is no longer present (although the diff didn't
        // record it as deleted) => mock non-existence of nested cug
        NodeState rootState = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        NodeState afterSubtree = spy(NodeStateUtils.getNode(rootState, "content/subtree"));
        when(afterSubtree.hasChildNode("rep:cugPolicy")).thenReturn(false);
        NodeState afterContent = spy(rootState.getChildNode("content"));
        when(afterContent.getChildNode("subtree")).thenReturn(afterSubtree);
        NodeState after = spy(rootState);
        when(after.getChildNode("content")).thenReturn(afterContent);

        NodeState before = getTreeProvider().asNodeState(adminSession.getLatestRoot().getTree(PathUtils.ROOT_PATH));

        NestedCugHook nch = new NestedCugHook();
        nch.processCommit(before, after, new CommitInfo("sid", null));
    }
}
