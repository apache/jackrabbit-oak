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
package org.apache.jackrabbit.oak.plugins.migration.version;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.jcr.RepositoryException;

import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.plugins.migration.DescendantsIterator;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.plugins.version.ReadWriteVersionManager;
import org.apache.jackrabbit.oak.plugins.version.ReadWriteVersionManagerUtil;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_ROOTVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_SUCCESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONLABELS;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONSTORAGE;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT_FROZEN_NODE_REFERENCEABLE;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.commit;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryBuilder;
import static org.apache.jackrabbit.oak.plugins.migration.version.VersionHistoryUtil.getVersionHistoryNodeState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class VersionCopierTest {

    @Test
    public void copyVersionSourceFrozenNodeReferenceable() throws Exception {
        String path = "/foo";
        NodeStore source = createVersionFor(path, createStore(true));
        NodeStore target = createStore(false);

        copyContent(source, target, path);
        copyVersionStorage(source, target);
        assertVersionNotReferenceable(target.getRoot(), path);
    }

    @Test
    public void copyVersionSourceFrozenNodeNotReferenceable() throws Exception {
        String path = "/foo";
        NodeStore source = createVersionFor(path, createStore(false));
        NodeStore target = createStore(false);

        copyContent(source, target, path);
        copyVersionStorage(source, target);
        assertVersionNotReferenceable(target.getRoot(), path);
    }

    @Test
    public void copyVersionTargetFrozenNodeReferenceable() throws Exception {
        String path = "/foo";
        NodeStore source = createVersionFor(path, createStore(true));
        NodeStore target = createStore(true);

        copyContent(source, target, path);
        copyVersionStorage(source, target);
        assertVersionReferenceable(target.getRoot(), path);
    }

    @Ignore("VersionCopier does not generate referenceable target nt:frozenNode from source frozen nodes that are not referenceable")
    @Test
    public void copyVersionTargetGenerateReferenceableFrozenNode() throws Exception {
        String path = "/foo";
        NodeStore source = createVersionFor(path, createStore(false));
        NodeStore target = createStore(true);

        copyContent(source, target, path);
        copyVersionStorage(source, target);
        assertVersionReferenceable(target.getRoot(), path);
    }

    @Test
    public void copyVersionSourceRemovingTargetVersionHistory() throws Exception {
        String path = "/foo";
        NodeStore source = createVersionFor(path, createStore(false));
        NodeStore target = createVersionFor(path, createStore(false));

        // Copy source to target as starting point to duplicate.
        copyVersionStorage(source, target);

        // Add a test property to the nodes of the target version history.
        addVersionHistoryTestProperty(source, target);

        // Copy again, this time setting the remove target version history flag.
        copyVersionStorage(source, target, true);

        // Verify the test properties no longer exist in the target version history.
        assertVersionHistoryTestPropertyRemoved(source, target);
    }

    @Test
    public void copyVersionSourceNotRemovingTargetVersionHistory() throws Exception {
        String path = "/foo";
        NodeStore source = createVersionFor(path, createStore(false));
        NodeStore target = createStore(false);

        // Copy source to target as starting point to duplicate.
        copyContent(source, target, path);
        copyVersionStorage(source, target);

        // Add another version
        target = createNewVersion(path, target, "a");
        
        // copy versions again preserving any new target versions created
        VersionCopyConfiguration config = new VersionCopyConfiguration();
        config.setPreserveOnTarget(true);
        copyVersionStorageInternal(source, target, config);

        // Verify the test properties no longer exist in the target version history.
        assertVersionHistoryPreserveTarget(source, target);
    }
    
    @Test
    public void copyVersionSourceForIncrementalTargetVersionHistory() throws Exception {
        String path = "/foo";
        // create 2 paths & 2 versions
        NodeStore sourceStore = createStore(false);
        for (int i = 0; i < 2; i++) {
            createVersionFor(path + i, sourceStore);
            createNewVersion(path + i, sourceStore, "a");
        }
        NodeStore targetStore = createStore(false);

        // Copy source to target as starting point to duplicate
        for (int i = 0; i < 2; i++) {
            copyContent(sourceStore, targetStore, path + i);
        }
        copyVersionStorage(sourceStore, targetStore);
        
        // Remove 1 version for path /foo0 and create a new one
        removeSpecificVersion(targetStore, "/foo0", "1.0");
        createNewVersion("/foo0", targetStore, "a");

        // copy versions again preserving any new target versions created
        VersionCopyConfiguration config = new VersionCopyConfiguration();
        config.setPreserveOnTarget(true);
        copyVersionStorageInternal(sourceStore, targetStore, config);

        assertVersionsAndUUID(targetStore, sourceStore, "/foo0", true, "1.2");
        assertSuccessorBaseVersion(targetStore, "/foo0", "1.2");
        assertVersionsAndUUID(targetStore, sourceStore, "/foo1", true, "1.1");
        assertSuccessorBaseVersion(targetStore, "/foo1", "1.1");
    }
        
    @Test
    public void copyVersionSourceForMutatedTargetVersionHistory() throws Exception {
        String path = "/foo";
        // create 4 paths & version
        NodeStore sourceStore = createStore(false);
        for (int i = 0; i < 5; i++) {
            createVersionFor(path + i, sourceStore);
            // create another version for foo0
            if (i == 0) {
                createNewVersion(path + i, sourceStore, "");
            }
        }
        NodeStore targetStore = createStore(false);

        // Copy source to target as starting point to duplicate
        for (int i = 0; i < 5; i++) {
            copyContent(sourceStore, targetStore, path + i);
        }
        copyVersionStorage(sourceStore, targetStore);

        // Remove version history for path foo3
        removeVersionHistoryAndRecreate(targetStore, "/foo3");
        // Remove version and recreate 1 version for foo0 // earlier had 2
        removeVersionsAndRecreate(targetStore, "/foo0", false, true, 1);
        // Remove version and recreate 2 versions for foo1 // earlier had 1
        removeVersionsAndRecreate(targetStore, "/foo1", false, true, 2);
        // Remove version and recreate 0 version for foo2 // earlier had 1
        removeVersions(targetStore, "/foo2", false, true);
        // Remove version and recreate 1 different branch versions for foo4 // earlier had 1
        removeVersionsAndRecreate(targetStore, "/foo4", false, false, 1);

        // copy versions again preserving any new target versions created
        VersionCopyConfiguration config = new VersionCopyConfiguration();
        config.setPreserveOnTarget(true);
        copyVersionStorageInternal(sourceStore, targetStore, config);
        
        assertVersionsAndUUID(targetStore, sourceStore,"/foo3", false, "1.0");
        assertVersionsAndUUID(targetStore, sourceStore, "/foo0", true, "1.0");
        assertVersionsAndUUID(targetStore, sourceStore, "/foo1", true, "1.1");
        assertVersionsAndUUID(targetStore, sourceStore, "/foo2", true, "jcr:rootVersion");
        assertVersionsAndUUID(targetStore, sourceStore, "/foo4", true, "2.0");
    }

    private void addVersionHistoryTestProperty(NodeStore source, NodeStore target) throws CommitFailedException {
        final NodeState sourceVersionStorage = VersionHistoryUtil.getVersionStorage(source.getRoot());
        final NodeBuilder targetRootBuilder = target.getRoot().builder();
        final NodeBuilder targetVersionStorage = VersionHistoryUtil.getVersionStorage(targetRootBuilder);

        final Iterator<NodeState> versionStorageIterator = new DescendantsIterator(sourceVersionStorage, 3);
        final NodeState versionHistoryBucket = versionStorageIterator.next();
        for (String versionHistory : versionHistoryBucket.getChildNodeNames()) {
            getVersionHistoryBuilder(targetVersionStorage, versionHistory).setProperty("jcr:test", "test");
        }
        commit(target, targetRootBuilder);
    }

    private void assertVersionHistoryTestPropertyRemoved(NodeStore source, NodeStore target) {
        final NodeState sourceVersionStorage = VersionHistoryUtil.getVersionStorage(source.getRoot());
        final NodeState targetVersionStorage = VersionHistoryUtil.getVersionStorage(target.getRoot());

        final Iterator<NodeState> versionStorageIterator = new DescendantsIterator(sourceVersionStorage, 3);
        final NodeState versionHistoryBucket = versionStorageIterator.next();
        for (String versionHistory : versionHistoryBucket.getChildNodeNames()) {
            assertFalse(getVersionHistoryNodeState(targetVersionStorage, versionHistory).hasProperty("jcr:test"));
        }
    }

    private void assertVersionHistoryPreserveTarget(NodeStore source, NodeStore target) {
        final NodeState sourceVersionStorage = VersionHistoryUtil.getVersionStorage(source.getRoot());
        final NodeState targetVersionStorage = VersionHistoryUtil.getVersionStorage(target.getRoot());

        final Iterator<NodeState> versionStorageIterator = new DescendantsIterator(targetVersionStorage, 3);
        final NodeState versionHistoryBucket = versionStorageIterator.next();
        for (String versionHistory : versionHistoryBucket.getChildNodeNames()) {
            NodeState historyNodeState = getVersionHistoryNodeState(targetVersionStorage, versionHistory);
            List<String> targetList = StreamSupport.stream(historyNodeState.getChildNodeNames().spliterator(), false)
                .collect(Collectors.toList());
            
            NodeState srcHistoryNodeState = getVersionHistoryNodeState(sourceVersionStorage, versionHistory);
            List<String> sourceList = StreamSupport.stream(srcHistoryNodeState.getChildNodeNames().spliterator(), false)
                .collect(Collectors.toList());
            
            // Check all source versions in target
            assertTrue(targetList.containsAll(sourceList));
            // Check target has more versions
            assertTrue(targetList.size() > sourceList.size());
            
            // check successors not empty for 1.0 version
            String successorId = historyNodeState.getChildNode("1.1").getString(JCR_UUID);
            Iterable<String> values =
                historyNodeState.getChildNode("1.0").getProperty(JCR_SUCCESSORS).getValue(Type.STRINGS);
            Set<String> successorSet =
                StreamSupport.stream(values.spliterator(), false).filter(s -> !s.isEmpty()).collect(Collectors.toSet());
            assertFalse(successorSet.isEmpty());
            assertTrue(successorSet.contains(successorId));
        }
    }

    private void copyContent(NodeStore source, NodeStore target, String path)
            throws CommitFailedException {
        NodeStateCopier.builder().include(path).copy(source, target);
    }

    private void copyVersionStorage(NodeStore source, NodeStore target)
        throws CommitFailedException {

        copyVersionStorage(source, target, false);
    }

    private void copyVersionStorage(NodeStore source, NodeStore target, boolean removeTargetVersionHistory)
            throws CommitFailedException {

        VersionCopyConfiguration config = new VersionCopyConfiguration();
        config.setRemoveTargetVersionHistory(removeTargetVersionHistory);

        copyVersionStorageInternal(source, target, config);
    }

    private void copyVersionStorageInternal(NodeStore source, NodeStore target, VersionCopyConfiguration config) 
            throws CommitFailedException {

        NodeBuilder targetRootBuilder = target.getRoot().builder();
        VersionCopier.copyVersionStorage(
            targetRootBuilder,
            VersionHistoryUtil.getVersionStorage(source.getRoot()),
            VersionHistoryUtil.getVersionStorage(targetRootBuilder),
            config
        );
        commit(target, targetRootBuilder);
    }

    private void assertVersionReferenceable(NodeState rootState,
                                            String versionablePath)
            throws RepositoryException {
        assertTrue(getFrozenNode(rootState, versionablePath).hasProperty(JCR_UUID));
    }

    private void assertVersionNotReferenceable(NodeState rootState,
                                               String versionablePath)
            throws RepositoryException {
        assertFalse(getFrozenNode(rootState, versionablePath).hasProperty(JCR_UUID));
    }

    private Tree getFrozenNode(NodeState rootState, String versionablePath)
            throws RepositoryException {
        Root root = new ImmutableRoot(rootState);
        ReadOnlyVersionManager vMgr = ReadOnlyVersionManager.getInstance(
                root, NamePathMapper.DEFAULT);
        Tree version = vMgr.getBaseVersion(root.getTree(versionablePath));
        assertNotNull(version);
        Tree frozenNode = version.getChild(JCR_FROZENNODE);
        assertNotNull(frozenNode);
        return frozenNode;
    }

    private NodeStore createStore(boolean referenceableFrozenNodes) {
        NodeState initial = INITIAL_CONTENT;
        if (referenceableFrozenNodes) {
            initial = INITIAL_CONTENT_FROZEN_NODE_REFERENCEABLE;
        }
        return new MemoryNodeStore(initial);
    }

    private NodeStore createVersionFor(String path, NodeStore ns) throws Exception {
        return createVersion(path, ns, "");
    }

    private NodeStore createVersion(String path, NodeStore ns, String property) throws Exception {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }
        builder.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        builder.setProperty(JCR_MIXINTYPES, Arrays.asList(MIX_VERSIONABLE, MIX_REFERENCEABLE), Type.NAMES);
        builder.setProperty(JCR_UUID, UUID.randomUUID().toString());
        if (StringUtils.isNotEmpty(property)) {
            builder.setProperty(property, property);
        }
        ReadWriteVersionManager vMgr = new ReadWriteVersionManager(
                VersionHistoryUtil.getVersionStorage(rootBuilder),
                rootBuilder
        );
        vMgr.checkin(builder);
        commit(ns, rootBuilder);
        return ns;
    }

    private NodeStore createNewVersion(String path, NodeStore ns, String property) throws Exception {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }
        if (StringUtils.isNotEmpty(property)) {
            builder.setProperty(property, property);
        }
        // Add the mixing mix:versionable
        builder.setProperty(JCR_MIXINTYPES, Arrays.asList(MIX_VERSIONABLE, MIX_REFERENCEABLE), Type.NAMES);
        ReadWriteVersionManager vMgr = new ReadWriteVersionManager(
            VersionHistoryUtil.getVersionStorage(rootBuilder),
            rootBuilder
        );
        vMgr.checkout(builder);
        vMgr.checkin(builder);
        commit(ns, rootBuilder);
        return ns;
    }

    protected String removeVersionableMixin(NodeStore ns, String path) throws CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }

        // remove mix:versionable mixin
        builder.setProperty(JCR_MIXINTYPES, singletonList(MIX_REFERENCEABLE), Type.NAMES);
        String versionableUuid = builder.getString(JCR_UUID);
        commit(ns, rootBuilder);
        return versionableUuid;
    }
    
    protected void removeVersionHistory(NodeStore ns, String path) throws CommitFailedException {
        String versionableUuid = removeVersionableMixin(ns, path);

        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder versionHistoryRoot = VersionHistoryUtil.getVersionStorage(rootBuilder);
        NodeBuilder versionHistoryBuilder =
            VersionHistoryUtil.getVersionHistoryBuilder(versionHistoryRoot, versionableUuid);
        versionHistoryBuilder.remove();
        commit(ns, rootBuilder);
    }

    protected void removeVersions(NodeStore ns, String path, boolean removeRootVersion, boolean removeRootSuccessor) throws CommitFailedException {
        String versionableUuid = removeVersionableMixin(ns, path);
        NodeBuilder rootBuilder = ns.getRoot().builder();
        
        // Remove versions 
        NodeBuilder versionHistoryRoot = VersionHistoryUtil.getVersionStorage(rootBuilder);
        NodeBuilder versionHistoryBuilder =
            VersionHistoryUtil.getVersionHistoryBuilder(versionHistoryRoot, versionableUuid);
        Iterable<String> versionNames = versionHistoryBuilder.getChildNodeNames();
        for (String version: versionNames) {
            if (!version.equals(JCR_ROOTVERSION) || removeRootVersion) {
                versionHistoryBuilder.getChildNode(version).remove();
            } else {
                NodeBuilder rootVersionBuilder = versionHistoryBuilder.getChildNode(version);
                String rootVersionUUID = rootVersionBuilder.getString(JCR_UUID);
                if (removeRootSuccessor) {
                    rootVersionBuilder.setProperty(MultiStringPropertyState.stringProperty(JCR_SUCCESSORS, new ArrayList<>()));
                }
                // Change base version of path to rootVersion
                NodeBuilder builder = rootBuilder;
                for (String name : PathUtils.elements(path)) {
                    builder = builder.child(name);
                }

                // remove mix:versionable mixin
                builder.setProperty(JCR_BASEVERSION, rootVersionUUID, Type.REFERENCE);
            }
        }
        commit(ns, rootBuilder);
    }
    
    protected void removeSpecificVersion(NodeStore ns, String path, String version) throws CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }
        String versionableUuid = builder.getString(JCR_UUID);
        String relPath = VersionHistoryUtil.getRelativeVersionHistoryPath(versionableUuid);
        String versionPath = PathUtils.concat("/", JCR_SYSTEM, JCR_VERSIONSTORAGE + relPath, version);
        ReadWriteVersionManagerUtil.removeVersion(rootBuilder, versionPath);
        commit(ns, rootBuilder);
    }

    protected void assertSuccessorBaseVersion(NodeStore ns, String path, String baseVersion) throws CommitFailedException {
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }
        String versionableUuid = builder.getString(JCR_UUID);

        // Remove version
        NodeBuilder versionHistoryRoot = VersionHistoryUtil.getVersionStorage(rootBuilder);
        NodeBuilder versionHistoryBuilder =
            VersionHistoryUtil.getVersionHistoryBuilder(versionHistoryRoot, versionableUuid);
        NodeBuilder versionNode = versionHistoryBuilder.getChildNode(baseVersion);
        
        String predVersion = getPredecessorVersion(baseVersion);
        NodeBuilder predecessorVersionNode = versionHistoryBuilder.getChildNode(predVersion);
        String baseVersionId = versionNode.getString(JCR_UUID);
        String baseVersionPredecessor =
            StreamSupport.stream(versionNode.getProperty(JCR_PREDECESSORS).getValue(Type.STRINGS).spliterator(), false).collect(
                Collectors.toList()).get(0);
        String predecessorVersionId = predecessorVersionNode.getString(JCR_UUID);
        assertEquals("predecessor version not correct", baseVersionPredecessor, predecessorVersionId);

        String predSuccessorVersionId =
            StreamSupport.stream(predecessorVersionNode.getProperty(JCR_SUCCESSORS).getValue(Type.STRINGS).spliterator(), false).collect(
                Collectors.toList()).get(0);
        assertEquals("successor version not correct", baseVersionId, predSuccessorVersionId);

        NodeBuilder prevVersionNode = versionHistoryBuilder.getChildNode(getPredecessorVersion(predVersion));
        String prevVersionSuccessor =
            StreamSupport.stream(prevVersionNode.getProperty(JCR_SUCCESSORS).getValue(Type.STRINGS).spliterator(), false).collect(
                Collectors.toList()).get(0);
        assertEquals("root successor version not correct", predecessorVersionId, prevVersionSuccessor);
    }

    private static String getPredecessorVersion(String version) {
        if (version.equals("1.0")) {
            return JCR_ROOTVERSION;
        }
        String[] versionSegs = version.split("\\.");
        return versionSegs[0] + "." + (Integer.parseInt(versionSegs[1]) - 1);
    }
    
    private void removeVersionHistoryAndRecreate(NodeStore store, String path) throws Exception {
        removeVersionHistory(store, path);
        createNewVersion(path, store, "");
    }

    private void removeVersionsAndRecreate(NodeStore store, String path, boolean removeRootVersion,
        boolean removeRootSuccessor, int num)
        throws Exception {
        removeVersions(store, path, removeRootVersion, removeRootSuccessor);
        for (int i = 0; i < num; i++) {
            createNewVersion(path, store, "");
        }
    }

    private void assertVersionsAndUUID(NodeStore targetStore, NodeStore sourceStore, String path, boolean isVhSame,
        String targetBaseVersion) {
        NodeState targetRoot = targetStore.getRoot();
        NodeState targetNodeState = targetRoot;
        for (String name : PathUtils.elements(path)) {
            targetNodeState = targetNodeState.getChildNode(name);
        }

        String uuid = targetNodeState.getString(JCR_UUID);
        String baseVersionUuid = targetNodeState.getProperty(JCR_BASEVERSION).getValue(Type.STRING);
        String vhId = targetNodeState.getProperty(JCR_VERSIONHISTORY).getValue(Type.STRING);

        NodeState versionStorage = VersionHistoryUtil.getVersionStorage(targetRoot);
        NodeState versionHistory = getVersionHistoryNodeState(versionStorage, uuid);
        assertEquals(vhId, versionHistory.getString(JCR_UUID));

        NodeState sourceRoot = sourceStore.getRoot();
        NodeState sourceVersionHistory =
            getVersionHistoryNodeState(VersionHistoryUtil.getVersionStorage(sourceRoot), uuid);
        if (isVhSame) {
            assertEquals(vhId, sourceVersionHistory.getString(JCR_UUID));
        } else {
            assertNotEquals(vhId, sourceVersionHistory.getString(JCR_UUID));
        }
        
        List<String> baseVersionFoundList = StreamSupport.stream(versionHistory.getChildNodeNames().spliterator(), false)
            .filter(s -> !s.equals(JCR_VERSIONLABELS))
            .filter(s -> versionHistory.getChildNode(s).getString(JCR_UUID).equals(baseVersionUuid))
            .collect(Collectors.toList());
        assertFalse(baseVersionFoundList.isEmpty());
        assertEquals(targetBaseVersion, baseVersionFoundList.get(0));
    }
}
