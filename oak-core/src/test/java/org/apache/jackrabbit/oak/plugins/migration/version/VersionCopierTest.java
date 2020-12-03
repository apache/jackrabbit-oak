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

import java.util.UUID;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateCopier;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.plugins.version.ReadWriteVersionManager;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT_FROZEN_NODE_REFERENCEABLE;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.commit;
import static org.junit.Assert.assertFalse;
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

    private void copyContent(NodeStore source, NodeStore target, String path)
            throws CommitFailedException {
        NodeStateCopier.builder().include(path).copy(source, target);
    }

    private void copyVersionStorage(NodeStore source, NodeStore target)
            throws CommitFailedException {
        NodeBuilder targetRootBuilder = target.getRoot().builder();
        VersionCopier.copyVersionStorage(
                targetRootBuilder,
                VersionHistoryUtil.getVersionStorage(source.getRoot()),
                VersionHistoryUtil.getVersionStorage(targetRootBuilder),
                new VersionCopyConfiguration()
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
        NodeBuilder rootBuilder = ns.getRoot().builder();
        NodeBuilder builder = rootBuilder;
        for (String name : PathUtils.elements(path)) {
            builder = builder.child(name);
        }
        builder.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        builder.setProperty(JCR_MIXINTYPES, singletonList(MIX_VERSIONABLE), Type.NAMES);
        builder.setProperty(JCR_UUID, UUID.randomUUID().toString());
        ReadWriteVersionManager vMgr = new ReadWriteVersionManager(
                VersionHistoryUtil.getVersionStorage(rootBuilder),
                rootBuilder
        );
        vMgr.checkin(builder);
        commit(ns, rootBuilder);
        return ns;
    }
}
