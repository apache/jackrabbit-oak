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
package org.apache.jackrabbit.oak.upgrade;

import java.io.File;

import javax.jcr.Credentials;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test case to simulate an incremental upgrade, where a source repository is
 * copied to target initially. Then some modifications are made in the source
 * repository and these are (incrementally) copied to the target repository.
 * <br>
 * The expectation is that in the end the state in the target repository is
 * identical to the state in the source repository, with the exception of any
 * initial content that the upgrade tool created.
 */
public class RepeatedRepositorySidegradeTest {

    private static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private void copyTwice(File source, NodeStore target) throws Exception {
        updateSource(source, this::createSourceContent);
        sidegrade(source, target);

        updateSource(source, this::modifySourceContent);
        sidegrade(source, target);
    }

    private void updateSource(File source, SourceUpdate update) throws Exception {
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(source).build()) {
            NodeStore sourceStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(sourceStore)).createRepository();
            try {
                Session session = repository.login(CREDENTIALS);
                try {
                    update.apply(session);
                } finally {
                    session.logout();
                }
            } finally {
                repository.shutdown();
            }
        }
    }

    private void sidegrade(File source, NodeStore target) throws Exception {
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(source).build()) {
            NodeStore sourceStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = target.getRoot().builder();
            builder.child("foo").child("bar");
            target.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            new RepositorySidegrade(sourceStore, target).copy();
        }
    }

    private void createSourceContent(Session session) throws RepositoryException {
        registerCustomPrivileges(session);

        JcrUtils.getOrCreateByPath("/content/child1/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child1/grandchild2", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child1/grandchild3", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child2/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child2/grandchild2", "nt:unstructured", session);

        session.save();
    }

    private void modifySourceContent(Session session) throws RepositoryException {
        JcrUtils.getOrCreateByPath("/content/child2/grandchild3", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child3", "nt:unstructured", session);

        final Node child1 = JcrUtils.getOrCreateByPath("/content/child1", "nt:unstructured", session);
        child1.remove();

        session.save();
    }

    private void registerCustomPrivileges(Session session) throws RepositoryException {
        final JackrabbitWorkspace workspace = (JackrabbitWorkspace) session.getWorkspace();

        final NamespaceRegistry registry = workspace.getNamespaceRegistry();
        registry.registerNamespace("test", "http://www.example.org/");

        final PrivilegeManager privilegeManager = workspace.getPrivilegeManager();
        privilegeManager.registerPrivilege("test:privilege", false, null);
        privilegeManager.registerPrivilege(
                "test:aggregate", false, new String[]{"jcr:read", "test:privilege"});
    }

    @Test
    public void shouldReflectSourceAfterModifications() throws Exception {
        assertTargetContent(new String[] {
                "/",
                "/content",
                "/content/child2",
                "/content/child2/grandchild1",
                "/content/child2/grandchild2",
                "/content/child2/grandchild3",
                "/content/child3"
        }, "/content/child1");
    }

    @Test
    public void shouldPreserveUnrelatedTargetContent() throws Exception {
        assertTargetContent(new String[] {"/foo", "/foo/bar"});
    }

    @Test
    public void shouldContainOakRepositoryContent() throws Exception {
        assertTargetContent(new String[] {"/rep:security", "/oak:index"});
    }

    private void assertTargetContent(String[] existing, String... missing) throws Exception {
        try (FileStore targetStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withMaxFileSize(128).build()) {
            NodeStore target = SegmentNodeStoreBuilders.builder(targetStore).build();
            copyTwice(folder.newFolder(), target);
            NodeState root = target.getRoot();
            for (String path : existing) {
                Assert.assertTrue("node " + path + " should exist", NodeStateUtils.getNode(root, path).exists());
            }
            for (String path : missing) {
                Assert.assertFalse("node " + path + " should not exist", NodeStateUtils.getNode(root, path).exists());
            }
        }
    }

    @FunctionalInterface
    private interface SourceUpdate {
        void apply(Session session) throws RepositoryException;
    }
}
