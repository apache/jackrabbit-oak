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
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
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
public abstract class AbstractRepeatedRepositoryMigrationTest {

    protected static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    protected abstract void copyTwice(File source, NodeStore target, FileStore targetStore) throws Exception;

    protected void createSourceContent(Session session) throws RepositoryException {
        registerCustomPrivileges(session);

        JcrUtils.getOrCreateByPath("/content/child1/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child1/grandchild2", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child1/grandchild3", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child2/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/content/child2/grandchild2", "nt:unstructured", session);

        session.save();
    }

    protected void modifySourceContent(Session session) throws RepositoryException {
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
    public void shouldContainCustomInitializerContent() throws Exception {
        assertTargetContent(new String[] {"/foo", "/foo/bar"});
    }

    @Test
    public void shouldContainUpgradeInitializedContent() throws Exception {
        assertTargetContent(new String[] {"/rep:security", "/oak:index"});
    }

    private void assertTargetContent(String[] existing, String... missing) throws Exception {
        try (FileStore targetStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withMaxFileSize(128).build()) {
            NodeStore target = SegmentNodeStoreBuilders.builder(targetStore).build();
            copyTwice(folder.newFolder(), target, targetStore);
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
    protected interface SourceUpdate {
        void apply(Session session) throws RepositoryException;
    }
}
