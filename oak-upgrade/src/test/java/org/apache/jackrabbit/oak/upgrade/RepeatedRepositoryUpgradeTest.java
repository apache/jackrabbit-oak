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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnull;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test case to simulate an incremental upgrade, where a source repository is
 * copied to target initially. Then some modifications are made in the source
 * repository and these are (incrementally) copied to the target repository.
 * <br>
 * The expectation is that in the end the state in the target repository is
 * identical to the state in the source repository, with the exception of any
 * initial content that the upgrade tool created.
 */
public class RepeatedRepositoryUpgradeTest extends AbstractRepositoryUpgradeTest {

    protected static boolean upgradeComplete;
    private static FileStore fileStore;

    @Override
    protected NodeStore createTargetNodeStore() {
        return SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    @BeforeClass
    public static void initialize() {
        final File dir = new File(getTestDirectory(), "segments");
        dir.mkdirs();
        try {
            fileStore = fileStoreBuilder(dir).withMaxFileSize(128).build();
            upgradeComplete = false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterClass
    public static void cleanup() {
        fileStore.close();
        fileStore = null;
    }

    @Before
    public synchronized void upgradeRepository() throws Exception {
        if (!upgradeComplete) {
            final File sourceDir = new File(getTestDirectory(), "jackrabbit2");

            sourceDir.mkdirs();

            RepositoryImpl source = createSourceRepository(sourceDir);
            Session session = source.login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.save();
                session.logout();
                source.shutdown();
            }

            final NodeStore target = getTargetNodeStore();
            doUpgradeRepository(sourceDir, target, false);
            fileStore.flush();

            // re-create source repo
            source = createSourceRepository(sourceDir);
            session = source.login(CREDENTIALS);
            try {
                modifySourceContent(session);
            } finally {
                session.save();
                session.logout();
                source.shutdown();
            }

            doUpgradeRepository(sourceDir, target, true);
            fileStore.flush();

            upgradeComplete = true;
        }
    }

    protected void doUpgradeRepository(File source, NodeStore target, boolean skipInit) throws RepositoryException, IOException {
        final RepositoryConfig config = RepositoryConfig.create(source);
        final RepositoryContext context = RepositoryContext.create(config);
        try {
            final RepositoryUpgrade repositoryUpgrade = new RepositoryUpgrade(context, target);
            repositoryUpgrade.setSkipInitialization(skipInit);
            repositoryUpgrade.copy(new RepositoryInitializer() {
                @Override
                public void initialize(@Nonnull NodeBuilder builder) {
                    builder.child("foo").child("bar");
                }
            });
        } finally {
            context.getRepository().shutdown();
        }
    }

    @Override
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

        assertExisting(
                "/",
                "/content",
                "/content/child2",
                "/content/child2/grandchild1",
                "/content/child2/grandchild2",
                "/content/child2/grandchild3",
                "/content/child3"
        );

        assertMissing(
                "/content/child1"
        );
    }

    @Test
    public void shouldContainCustomInitializerContent() throws Exception {
        assertExisting(
                "/foo",
                "/foo/bar"
        );
    }

    @Test
    public void shouldContainUpgradeInitializedContent() throws Exception {
        assertExisting(
                "/rep:security",
                "/oak:index"
        );
    }

}
