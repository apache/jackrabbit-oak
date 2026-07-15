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
package org.apache.jackrabbit.oak.upgrade;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test case that simulates copying different paths from two source repositories
 * into a single target repository.
 */
public class UpgradeFromTwoSourcesTest extends AbstractRepositoryUpgradeTest {

    private static boolean upgradeComplete;
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
            final File sourceDir1 = new File(getTestDirectory(), "source1");
            final File sourceDir2 = new File(getTestDirectory(), "source2");

            sourceDir1.mkdirs();
            sourceDir2.mkdirs();

            final RepositoryImpl source1 = createSourceRepository(sourceDir1);
            final RepositoryImpl source2 = createSourceRepository(sourceDir2);
            final Session session1 = source1.login(CREDENTIALS);
            final Session session2 = source2.login(CREDENTIALS);
            try {
                createSourceContent(session1);
                createSourceContent2(session2);
            } finally {
                session1.save();
                session2.save();
                session1.logout();
                session2.logout();
                source1.shutdown();
                source2.shutdown();
            }

            final NodeStore target = getTargetNodeStore();
            doUpgradeRepository(sourceDir1, target, "/left");
            doUpgradeRepository(sourceDir2, target, "/right", "/left/child2", "/left/child3");
            fileStore.flush();
            upgradeComplete = true;
        }
    }

    private void doUpgradeRepository(File source, NodeStore target, String... includes) throws RepositoryException {
        final RepositoryConfig config = RepositoryConfig.create(source);
        final RepositoryContext context = RepositoryContext.create(config);
        try {
            final RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setIncludes(includes);
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }
    }

    @Override
    protected void createSourceContent(Session session) throws RepositoryException {
        JcrUtils.getOrCreateByPath("/left/child1/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/left/child1/grandchild2", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/left/child1/grandchild3", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/left/child2/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/left/child2/grandchild2", "nt:unstructured", session);

        session.save();
    }

    protected void createSourceContent2(Session session) throws RepositoryException {
        JcrUtils.getOrCreateByPath("/left/child2/grandchild3", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/left/child2/grandchild2", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/left/child3", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/right/child1/grandchild1", "nt:unstructured", session);
        JcrUtils.getOrCreateByPath("/right/child1/grandchild2", "nt:unstructured", session);

        session.save();
    }

    @Test
    public void shouldContainNodesFromBothSources() throws Exception {
        assertExisting(
                "/",
                "/left",
                "/left/child1",
                "/left/child2",
                "/left/child3",
                "/left/child1/grandchild1",
                "/left/child1/grandchild2",
                "/left/child1/grandchild3",
                "/left/child2/grandchild2",
                "/left/child2/grandchild3",
                "/right",
                "/right/child1",
                "/right/child1/grandchild1",
                "/right/child1/grandchild2"
        );

        assertMissing(
                "/left/child2/grandchild1"
        );
    }
}
