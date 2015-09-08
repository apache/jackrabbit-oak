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

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.VersionCopySetup;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.VersionManager;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.MIX_REP_VERSIONABLE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.createVersionableNode;
import static org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.isVersionable;

public class CopyVersionHistoryTest extends AbstractRepositoryUpgradeTest {

    private static final String VERSIONABLES_OLD = "/versionables/old";

    private static final String VERSIONABLES_OLD_ORPHANED = "/versionables/oldOrphaned";

    private static final String VERSIONABLES_YOUNG = "/versionables/young";

    private static final String VERSIONABLES_YOUNG_ORPHANED = "/versionables/youngOrphaned";

    protected RepositoryImpl repository;

    protected List<Session> sessions = Lists.newArrayList();

    private static Calendar betweenHistories;

    /**
     * Home directory of source repository.
     */
    private static File source;

    private static String oldOrphanedHistory;
    private static String youngOrphanedHistory;
    private static String oldHistory;
    private static String youngHistory;

    @Override
    protected void createSourceContent(Session session) throws Exception {
        oldHistory = createVersionableNode(session, VERSIONABLES_OLD);
        oldOrphanedHistory = createVersionableNode(session, VERSIONABLES_OLD_ORPHANED);
        Thread.sleep(10);
        betweenHistories = Calendar.getInstance();
        Thread.sleep(10);
        youngOrphanedHistory = createVersionableNode(session, VERSIONABLES_YOUNG_ORPHANED);
        youngHistory = createVersionableNode(session, VERSIONABLES_YOUNG);

        session.getNode(VERSIONABLES_OLD_ORPHANED).remove();
        session.getNode(VERSIONABLES_YOUNG_ORPHANED).remove();
        session.save();
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target) throws RepositoryException {
        // abuse this method to capture the source repo directory
        CopyVersionHistoryTest.source = source;
    }

    @AfterClass
    public static void teardown() {
        CopyVersionHistoryTest.source = null;
    }

    @Test
    public void copyAllVersions() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                // copying all versions is enabled by default
            }
        });
        assertTrue(isVersionable(session, VERSIONABLES_OLD));
        assertTrue(isVersionable(session, VERSIONABLES_YOUNG));
        assertExisting(session, oldOrphanedHistory, youngOrphanedHistory, oldHistory, youngHistory);
        assertHasVersionablePath(session, oldHistory, youngHistory);
    }

    @Test
    public void referencedSinceDate() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(betweenHistories);
            }
        });

        assertFalse(isVersionable(session, VERSIONABLES_OLD));
        assertTrue(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldHistory, oldOrphanedHistory);
        assertExisting(session, youngHistory, youngOrphanedHistory);
        assertHasVersionablePath(session, youngHistory);
    }

    @Test
    public void referencedOlderThanOrphaned() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyOrphanedVersions(betweenHistories);
            }
        });

        assertTrue(isVersionable(session, VERSIONABLES_OLD));
        assertTrue(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldOrphanedHistory);
        assertExisting(session, oldHistory, youngHistory, youngOrphanedHistory);
        assertHasVersionablePath(session, oldHistory, youngHistory);
    }

    @Test
    public void onlyReferenced() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyOrphanedVersions(null);
            }
        });
        assertTrue(isVersionable(session, VERSIONABLES_OLD));
        assertTrue(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldOrphanedHistory, youngOrphanedHistory);
        assertExisting(session, oldHistory, youngHistory);
        assertHasVersionablePath(session, oldHistory, youngHistory);
    }

    @Test
    public void onlyReferencedAfterDate() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(betweenHistories);
                config.setCopyOrphanedVersions(null);
            }
        });
        assertFalse(isVersionable(session, VERSIONABLES_OLD));
        assertTrue(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldHistory, oldOrphanedHistory, youngOrphanedHistory);
        assertExisting(session, youngHistory);
        assertHasVersionablePath(session, youngHistory);
    }

    @Test
    public void onlyOrphaned() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(null);
            }
        });

        assertFalse(isVersionable(session, VERSIONABLES_OLD));
        assertFalse(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldHistory, youngHistory, oldOrphanedHistory, youngOrphanedHistory);
    }

    @Test
    public void onlyOrphanedAfterDate() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(null);
                config.setCopyOrphanedVersions(betweenHistories);
            }
        });

        assertFalse(isVersionable(session, VERSIONABLES_OLD));
        assertFalse(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldHistory, youngHistory, oldOrphanedHistory, youngOrphanedHistory);
    }

    @Test
    public void dontCopyVersionHistory() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(null);
                config.setCopyOrphanedVersions(null);
            }
        });

        assertFalse(isVersionable(session, VERSIONABLES_OLD));
        assertFalse(isVersionable(session, VERSIONABLES_YOUNG));
        assertMissing(session, oldHistory, youngHistory, oldOrphanedHistory, youngOrphanedHistory);
    }

    protected Session performCopy(VersionCopySetup setup) throws RepositoryException, IOException {
        final RepositoryConfig sourceConfig = RepositoryConfig.create(source);
        final RepositoryContext sourceContext = RepositoryContext.create(sourceConfig);
        final NodeStore targetNodeStore = new SegmentNodeStore();
        try {
            final RepositoryUpgrade upgrade = new RepositoryUpgrade(sourceContext, targetNodeStore);
            setup.setup(upgrade.versionCopyConfiguration);
            upgrade.setEarlyShutdown(true);
            upgrade.copy(null);
        } finally {
            sourceContext.getRepository().shutdown();
        }

        repository = (RepositoryImpl) new Jcr(new Oak(targetNodeStore)).createRepository();
        Session s = repository.login(AbstractRepositoryUpgradeTest.CREDENTIALS);
        sessions.add(s);
        return s;
    }

    @After
    public void closeRepository() {
        for (Session s : sessions) {
            s.logout();
        }
        sessions.clear();
        repository.shutdown();
    }

    private static void assertExisting(final Session session, final String... paths) throws RepositoryException {
        for (final String path : paths) {
            final String relPath = path.substring(1);
            assertTrue("node " + path + " should exist", session.getRootNode().hasNode(relPath));
        }
    }

    private static void assertMissing(final Session session, final String... paths) throws RepositoryException {
        for (final String path : paths) {
            final String relPath = path.substring(1);
            assertFalse("node " + path + " should not exist", session.getRootNode().hasNode(relPath));
        }
    }
    
    public static void assertHasVersionablePath(final Session session, final String... historyPaths) throws RepositoryException {
        for (String historyPath : historyPaths) {
            final String workspaceName = session.getWorkspace().getName();
            final Node versionHistory = session.getNode(historyPath);
            assertTrue(versionHistory.isNodeType(MIX_REP_VERSIONABLE_PATHS));
            assertTrue(versionHistory.hasProperty(workspaceName));
            final Property pathProperty = versionHistory.getProperty(workspaceName);
            assertEquals(PropertyType.PATH, pathProperty.getType());
    
            final VersionManager vm = session.getWorkspace().getVersionManager();
            assertEquals(historyPath, vm.getVersionHistory(pathProperty.getString()).getPath());
        }
    }
}
