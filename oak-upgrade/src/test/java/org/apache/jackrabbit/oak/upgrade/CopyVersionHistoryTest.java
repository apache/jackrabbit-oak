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

import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.VersionCopySetup;
import org.apache.jackrabbit.oak.upgrade.version.VersionCopyConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.VersionHistory;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.MIX_REP_VERSIONABLE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.getOrAddNodeWithMixins;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.createLabeledVersions;

public class CopyVersionHistoryTest extends AbstractRepositoryUpgradeTest {

    private static final String VERSIONABLES_PATH_PREFIX = "/versionables/";

    private static final String VERSIONABLES_OLD = "old";

    private static final String VERSIONABLES_OLD_ORPHANED = "oldOrphaned";

    private static final String VERSIONABLES_YOUNG = "young";

    private static final String VERSIONABLES_YOUNG_ORPHANED = "youngOrphaned";

    protected RepositoryImpl repository;

    protected List<Session> sessions = Lists.newArrayList();

    private static Calendar betweenHistories;

    private static Map<String, String> pathToVersionHistory = Maps.newHashMap();

    /**
     * Home directory of source repository.
     */
    private static File source;

    private static String[] MIXINS;

    @Override
    protected void createSourceContent(Session session) throws Exception {

        if (hasSimpleVersioningSupport(session.getRepository())) {
            MIXINS = new String[] { "mix:simpleVersionable", MIX_VERSIONABLE };
        } else {
            MIXINS = new String[] { MIX_VERSIONABLE };
        }

        final Node root = session.getRootNode();

        for (final String mixinType : MIXINS) {
            final Node parent = VersionCopyTestUtils.getOrAddNode(root, rel(VERSIONABLES_PATH_PREFIX + mixinType));

            final Node oldNode = getOrAddNodeWithMixins(parent, VERSIONABLES_OLD, mixinType);
            pathToVersionHistory.put(oldNode.getPath(), createLabeledVersions(oldNode));

            final Node oldOrphanNode = getOrAddNodeWithMixins(parent, VERSIONABLES_OLD_ORPHANED, mixinType);
            pathToVersionHistory.put(oldOrphanNode.getPath(), createLabeledVersions(oldOrphanNode));
        }

        Thread.sleep(10);
        betweenHistories = Calendar.getInstance();
        Thread.sleep(10);

        for (final String mixinType : MIXINS) {
            final Node parent = VersionCopyTestUtils.getOrAddNode(root, rel(VERSIONABLES_PATH_PREFIX + mixinType));

            final Node youngNode = getOrAddNodeWithMixins(parent, VERSIONABLES_YOUNG, mixinType);
            pathToVersionHistory.put(youngNode.getPath(), createLabeledVersions(youngNode));

            final Node youngOrphanNode = getOrAddNodeWithMixins(parent, VERSIONABLES_YOUNG_ORPHANED, mixinType);
            pathToVersionHistory.put(youngOrphanNode.getPath(), createLabeledVersions(youngOrphanNode));

            // create orphaned version histories by deleting the original nodes
            parent.getNode(VERSIONABLES_OLD_ORPHANED).remove();
            parent.getNode(VERSIONABLES_YOUNG_ORPHANED).remove();
        }

        session.save();
    }

    private boolean hasSimpleVersioningSupport(final Repository repository) {
        return Boolean.parseBoolean(repository.getDescriptor(Repository.OPTION_SIMPLE_VERSIONING_SUPPORTED));
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target) throws RepositoryException {
        // abuse this method to capture the source repo directory
        CopyVersionHistoryTest.source = source;
    }

    @AfterClass
    public static void teardown() {
        CopyVersionHistoryTest.pathToVersionHistory.clear();
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

        assertExistingHistories(session,
                VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionablePaths(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
    }

    @Test
    public void referencedSinceDate() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(betweenHistories);
            }
        });

        assertExistingHistories(session, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionablePaths(session, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED);
    }

    @Test
    public void referencedOlderThanOrphaned() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyOrphanedVersions(betweenHistories);
            }
        });

        assertExistingHistories(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionablePaths(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD_ORPHANED);
    }

    @Test
    public void onlyReferenced() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyOrphanedVersions(null);
            }
        });
        assertExistingHistories(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertVersionablePaths(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);;
        assertMissingHistories(session, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG_ORPHANED);
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
        assertExistingHistories(session, VERSIONABLES_YOUNG);
        assertVersionablePaths(session, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG_ORPHANED);
    }

    @Test
    public void overrideOrphaned() throws RepositoryException, IOException {
        Session session = performCopy(new VersionCopySetup() {
            @Override
            public void setup(VersionCopyConfiguration config) {
                config.setCopyVersions(null);
                config.setCopyOrphanedVersions(betweenHistories);
            }
        });

        assertMissingHistories(session,
                VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
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

        assertMissingHistories(session,
                VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
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

    private static String rel(final String path) {
        if (path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }

    private static VersionHistory getVersionHistoryForPath(Session session, String path)
            throws RepositoryException {
        final Node root = session.getRootNode();
        if (root.hasNode(rel(pathToVersionHistory.get(path)))) {
            return (VersionHistory)session.getNode(pathToVersionHistory.get(path));
        }
        return null;
    }

    private static void assertExistingHistories(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : MIXINS) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                final VersionHistory history = getVersionHistoryForPath(session, path);
                assertNotNull("No history found for " + path, history);
                VersionCopyTestUtils.assertLabeledVersions(history);
            }
        }
    }

    private static void assertMissingHistories(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : MIXINS) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                final VersionHistory history = getVersionHistoryForPath(session, path);
                assertNull("Should not have found history for " + path, history);
            }
        }
    }

    private static void assertVersionablePaths(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : MIXINS) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                final Node node = session.getNode(path);
                assertTrue("Node " + path + " should have mix:versionable mixin", node.isNodeType(MIX_VERSIONABLE));
                final VersionHistory history = getVersionHistoryForPath(session, path);
                assertVersionablePath(history, path);
            }
        }
    }

    private static void assertVersionablePath(final VersionHistory history, final String versionablePath)
            throws RepositoryException {
        final String workspaceName = history.getSession().getWorkspace().getName();
        assertTrue(history.isNodeType(MIX_REP_VERSIONABLE_PATHS));
        assertTrue(history.hasProperty(workspaceName));
        final Property pathProperty = history.getProperty(workspaceName);
        assertEquals(PropertyType.PATH, pathProperty.getType());
        assertEquals(versionablePath, pathProperty.getString());
    }
}
