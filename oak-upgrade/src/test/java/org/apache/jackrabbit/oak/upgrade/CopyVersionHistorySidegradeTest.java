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

import static org.apache.jackrabbit.JcrConstants.JCR_PREDECESSORS;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.MIX_REP_VERSIONABLE_PATHS;
import static org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.createLabeledVersions;
import static org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.getOrAddNodeWithMixins;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils;
import org.apache.jackrabbit.oak.upgrade.util.VersionCopyTestUtils.VersionCopySetup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

public class CopyVersionHistorySidegradeTest {

    private static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private static final String VERSIONABLES_PATH_PREFIX = "/versionables/";

    private static final String VERSIONABLES_OLD = "old";

    private static final String VERSIONABLES_OLD_ORPHANED = "oldOrphaned";

    private static final String VERSIONABLES_YOUNG = "young";

    private static final String VERSIONABLES_YOUNG_ORPHANED = "youngOrphaned";

    private RepositoryImpl repository;

    private final NodeStore sourceNodeStore = new MemoryNodeStore();

    private final List<Session> sessions = new ArrayList<>();

    private Calendar betweenHistories;

    private final Map<String, String> pathToVersionHistory = new HashMap<>();

    private String[] mixins;

    @Before
    public void createSourceRepository() throws Exception {
        RepositoryImpl sourceRepository = (RepositoryImpl) new Jcr(new Oak(sourceNodeStore)).createRepository();
        try {
            Session session = sourceRepository.login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.logout();
            }
        } finally {
            sourceRepository.shutdown();
        }
    }

    @Test
    public void logsHookProgressUnderHistoricalCategory() throws RepositoryException, IOException {
        LogCustomizer logs = LogCustomizer.forLogger("org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade")
                .enable(Level.INFO).contains("Processing commit via").create();
        logs.starting();
        try {
            new RepositorySidegrade(sourceNodeStore, new MemoryNodeStore()).copy();
            assertFalse("Expected sidegrade progress from the historical migration logger", logs.getLogs().isEmpty());
        } finally {
            logs.finished();
        }
    }

    /** A custom sidegrade hook can add content to the migrated state. */
    @Test
    public void persistsChangesMadeByCustomCommitHook() throws RepositoryException, IOException {
        NodeStore target = new MemoryNodeStore();
        RepositorySidegrade sidegrade = new RepositorySidegrade(sourceNodeStore, target);
        sidegrade.setCustomCommitHooks(List.of((before, after, info) -> {
            assertTrue(after.hasChildNode("versionables"));
            NodeBuilder builder = after.builder();
            builder.child("versionables").setProperty("processedByHook", true);
            return builder.getNodeState();
        }));

        sidegrade.copy();

        assertTrue(target.getRoot().getChildNode("versionables").getBoolean("processedByHook"));
    }

    private void createSourceContent(Session session) throws Exception {

        if (hasSimpleVersioningSupport(session.getRepository())) {
            mixins = new String[] { "mix:simpleVersionable", MIX_VERSIONABLE };
        } else {
            mixins = new String[] { MIX_VERSIONABLE };
        }

        final Node root = session.getRootNode();

        for (final String mixinType : mixins) {
            final Node parent = VersionCopyTestUtils.getOrAddNode(root, rel(VERSIONABLES_PATH_PREFIX + mixinType));

            final Node oldNode = getOrAddNodeWithMixins(parent, VERSIONABLES_OLD, mixinType);
            pathToVersionHistory.put(oldNode.getPath(), createLabeledVersions(oldNode));

            final Node oldOrphanNode = getOrAddNodeWithMixins(parent, VERSIONABLES_OLD_ORPHANED, mixinType);
            pathToVersionHistory.put(oldOrphanNode.getPath(), createLabeledVersions(oldOrphanNode));
        }

        Thread.sleep(10);
        betweenHistories = Calendar.getInstance();
        Thread.sleep(10);

        for (final String mixinType : mixins) {
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

    @Test
    public void copyAllVersions() throws RepositoryException, IOException {
        Session session = performCopy(config -> {
            // copying all versions is enabled by default
        });

        assertVersionableProperties(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertExistingHistories(session,
                VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionablePaths(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertVersionsCanBeRestored(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
    }

    @Test
    public void referencedSinceDate() throws RepositoryException, IOException {
        Session session = performCopy(config -> config.setCopyVersions(betweenHistories));

        assertVersionableProperties(session, VERSIONABLES_YOUNG);
        assertExistingHistories(session, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionablePaths(session, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED);
        assertVersionsCanBeRestored(session, VERSIONABLES_YOUNG);
    }

    @Test
    public void referencedOlderThanOrphaned() throws RepositoryException, IOException {
        Session session = performCopy(config -> config.setCopyOrphanedVersions(betweenHistories));

        assertVersionableProperties(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertExistingHistories(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionablePaths(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD_ORPHANED);
        assertVersionsCanBeRestored(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
    }

    @Test
    public void onlyReferenced() throws RepositoryException, IOException {
        Session session = performCopy(config -> config.setCopyOrphanedVersions(null));
        assertVersionableProperties(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertExistingHistories(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertVersionablePaths(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionsCanBeRestored(session, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
    }

    @Test
    public void onlyReferencedAfterDate() throws RepositoryException, IOException {
        Session session = performCopy(config -> {
            config.setCopyVersions(betweenHistories);
            config.setCopyOrphanedVersions(null);
        });
        assertVersionableProperties(session, VERSIONABLES_YOUNG);
        assertExistingHistories(session, VERSIONABLES_YOUNG);
        assertVersionablePaths(session, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionsCanBeRestored(session, VERSIONABLES_YOUNG);
    }

    @Test
    public void onlyReferencedAfterDateWithIncludePaths() throws RepositoryException, IOException {
        final NodeStore targetNodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();

        // initialize the target node store, with no versions at all
        migrate(config -> {
            config.setCopyVersions(null);
            config.setCopyOrphanedVersions(null);
        }, targetNodeStore, "/");

        // now only copy the recent versions
        migrate(config -> {
            config.setCopyVersions(betweenHistories);
            config.setCopyOrphanedVersions(null);
        }, targetNodeStore, "/versionables");

        repository = (RepositoryImpl) new Jcr(new Oak(targetNodeStore)).createRepository();
        Session session = repository.login(CREDENTIALS);
        sessions.add(session);

        assertVersionableProperties(session, VERSIONABLES_YOUNG);
        assertExistingHistories(session, VERSIONABLES_YOUNG);
        assertVersionablePaths(session, VERSIONABLES_YOUNG);
        assertMissingHistories(session, VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG_ORPHANED);
        assertVersionsCanBeRestored(session, VERSIONABLES_YOUNG);
    }

    @Test
    public void overrideOrphaned() throws RepositoryException, IOException {
        Session session = performCopy(config -> {
            config.setCopyVersions(null);
            config.setCopyOrphanedVersions(betweenHistories);
        });

        assertMissingHistories(session,
                VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
    }

    @Test
    public void dontCopyVersionHistory() throws RepositoryException, IOException {
        Session session = performCopy(config -> {
            config.setCopyVersions(null);
            config.setCopyOrphanedVersions(null);
        });

        assertMissingHistories(session,
                VERSIONABLES_OLD, VERSIONABLES_OLD_ORPHANED, VERSIONABLES_YOUNG, VERSIONABLES_YOUNG_ORPHANED);
        assertNotNull(session.getNode("/jcr:system/jcr:versionStorage")
                .getPrimaryNodeType());
    }

    @Test
    public void removeVersionHistory() throws RepositoryException, IOException {
        final NodeStore targetNodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        migrate((VersionCopySetup) config -> {
        }, targetNodeStore, PathUtils.ROOT_PATH);
        migrate((VersionCopySetup) config -> {
            config.setCopyVersions(null);
            config.setCopyOrphanedVersions(null);
        }, targetNodeStore, "/versionables");
        repository = (RepositoryImpl) new Jcr(new Oak(targetNodeStore)).createRepository();
        Session s = repository.login(CREDENTIALS);
        sessions.add(s);
        assertMissingHistories(s, VERSIONABLES_OLD, VERSIONABLES_YOUNG);
        assertNonVersionablePaths(s, VERSIONABLES_OLD, VERSIONABLES_YOUNG);

    }

    private Session performCopy(VersionCopySetup setup) throws RepositoryException, IOException {
        final NodeStore targetNodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        migrate(setup, targetNodeStore, PathUtils.ROOT_PATH);
        repository = (RepositoryImpl) new Jcr(new Oak(targetNodeStore)).createRepository();
        Session s = repository.login(CREDENTIALS);
        sessions.add(s);
        return s;
    }

    private void migrate(VersionCopySetup setup, NodeStore target, String includePath)
            throws RepositoryException {
        RepositorySidegrade sidegrade = new RepositorySidegrade(sourceNodeStore, target);
        sidegrade.setIncludes(includePath);
        setup.setup(sidegrade.versionCopyConfiguration);
        sidegrade.copy();
    }

    @After
    public void closeRepository() {
        for (Session s : sessions) {
            s.logout();
        }
        sessions.clear();
        if (repository != null) {
            repository.shutdown();
        }
    }

    private static String rel(final String path) {
        if (path.startsWith("/")) {
            return path.substring(1);
        }
        return path;
    }

    private VersionHistory getVersionHistoryForPath(Session session, String path)
            throws RepositoryException {
        final Node root = session.getRootNode();
        if (root.hasNode(rel(pathToVersionHistory.get(path)))) {
            return (VersionHistory)session.getNode(pathToVersionHistory.get(path));
        }
        return null;
    }

    private void assertVersionableProperties(final Session session, final String... names) throws RepositoryException {
        VersionManager vMgr = session.getWorkspace().getVersionManager();
        for (final String mixin : mixins) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                Node versionable = session.getNode(path);
                assertTrue(versionable.isNodeType(MIX_VERSIONABLE));

                String versionHistoryUuid = versionable.getProperty(JCR_VERSIONHISTORY).getString();
                assertEquals(getVersionHistoryForPath(session, path).getIdentifier(), versionHistoryUuid);

                final Version baseVersion = vMgr.getBaseVersion(path);
                assertEquals("1.2", baseVersion.getName());
                final Value[] predecessors = versionable.getProperty(JCR_PREDECESSORS).getValues();
                assertEquals(1, predecessors.length);
                assertEquals(baseVersion.getIdentifier(), predecessors[0].getString());
            }
        }
    }

    private void assertExistingHistories(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : mixins) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                final VersionHistory history = getVersionHistoryForPath(session, path);
                assertNotNull("No history found for " + path, history);
                VersionCopyTestUtils.assertLabeledVersions(history);
            }
        }
    }

    private void assertMissingHistories(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : mixins) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                final VersionHistory history = getVersionHistoryForPath(session, path);
                assertNull("Should not have found history for " + path, history);
            }
        }
    }

    private void assertVersionablePaths(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : mixins) {
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

    private void assertNonVersionablePaths(final Session session, final String... names)
            throws RepositoryException {
        for (final String mixin : mixins) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                final Node node = session.getNode(path);
                assertFalse("Node " + path + " shouldn't have mix:versionable mixin", node.isNodeType(MIX_VERSIONABLE));
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

    private void assertVersionsCanBeRestored(final Session session, final String... names) throws RepositoryException {
        VersionManager vMgr = session.getWorkspace().getVersionManager();
        for (final String mixin : mixins) {
            final String pathPrefix = VERSIONABLES_PATH_PREFIX + mixin + "/";
            for (final String name : names) {
                final String path = pathPrefix + name;
                VersionHistory history = vMgr.getVersionHistory(path);
                assertEquals("1.2", session.getNode(path).getProperty("version").getString());
                vMgr.restore(history.getVersion("1.0"), false);

                Node versionable = session.getNode(path);
                assertEquals("1.0", versionable.getProperty("version").getString());

                // restored node should have correct properties
                String versionHistoryUuid = versionable.getProperty(JCR_VERSIONHISTORY).getString();
                assertEquals(history.getIdentifier(), versionHistoryUuid);

                final Version baseVersion = vMgr.getBaseVersion(path);
                assertEquals("1.0", baseVersion.getName());
                final Value[] predecessors = versionable.getProperty(JCR_PREDECESSORS).getValues();
                assertEquals(0, predecessors.length);
                assertFalse(vMgr.isCheckedOut(path));
            }
        }
        // after restoring, the paths should be still versionable
        assertVersionablePaths(session, names);
    }
}
