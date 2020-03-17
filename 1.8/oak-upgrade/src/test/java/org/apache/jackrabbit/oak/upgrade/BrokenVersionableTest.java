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

import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionIterator;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BrokenVersionableTest {

    private static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private NodeStore targetNodeStore;

    private RepositoryImpl targetRepository;

    @Before
    public synchronized void upgradeRepository() throws Exception {
        targetNodeStore = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        targetRepository = (RepositoryImpl) new Jcr(new Oak(targetNodeStore)).createRepository();
        NodeStore source = createSourceContent();
        RepositorySidegrade sidegrade = new RepositorySidegrade(source, targetNodeStore);
        sidegrade.setCopyOrphanedVersions(null);
        sidegrade.copy();
    }

    @After
    public synchronized void shutdownRepository() {
        targetRepository.shutdown();
        targetRepository = null;
        targetNodeStore = null;
    }

    private NodeStore createSourceContent() throws Exception {
        SegmentNodeStore source = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(source)).createRepository();
        Session session = repository.login(CREDENTIALS);
        List<String> versionHistoryPaths = new ArrayList<String>();
        try {
            CndImporter.registerNodeTypes(new StringReader("<test = 'http://jackrabbit.apache.org/ns/test'>\n"
                    + "[test:Versionable] > nt:unstructured, mix:versionable"), session);

            Node root = session.getRootNode();

            Node versionable1 = root.addNode("versionable1", NT_UNSTRUCTURED);
            versionable1.addMixin(MIX_VERSIONABLE);
            versionable1.addNode("child", NT_UNSTRUCTURED);

            Node versionable2 = root.addNode("versionable2", "test:Versionable");
            versionable2.addNode("child", NT_UNSTRUCTURED);

            session.save();

            VersionManager vMgr = session.getWorkspace().getVersionManager();
            vMgr.checkin("/versionable1");
            vMgr.checkin("/versionable2");
            versionHistoryPaths.add(vMgr.getVersionHistory("/versionable1").getPath());
            versionHistoryPaths.add(vMgr.getVersionHistory("/versionable2").getPath());
        } finally {
            session.logout();
            repository.shutdown();
        }

        // remove version history to corrupt the JCR repository structure
        NodeBuilder rootBuilder = source.getRoot().builder();
        for (String versionHistoryPath : versionHistoryPaths) {
            NodeStateTestUtils.createOrGetBuilder(rootBuilder, versionHistoryPath).remove();
        }
        source.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return source;
    }

    public JackrabbitSession createAdminSession() throws RepositoryException {
        return (JackrabbitSession) targetRepository.login(CREDENTIALS);
    }

    @Test
    public void verifyNoVersionable() throws RepositoryException {
        Session session = createAdminSession();
        VersionManager vMgr = session.getWorkspace().getVersionManager();
        try {
            assertFalse(session.getNode("/versionable1").isNodeType(MIX_VERSIONABLE));

            Node versionable2 = session.getNode("/versionable2");
            assertTrue(versionable2.isNodeType(MIX_VERSIONABLE));
            VersionHistory history = vMgr.getVersionHistory(versionable2.getPath());
            VersionIterator versions = history.getAllVersions();
            assertEquals("jcr:rootVersion", versions.nextVersion().getName());
            assertFalse(versions.hasNext());
        } finally {
            session.logout();
        }
    }
}
