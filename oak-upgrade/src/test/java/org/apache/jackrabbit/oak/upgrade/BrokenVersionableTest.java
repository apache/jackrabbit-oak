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

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.util.NodeStateTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.version.VersionManager;

import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertFalse;

public class BrokenVersionableTest {

    private static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private NodeStore targetNodeStore;

    private RepositoryImpl targetRepository;

    @Before
    public synchronized void upgradeRepository() throws Exception {
        targetNodeStore = new SegmentNodeStore();
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
        SegmentNodeStore source = new SegmentNodeStore();
        RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(source)).createRepository();
        Session session = repository.login(CREDENTIALS);
        String versionHistoryPath;
        try {
            Node root = session.getRootNode();
            Node versionable = root.addNode("versionable", NT_UNSTRUCTURED);
            versionable.addMixin(MIX_VERSIONABLE);
            versionable.addNode("child", NT_UNSTRUCTURED);
            session.save();

            VersionManager vMgr = session.getWorkspace().getVersionManager();
            vMgr.checkin("/versionable");
            versionHistoryPath = vMgr.getVersionHistory("/versionable").getPath();
        } finally {
            session.logout();
            repository.shutdown();
        }

        // remove version history to corrupt the JCR repository structure
        NodeBuilder rootBuilder = source.getRoot().builder();
        NodeStateTestUtils.createOrGetBuilder(rootBuilder, versionHistoryPath).remove();
        source.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        return source;
    }

    public JackrabbitSession createAdminSession() throws RepositoryException {
        return (JackrabbitSession) targetRepository.login(CREDENTIALS);
    }

    @Test
    public void verifyNoVersionable() throws RepositoryException {
        Session session = createAdminSession();
        try {
            assertFalse(session.getNode("/versionable").isNodeType(MIX_VERSIONABLE));
        } finally {
            session.logout();
        }
    }
}
