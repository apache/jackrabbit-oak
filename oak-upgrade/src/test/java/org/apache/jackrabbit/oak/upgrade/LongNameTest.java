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

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;

import java.io.File;
import java.io.IOException;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class LongNameTest {

    public static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private static final String TOO_LONG_NAME = "this string is an example of a very long node name which is approximately one hundred fifty eight bytes long so too long for the document node store to handle";

    private static final String NOT_TOO_LONG_NAME = "this string despite it is very long as well is not too long for the document node store to handle so it may be migrated succesfully without troubles";

    private static RepositoryConfig sourceRepositoryConfig;

    private static File crx2RepoDir;

    @BeforeClass
    public static void prepareSourceRepository() throws RepositoryException, IOException, InterruptedException {
        crx2RepoDir = new File("target", "upgrade-" + Clock.SIMPLE.getTimeIncreasing());
        FileUtils.deleteQuietly(crx2RepoDir);

        sourceRepositoryConfig = createCrx2Config(crx2RepoDir);
        RepositoryImpl sourceRepository = RepositoryImpl.create(sourceRepositoryConfig);
        Session session = sourceRepository.login(CREDENTIALS);
        try {
            Assert.assertTrue(TOO_LONG_NAME.getBytes().length > 150);
            Assert.assertTrue(NOT_TOO_LONG_NAME.getBytes().length < 150);

            Node longNameParent = createParent(session.getRootNode());
            Assert.assertTrue(longNameParent.getPath().length() >= 350);

            longNameParent.addNode(TOO_LONG_NAME);
            longNameParent.addNode(NOT_TOO_LONG_NAME);
            session.save();

            Assert.assertTrue(longNameParent.hasNode(TOO_LONG_NAME));
            Assert.assertTrue(longNameParent.hasNode(NOT_TOO_LONG_NAME));
        } finally {
            session.logout();
        }
        sourceRepository.shutdown();
    }

    private static RepositoryConfig createCrx2Config(File crx2RepoDir) throws RepositoryException, IOException {
        File source = new File(crx2RepoDir, "source");
        source.mkdirs();
        return RepositoryConfig.install(source);
    }

    @Test
    public void longNameShouldBeSkipped() throws RepositoryException, IOException {
        DocumentNodeStore nodeStore = new DocumentMK.Builder().getNodeStore();
        try {
            upgrade(nodeStore, true);

            NodeState parent = getParent(nodeStore.getRoot());
            Assert.assertTrue(parent.hasChildNode(NOT_TOO_LONG_NAME));
            Assert.assertEquals(1, parent.getChildNodeCount(10));

            // The following throws an DocumentStoreException:
            // Assert.assertFalse(parent.hasChildNode(TOO_LONG_NAME));
        } finally {
            nodeStore.dispose();
        }
    }

    @Test(expected = RepositoryException.class)
    @Ignore
    public void longNameOnDocumentStoreThrowsAnException() throws RepositoryException, IOException {
        DocumentNodeStore nodeStore = new DocumentMK.Builder().getNodeStore();
        try {
            upgrade(nodeStore, false);
        } finally {
            nodeStore.dispose();
        }
    }

    @Test
    @Ignore
    public void longNameOnSegmentStoreWorksFine() throws RepositoryException, IOException {
        SegmentStore memoryStore = new MemoryStore();
        try {
            SegmentNodeStore nodeStore = SegmentNodeStore.newSegmentNodeStore(memoryStore).create();
            upgrade(nodeStore, false);

            NodeState parent = getParent(nodeStore.getRoot());
            Assert.assertTrue(parent.hasChildNode(NOT_TOO_LONG_NAME));
            Assert.assertTrue(parent.hasChildNode(TOO_LONG_NAME));
        } finally {
            memoryStore.close();
        }
    }

    private static void upgrade(NodeStore target, boolean skipLongNames) throws RepositoryException, IOException {
        RepositoryConfig config = createCrx2Config(crx2RepoDir);
        RepositoryContext context = RepositoryContext.create(config);
        try {
            RepositoryUpgrade upgrade = new RepositoryUpgrade(context, target);
            upgrade.setSkipLongNames(skipLongNames);
            upgrade.copy(null);
        } finally {
            context.getRepository().shutdown();
        }
    }

    private static Node createParent(Node root) throws RepositoryException {
        Node current = root;
        for (String segment : getParentSegments()) {
            current = current.addNode(segment);
        }
        return current;
    }

    private static NodeState getParent(NodeState root) throws RepositoryException {
        NodeState current = root;
        for (String segment : getParentSegments()) {
            current = current.getChildNode(segment);
        }
        return current;
    }

    private static Iterable<String> getParentSegments() {
        return limit(cycle("this", "is", "a", "path"), 100); // total path
                                                             // length
                                                             // = 350
    }
}
