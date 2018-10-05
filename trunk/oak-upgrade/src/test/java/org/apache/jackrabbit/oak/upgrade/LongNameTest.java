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
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class LongNameTest {

    private static final Logger LOG = LoggerFactory.getLogger(LongNameTest.class);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<Object[]>();

        params.add(new Object[] { "Short parent, short name", 349, 150, false });
        params.add(new Object[] { "Short parent, long name", 349, 151, false });
        params.add(new Object[] { "Long parent, short name", 350, 150, false });
        params.add(new Object[] { "Long parent, long name", 350, 151, true });

        return params;
    }

    private final String parentPath;

    private final String nodeName;

    private final boolean shouldBeSkipped;

    @Rule
    public final TemporaryFolder crxRepo = new TemporaryFolder(new File("target"));

    public LongNameTest(String name, int parentPathLength, int nodeNameLength, boolean shouldBeSkipped) {
        this.parentPath = generatePath(parentPathLength);
        this.nodeName = generateNodeName(nodeNameLength);
        this.shouldBeSkipped = shouldBeSkipped;
    }

    @Test
    public void testMigrationToDocStore() throws IOException, CommitFailedException, RepositoryException {
        SegmentNodeStore src = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        createNodes(src);

        DocumentNodeStore dst = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder().build();

        RepositorySidegrade sidegrade = new RepositorySidegrade(src, dst);
        sidegrade.setFilterLongNames(true);
        sidegrade.copy();

        NodeState parent = getParent(dst);
        Assert.assertTrue("Parent should exists", parent.exists());
        if (shouldBeSkipped) {
            Assert.assertFalse("Node shouldn't exists", parent.hasChildNode(nodeName));
        } else {
            Assert.assertTrue("Node should exists", parent.hasChildNode(nodeName));
        }
    }

    @Test
    public void testNodeOnDocStore() throws CommitFailedException {
        DocumentNodeStore docStore = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder().build();

        try {
            createNodes(docStore);
            if (shouldBeSkipped) {
                fail("It shouldn't be possible to create a node");
            }
        } catch (CommitFailedException e) {
            if (!shouldBeSkipped) {
                LOG.warn("Unexpected exception", e);
                fail("It should be possible to create a node");
            }
        }
    }

    @Test
    @Ignore
    public void testUpgradeToDocStore() throws IOException, CommitFailedException, RepositoryException {
        File root = crxRepo.newFolder();
        File source = new File(root, "source");
        source.mkdirs();
        RepositoryImpl sourceRepository = RepositoryContext.create(RepositoryConfig.install(source)).getRepository();
        Session session = sourceRepository.login(new SimpleCredentials("admin", "admin".toCharArray()));
        try {
            Node node = session.getRootNode();
            for (String s : PathUtils.elements(parentPath)) {
                node = node.addNode(s);
            }
            node.addNode(nodeName);
            session.save();
        } finally {
            session.logout();
        }
        sourceRepository.shutdown();

        DocumentNodeStore dst = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder().build();
        RepositoryContext ctx = RepositoryContext.create(RepositoryConfig.install(source));
        RepositoryUpgrade upgrade = new RepositoryUpgrade(ctx, dst);
        upgrade.setCheckLongNames(true);
        try {
            upgrade.copy(null);
            if (shouldBeSkipped) {
                fail("Jackrabbit2 Lucene index should be used to inform about the node with long name");
            }
        } catch (Exception e) {
            if (!shouldBeSkipped) {
                LOG.warn("Unexpected exception", e);
                fail("Upgrade should be successful");
            }
        }

        ctx.getRepository().shutdown();
        ctx = RepositoryContext.create(RepositoryConfig.install(source));
        upgrade = new RepositoryUpgrade(ctx, dst);
        upgrade.setCheckLongNames(false);
        upgrade.copy(null);

        NodeState parent = getParent(dst);
        Assert.assertTrue("Parent should exists", parent.exists());
        if (shouldBeSkipped) {
            Assert.assertFalse("Node shouldn't exists", parent.hasChildNode(nodeName));
        } else {
            Assert.assertTrue("Node should exists", parent.hasChildNode(nodeName));
        }
    }

    private void createNodes(NodeStore ns) throws CommitFailedException {
        NodeBuilder root = ns.getRoot().builder();
        NodeBuilder nb = root;
        for (String s : PathUtils.elements(parentPath)) {
            nb = nb.child(s);
        }
        nb.child(nodeName);
        ns.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static String generatePath(int length) {
        if (length == 1) {
            return "/";
        }

        Random random = new Random();
        StringBuilder path = new StringBuilder();
        while (path.length() < length) {
            int remaining = length - path.length();
            if (remaining == 1) {
                path.append(generateNodeName(1));
            } else {
                path.append('/');
                remaining--;
                path.append(generateNodeName(1 + random.nextInt(Math.min(remaining, 150))));
            }
        }
        return path.toString();
    }

    private static String generateNodeName(int length) {
        Random random = new Random();
        StringBuilder nodeName = new StringBuilder();
        for (int i = 0; i < length; i++) {
            nodeName.append((char) ('a' + random.nextInt('z' - 'a')));
        }
        return nodeName.toString();
    }

    private NodeState getParent(NodeStore ns) {
        NodeState node = ns.getRoot();
        for (String s : PathUtils.elements(parentPath)) {
            node = node.getChildNode(s);
        }
        return node;
    }
}
