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
package org.apache.jackrabbit.oak.upgrade.cli;

import static java.util.Collections.singletonMap;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositorySidegrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public abstract class AbstractOak2OakTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractOak2OakTest.class);

    protected static SegmentTarNodeStoreContainer testContent;

    private NodeStore destination;

    protected Session session;

    private RepositoryImpl repository;

    private String checkpointReference;

    protected abstract NodeStoreContainer getSourceContainer();

    protected abstract NodeStoreContainer getDestinationContainer();

    protected abstract String[] getArgs();

    @BeforeClass
    public static void unpackSegmentRepo() throws IOException {
        File tempDir = new File("target", "test-segment-store");
        if (!tempDir.isDirectory()) {
            Util.unzip(AbstractOak2OakTest.class.getResourceAsStream("/segmentstore.zip"), tempDir);
        }
        testContent = new SegmentTarNodeStoreContainer(tempDir);
    }

    @Before
    public void prepare() throws Exception {
        NodeStore source = getSourceContainer().open();
        try {
            initContent(source);
        } finally {
            getSourceContainer().close();
        }

        String[] args = getArgs();
        log.info("oak2oak {}", Joiner.on(' ').join(args));
        OakUpgrade.main(args);
        createSession();
    }

    protected void createSession() throws RepositoryException, IOException {
        destination = getDestinationContainer().open();
        repository = (RepositoryImpl) new Jcr(destination).with("oak.sling").with(new ReferenceIndexProvider()).createRepository();
        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    @After
    public void clean() throws IOException {
        try {
            session.logout();
            repository.shutdown();
        } finally {
            IOUtils.closeQuietly(getDestinationContainer());
            getDestinationContainer().clean();
            getSourceContainer().clean();
        }
    }

    protected void initContent(NodeStore target) throws IOException, RepositoryException, CommitFailedException {
        NodeStore initialContent = testContent.open();
        try {
            RepositorySidegrade sidegrade = new RepositorySidegrade(initialContent, target);
            sidegrade.copy();
        } finally {
            testContent.close();
        }

        NodeBuilder builder = target.getRoot().builder();
        builder.setProperty("checkpoint-state", "before");
        target.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        checkpointReference = target.checkpoint(60000, singletonMap("key", "123"));

        builder.setProperty("checkpoint-state", "after");
        builder.child(":async").setProperty("test", "123");
        target.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void validateMigration() throws RepositoryException, IOException {
        verifyContent(session);
        verifyBlob(session);
        if (supportsCheckpointMigration()) {
            verifyCheckpoint();
        } else {
            verifyEmptyAsync();
        }
    }

    public static void verifyContent(Session session) throws RepositoryException {
        Node root = session.getRootNode();
        assertEquals("rep:root", root.getPrimaryNodeType().getName());
        assertEquals(1, root.getMixinNodeTypes().length);
        assertEquals("rep:AccessControllable", root.getMixinNodeTypes()[0].getName());
        assertEquals("sling:redirect", root.getProperty("sling:resourceType").getString());

        Node allow = session.getNode("/apps");
        assertEquals("sling:Folder", allow.getProperty("jcr:primaryType").getString());
        assertEquals("admin", allow.getProperty("jcr:createdBy").getString());

        Node nodeType = session.getNode("/jcr:system/jcr:nodeTypes/sling:OrderedFolder");
        assertEquals("rep:NodeType", nodeType.getProperty("jcr:primaryType").getString());
        assertEquals("jcr:mixinTypes", nodeType.getProperty("rep:protectedProperties").getValues()[0].getString());
        assertEquals("false", nodeType.getProperty("jcr:isAbstract").getString());
    }

    public static void verifyBlob(Session session) throws IOException, RepositoryException {
        Property p = session.getProperty("/sling-logo.png/jcr:content/jcr:data");
        InputStream is = p.getValue().getBinary().getStream();
        String expectedMD5 = "35504d8c59455ab12a31f3d06f139a05";
        try {
            assertEquals(expectedMD5, DigestUtils.md5Hex(is));
        } finally {
            is.close();
        }
    }

    private void verifyCheckpoint() {
        assertEquals("after", destination.getRoot().getString("checkpoint-state"));

        Map<String, String> info = destination.checkpointInfo(checkpointReference);
        assertEquals("123", info.get("key"));

        NodeState checkpoint = destination.retrieve(checkpointReference);
        assertEquals("before", checkpoint.getString("checkpoint-state"));

        assertEquals("123", destination.getRoot().getChildNode(":async").getString("test"));

        for (String name : new String[] {"var", "etc", "sling.css", "apps", "libs", "sightly"}) {
            assertSameRecord(destination.getRoot().getChildNode(name), checkpoint.getChildNode(name));
        }
    }

    private static void assertSameRecord(NodeState ns1, NodeState ns2) {
        String recordId1 = getRecordId(ns1);
        String recordId2 = getRecordId(ns2);
        assertNotNull(recordId1);
        assertEquals(recordId1, recordId2);
    }

    private static String getRecordId(NodeState node) {
        if (node instanceof SegmentNodeState) {
            return ((SegmentNodeState) node).getRecordId().toString();
        } else if (node instanceof org.apache.jackrabbit.oak.segment.SegmentNodeState) {
            return ((org.apache.jackrabbit.oak.segment.SegmentNodeState) node).getRecordId().toString();
        } else {
            return null;
        }
    }

    // OAK-2869
    private void verifyEmptyAsync() {
        NodeState state = destination.getRoot().getChildNode(":async");
        assertFalse(state.hasProperty("test"));
    }

    protected boolean supportsCheckpointMigration() {
        return false;
    }
}