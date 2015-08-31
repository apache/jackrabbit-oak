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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositorySidegrade;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentNodeStoreContainer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public abstract class AbstractOak2OakTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractOak2OakTest.class);

    protected static SegmentNodeStoreContainer testContent;

    private NodeStore destination;

    private Session session;

    private RepositoryImpl repository;

    protected abstract NodeStoreContainer getSourceContainer();

    protected abstract NodeStoreContainer getDestinationContainer();

    protected abstract String[] getArgs();

    @BeforeClass
    public static void unpackSegmentRepo() throws IOException {
        File tempDir = new File("target", "test-segment-store");
        if (!tempDir.isDirectory()) {
            Util.unzip(AbstractOak2OakTest.class.getResourceAsStream("/segmentstore.zip"), tempDir);
        }
        testContent = new SegmentNodeStoreContainer(tempDir);
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

        destination = getDestinationContainer().open();
        repository = (RepositoryImpl) new Jcr(destination).with(new ReferenceIndexProvider()).createRepository();
        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    @After
    public void clean() throws IOException {
        session.logout();
        repository.shutdown();
        getDestinationContainer().close();
        getDestinationContainer().clean();
    }

    private void initContent(NodeStore target) throws IOException, RepositoryException {
        NodeStore initialContent = testContent.open();
        try {
            RepositorySidegrade sidegrade = new RepositorySidegrade(initialContent, target);
            sidegrade.copy();
        } finally {
            testContent.close();
        }
    }

    @Test
    public void validateDestinationTest() throws RepositoryException, IOException {
        verifyContent(session);
        Property p = session.getProperty("/sling-logo.png/jcr:content/jcr:data");
        InputStream is = p.getValue().getBinary().getStream();
        String expectedMD5 = "35504d8c59455ab12a31f3d06f139a05";
        try {
            assertEquals(expectedMD5, DigestUtils.md5Hex(is));
        } finally {
            is.close();
        }
    }

    private static void verifyContent(Session session) throws RepositoryException {
        Node allow = session.getNode("/rep:policy/allow");
        assertEquals("rep:GrantACE", allow.getProperty("jcr:primaryType").getString());
        assertEquals("everyone", allow.getProperty("rep:principalName").getString());

        Node admin = session.getNode("/home/users/a/admin");
        assertEquals("rep:User", admin.getProperty("jcr:primaryType").getString());

        Node nodeType = session.getNode("/jcr:system/jcr:nodeTypes/sling:OrderedFolder");
        assertEquals("rep:NodeType", nodeType.getProperty("jcr:primaryType").getString());
        assertEquals("jcr:mixinTypes", nodeType.getProperty("rep:protectedProperties").getValues()[0].getString());
        assertEquals("false", nodeType.getProperty("jcr:isAbstract").getString());
    }

}