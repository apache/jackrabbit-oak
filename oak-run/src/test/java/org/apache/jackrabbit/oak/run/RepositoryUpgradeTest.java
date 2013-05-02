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
package org.apache.jackrabbit.oak.run;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.Credentials;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.RepositoryImpl;
import org.apache.jackrabbit.core.RepositoryUpgrade;
import org.apache.jackrabbit.core.config.RepositoryConfig;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class RepositoryUpgradeTest {

    private static final Credentials CREDENTIALS =
            new SimpleCredentials("admin", "admin".toCharArray());

    private static final Calendar DATE = Calendar.getInstance();

    private static final byte[] BINARY = new byte[64 * 1024];

    static {
        new Random().nextBytes(BINARY);
    }

    private String identifier;

    @Test
    public void testUpgrade() throws Exception{
        File directory = new File("target", "upgrade");
        FileUtils.deleteQuietly(directory);

        RepositoryConfig config = RepositoryConfig.install(directory);
        RepositoryImpl source = RepositoryImpl.create(config);
        try {
            createSourceContent(source);
        } finally {
            source.shutdown();
        }

        NodeStore store = new KernelNodeStore(new MicroKernelImpl());
        RepositoryUpgrade.copy(directory, store);
        Jcr jcr = new Jcr(new Oak(store));
        verifyTargetContent(jcr.createRepository());
    }

    private void createSourceContent(Repository repository) throws Exception {
        Session session = repository.login(CREDENTIALS);
        try {
            NamespaceRegistry registry =
                session.getWorkspace().getNamespaceRegistry();
            registry.registerNamespace("test", "http://www.example.org/");

            NodeTypeManager manager =
                session.getWorkspace().getNodeTypeManager();
            NodeTypeTemplate template = manager.createNodeTypeTemplate();
            template.setName("test:unstructured");
            template.setDeclaredSuperTypeNames(
                    new String[] { "nt:unstructured" });
            manager.registerNodeType(template, false);

            Node root = session.getRootNode();

            Node referenceable =
                root.addNode("referenceable", "test:unstructured");
            referenceable.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            identifier = referenceable.getIdentifier();

            Node properties = root.addNode("properties", "test:unstructured");
            properties.setProperty("boolean", true);
            Binary binary = session.getValueFactory().createBinary(
                    new ByteArrayInputStream(BINARY));
            try {
                properties.setProperty("binary", binary);
            } finally {
                binary.dispose();
            }
            properties.setProperty("date", DATE);
            properties.setProperty("decimal", new BigDecimal(123));
            properties.setProperty("double", Math.PI);
            properties.setProperty("long", 9876543210L);
            properties.setProperty("reference", referenceable);
            properties.setProperty("string", "test");
            properties.setProperty("multiple", "a,b,c".split(","));
            session.save();

            binary = properties.getProperty("binary").getBinary();
            try {
                InputStream stream = binary.getStream();
                try {
                    for (int i = 0; i < BINARY.length; i++) {
                        assertEquals(BINARY[i], (byte) stream.read());
                    }
                    assertEquals(-1, stream.read());
                } finally {
                    stream.close();
                }
            } finally {
                binary.dispose();
            }
        } finally {
            session.logout();
        }
    }

    private void verifyTargetContent(Repository repository) throws Exception {
        Session session = repository.login(CREDENTIALS);
        try {
            assertEquals(
                    "http://www.example.org/",
                    session.getNamespaceURI("test"));

            NodeTypeManager manager =
                    session.getWorkspace().getNodeTypeManager();
            assertTrue(manager.hasNodeType("test:unstructured"));
            NodeType type = manager.getNodeType("test:unstructured");
            assertFalse(type.isMixin());
            assertTrue(type.isNodeType("nt:unstructured"));

            assertTrue(session.nodeExists("/properties"));
            Node properties = session.getNode("/properties");
            assertEquals(
                    PropertyType.BOOLEAN,
                    properties.getProperty("boolean").getType());
            assertEquals(
                    true, properties.getProperty("boolean").getBoolean());
            assertEquals(
                    PropertyType.BINARY,
                    properties.getProperty("binary").getType());
            Binary binary = properties.getProperty("binary").getBinary();
            try {
                InputStream stream = binary.getStream();
                try {
                    for (int i = 0; i < BINARY.length; i++) {
                        assertEquals(BINARY[i], (byte) stream.read());
                    }
                    assertEquals(-1, stream.read());
                } finally {
                    stream.close();
                }
            } finally {
                binary.dispose();
            }
            assertEquals(
                    PropertyType.DATE,
                    properties.getProperty("date").getType());
            assertEquals(
                    DATE.getTimeInMillis(),
                    properties.getProperty("date").getDate().getTimeInMillis());
            assertEquals(
                    PropertyType.DECIMAL,
                    properties.getProperty("decimal").getType());
            assertEquals(
                    new BigDecimal(123),
                    properties.getProperty("decimal").getDecimal());
            assertEquals(
                    PropertyType.DOUBLE,
                    properties.getProperty("double").getType());
            assertEquals(
                    Math.PI, properties.getProperty("double").getDouble());
            assertEquals(
                    PropertyType.LONG,
                    properties.getProperty("long").getType());
            assertEquals(
                    9876543210L, properties.getProperty("long").getLong());
            assertEquals(
                    PropertyType.REFERENCE,
                    properties.getProperty("reference").getType());
            assertEquals(
                    identifier,
                    properties.getProperty("reference").getString());
            assertEquals(
                    "/referenceable",
                    properties.getProperty("reference").getNode().getPath());
            assertEquals(
                    PropertyType.STRING,
                    properties.getProperty("string").getType());
            assertEquals(
                    "test", properties.getProperty("string").getString());
            assertEquals(
                    PropertyType.STRING,
                    properties.getProperty("multiple").getType());
            Value[] values = properties.getProperty("multiple").getValues();
            assertEquals(3, values.length);
            assertEquals("a", values[0].getString());
            assertEquals("b", values[1].getString());
            assertEquals("c", values[2].getString());
        } finally {
            session.logout();
        }
    }

}
