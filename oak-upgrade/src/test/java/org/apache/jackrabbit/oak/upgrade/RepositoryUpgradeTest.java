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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Random;

import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class RepositoryUpgradeTest extends AbstractRepositoryUpgradeTest {

    private static final Calendar DATE = Calendar.getInstance();

    private static final byte[] BINARY = new byte[64 * 1024];

    static {
        new Random().nextBytes(BINARY);
    }

    // needs to be static because the content is created during the @BeforeClass phase
    private static String testNodeIdentifier;

    protected void createSourceContent(Repository repository) throws Exception {
        Session session = repository.login(CREDENTIALS);
        try {
            JackrabbitWorkspace workspace =
                    (JackrabbitWorkspace) session.getWorkspace();

            NamespaceRegistry registry = workspace.getNamespaceRegistry();
            registry.registerNamespace("test", "http://www.example.org/");

            PrivilegeManager privilegeManager = workspace.getPrivilegeManager();
            privilegeManager.registerPrivilege("test:privilege", false, null);
            privilegeManager.registerPrivilege(
                    "test:aggregate", false, new String[] { "jcr:read", "test:privilege" });

            NodeTypeManager nodeTypeManager = workspace.getNodeTypeManager();
            NodeTypeTemplate template = nodeTypeManager.createNodeTypeTemplate();
            template.setName("test:unstructured");
            template.setDeclaredSuperTypeNames(
                    new String[] { "nt:unstructured" });
            nodeTypeManager.registerNodeType(template, false);

            Node root = session.getRootNode();

            Node referenceable =
                root.addNode("referenceable", "test:unstructured");
            referenceable.addMixin(NodeType.MIX_REFERENCEABLE);
            Node referenceable2 =
                root.addNode("referenceable2", "test:unstructured");
            referenceable2.addMixin(NodeType.MIX_REFERENCEABLE);
            session.save();
            testNodeIdentifier = referenceable.getIdentifier();

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
            properties.setProperty("weak_reference", session.getValueFactory().createValue(referenceable, true));
            properties.setProperty("mv_reference", new Value[]{session.getValueFactory().createValue(referenceable2, false)});
            properties.setProperty("mv_weak_reference", new Value[]{session.getValueFactory().createValue(referenceable2, true)});
            properties.setProperty("string", "test");
            properties.setProperty("multiple", "a,b,c".split(","));
            session.save();

            binary = properties.getProperty("binary").getBinary();
            try {
                InputStream stream = binary.getStream();
                try {
                    for (byte aBINARY : BINARY) {
                        assertEquals(aBINARY, (byte) stream.read());
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

    @Test
    public void verifyNameSpaces() throws Exception {
        Session session = createAdminSession();
        try {
            assertEquals(
                    "http://www.example.org/",
                    session.getNamespaceURI("test"));
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyCustomPrivileges() throws Exception {
        JackrabbitSession session = createAdminSession();
        try {
            JackrabbitWorkspace workspace =
                    (JackrabbitWorkspace) session.getWorkspace();
            PrivilegeManager manager = workspace.getPrivilegeManager();

            Privilege privilege = manager.getPrivilege("test:privilege");
            assertNotNull(privilege);
            assertFalse(privilege.isAbstract());
            assertFalse(privilege.isAggregate());
            assertEquals(0, privilege.getDeclaredAggregatePrivileges().length);

            Privilege aggregate = manager.getPrivilege("test:aggregate");
            assertNotNull(aggregate);
            assertFalse(aggregate.isAbstract());
            assertTrue(aggregate.isAggregate());
            assertEquals(2, aggregate.getDeclaredAggregatePrivileges().length);
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyCustomNodeTypes() throws Exception {
        Session session = createAdminSession();
        try {
            NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();
            assertTrue(manager.hasNodeType("test:unstructured"));

            NodeType type = manager.getNodeType("test:unstructured");
            assertFalse(type.isMixin());
            assertTrue(type.isNodeType("nt:unstructured"));

        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyNewBuiltinNodeTypes() throws Exception {
        Session session = createAdminSession();
        try {
            NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();
            assertTrue(manager.hasNodeType(UserConstants.NT_REP_MEMBER_REFERENCES));
            assertTrue(manager.hasNodeType(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE));
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyReplacedBuiltinNodeTypes() throws Exception {
        Session session = createAdminSession();
        try {
            NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();
            NodeType nt = manager.getNodeType(UserConstants.NT_REP_GROUP);
            assertTrue("Migrated repository must have new nodetype definitions", nt.isNodeType(UserConstants.NT_REP_MEMBER_REFERENCES));
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyGenericProperties() throws Exception {
        Session session = createAdminSession();
        try {
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
                    for (byte aBINARY : BINARY) {
                        assertEquals(aBINARY, (byte) stream.read());
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

    @Test
    public void verifyReferencePropertiesContent() throws Exception {
        Session session = createAdminSession();
        try {
            assertTrue(session.nodeExists("/properties"));
            Node properties = session.getNode("/properties");

            assertEquals(
                    PropertyType.REFERENCE,
                    properties.getProperty("reference").getType());
            assertEquals(
                    testNodeIdentifier,
                    properties.getProperty("reference").getString());
            assertEquals(
                    "/referenceable",
                    properties.getProperty("reference").getNode().getPath());
            PropertyIterator refs = session.getNode("/referenceable").getReferences();
            assertTrue(refs.hasNext());
            assertEquals(properties.getPath() + "/reference", refs.nextProperty().getPath());
            assertFalse(refs.hasNext());

            PropertyIterator refs2 = session.getNode("/referenceable2").getReferences();
            assertTrue(refs2.hasNext());
            assertEquals(properties.getPath() + "/mv_reference", refs2.nextProperty().getPath());
            assertFalse(refs2.hasNext());

            assertEquals(
                    PropertyType.WEAKREFERENCE,
                    properties.getProperty("weak_reference").getType());
            assertEquals(
                    testNodeIdentifier,
                    properties.getProperty("weak_reference").getString());
            assertEquals(
                    "/referenceable",
                    properties.getProperty("weak_reference").getNode().getPath());
            PropertyIterator weakRefs = session.getNode("/referenceable").getWeakReferences();
            assertTrue(weakRefs.hasNext());
            assertEquals(properties.getPath() + "/weak_reference", weakRefs.nextProperty().getPath());
            assertFalse(weakRefs.hasNext());
            PropertyIterator weakRefs2 = session.getNode("/referenceable2").getWeakReferences();
            assertTrue(weakRefs2.hasNext());
            assertEquals(properties.getPath() + "/mv_weak_reference", weakRefs2.nextProperty().getPath());
            assertFalse(weakRefs2.hasNext());
        } finally {
            session.logout();
        }
    }

}
