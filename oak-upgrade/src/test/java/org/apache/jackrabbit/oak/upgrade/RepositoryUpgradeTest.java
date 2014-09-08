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

import static com.google.common.collect.Sets.newHashSet;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ADD_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LOCK_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_NODE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_RETENTION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_ADD_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_ALTER_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_REMOVE_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_USER_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_WRITE;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.jcr.Binary;
import javax.jcr.NamespaceRegistry;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinition;
import javax.jcr.nodetype.PropertyDefinitionTemplate;
import javax.jcr.security.Privilege;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

public class RepositoryUpgradeTest extends AbstractRepositoryUpgradeTest {

    private static final Calendar DATE = Calendar.getInstance();

    private static final byte[] BINARY = new byte[64 * 1024];

    static {
        new Random().nextBytes(BINARY);
    }

    @Override
    @SuppressWarnings("unchecked")
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
                    new String[] {"nt:unstructured"});
            PropertyDefinitionTemplate pDef1 = nodeTypeManager.createPropertyDefinitionTemplate();
            pDef1.setName("defaultString");
            pDef1.setRequiredType(PropertyType.STRING);
            Value stringValue = session.getValueFactory().createValue("stringValue");
            pDef1.setDefaultValues(new Value[] {stringValue});
            template.getPropertyDefinitionTemplates().add(pDef1);

            PropertyDefinitionTemplate pDef2 = nodeTypeManager.createPropertyDefinitionTemplate();
            pDef2.setName("defaultPath");
            pDef2.setRequiredType(PropertyType.PATH);
            Value pathValue = session.getValueFactory().createValue("/jcr:path/nt:value", PropertyType.PATH);
            pDef2.setDefaultValues(new Value[] {pathValue});
            template.getPropertyDefinitionTemplates().add(pDef2);

            nodeTypeManager.registerNodeType(template, false);

            template = nodeTypeManager.createNodeTypeTemplate();
            template.setName("test:referenceable");
            template.setDeclaredSuperTypeNames(
                    new String[] {"nt:unstructured", "mix:referenceable"});
            nodeTypeManager.registerNodeType(template, false);

            Node root = session.getRootNode();

            Node referenceable =
                root.addNode("referenceable", "test:unstructured");
            referenceable.addMixin(NodeType.MIX_REFERENCEABLE);
            Node versionable = root.addNode("versionable", NT_UNSTRUCTURED);
            versionable.addMixin(MIX_VERSIONABLE);
            Node child = versionable.addNode("child", "test:referenceable");
            child.addNode("child2", NT_UNSTRUCTURED);
            session.save();

            Node sns = root.addNode("sns");
            sns.addNode("sibling");
            sns.addNode("sibling");
            sns.addNode("sibling");

            session.getWorkspace().getVersionManager().checkin("/versionable");

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
            properties.setProperty("mv_reference", new Value[]{session.getValueFactory().createValue(versionable, false)});
            properties.setProperty("mv_weak_reference", new Value[]{session.getValueFactory().createValue(versionable, true)});
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
    public void verifyPrivileges() throws RepositoryException {
        Set<String> nonAggregatePrivileges = newHashSet(
            REP_READ_NODES, REP_READ_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES,
            REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
            JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
            JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
            JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
            JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT,
            REP_INDEX_DEFINITION_MANAGEMENT);

        Map<String, String[]> aggregatePrivileges = Maps.newHashMap();
        aggregatePrivileges.put(JCR_READ,
                new String[] {REP_READ_NODES, REP_READ_PROPERTIES});
        aggregatePrivileges.put(JCR_MODIFY_PROPERTIES,
                new String[] {REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES});
        aggregatePrivileges.put(JCR_WRITE,
                new String[] {JCR_MODIFY_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES,
                        REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES,
                        JCR_REMOVE_NODE});
        aggregatePrivileges.put(REP_WRITE,
                new String[] {JCR_WRITE, JCR_MODIFY_PROPERTIES, REP_ADD_PROPERTIES,
                        REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES,
                        JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE, JCR_NODE_TYPE_MANAGEMENT});
        aggregatePrivileges.put(JCR_ALL,
                new String[] {REP_READ_NODES, REP_READ_PROPERTIES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES,
                        REP_REMOVE_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
                        JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL, JCR_NODE_TYPE_MANAGEMENT,
                        JCR_VERSION_MANAGEMENT, JCR_LOCK_MANAGEMENT, JCR_LIFECYCLE_MANAGEMENT,
                        JCR_RETENTION_MANAGEMENT, JCR_WORKSPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT,
                        JCR_NAMESPACE_MANAGEMENT, REP_PRIVILEGE_MANAGEMENT, REP_USER_MANAGEMENT,
                        REP_INDEX_DEFINITION_MANAGEMENT, JCR_READ, JCR_MODIFY_PROPERTIES, JCR_WRITE, REP_WRITE});

        JackrabbitSession session = createAdminSession();
        try {
            JackrabbitWorkspace workspace = (JackrabbitWorkspace) session.getWorkspace();
            PrivilegeManager manager = workspace.getPrivilegeManager();
            Privilege[] privileges = manager.getRegisteredPrivileges();

            for (Privilege privilege : privileges) {
                if (privilege.isAggregate()) {
                    String[] expected = aggregatePrivileges.remove(privilege.getName());
                    if (expected != null) {
                        String[] actual = getNames(privilege.getAggregatePrivileges());
                        assertTrue("Miss match in aggregate privilege " + privilege.getName() +
                                " expected " + Arrays.toString(expected) +
                                " actual " + Arrays.toString(actual),
                            newHashSet(expected).equals(newHashSet(actual)));
                    }
                } else {
                    nonAggregatePrivileges.remove(privilege.getName());
                }
            }

            assertTrue("Missing non aggregate privileges: " + nonAggregatePrivileges,
                    nonAggregatePrivileges.isEmpty());
            assertTrue("Missing aggregate privileges: " + aggregatePrivileges.keySet(),
                aggregatePrivileges.isEmpty());
        }
        finally {
            session.logout();
        }
    }

    private static String[] getNames(Privilege[] privileges) {
        String[] names = new String[privileges.length];
        for (int i = 0; i < privileges.length; i++) {
            names[i] = privileges[i].getName();
        }
        return names;
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
            boolean foundDefaultString = false;
            boolean foundDefaultPath = false;
            for (PropertyDefinition pDef : type.getPropertyDefinitions()) {
                if ("defaultString".equals(pDef.getName())) {
                    assertEquals(PropertyType.STRING, pDef.getRequiredType());
                    assertNotNull(pDef.getDefaultValues());
                    assertEquals(1, pDef.getDefaultValues().length);
                    assertEquals("stringValue", pDef.getDefaultValues()[0].getString());
                    foundDefaultString = true;
                } else if ("defaultPath".equals(pDef.getName())) {
                    assertEquals(PropertyType.PATH, pDef.getRequiredType());
                    assertNotNull(pDef.getDefaultValues());
                    assertEquals(1, pDef.getDefaultValues().length);
                    assertEquals("/jcr:path/nt:value", pDef.getDefaultValues()[0].getString());
                    foundDefaultPath = true;
                }
            }
            assertTrue("Expected property definition with name \"defaultString\"", foundDefaultString);
            assertTrue("Expected property definition with name \"defaultPath\"", foundDefaultPath);
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
            assertTrue(session.nodeExists("/referenceable"));
            String testNodeIdentifier =
                    session.getNode("/referenceable").getIdentifier();

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

            PropertyIterator refs2 = session.getNode("/versionable").getReferences();
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
            PropertyIterator weakRefs2 = session.getNode("/versionable").getWeakReferences();
            assertTrue(weakRefs2.hasNext());
            assertEquals(properties.getPath() + "/mv_weak_reference", weakRefs2.nextProperty().getPath());
            assertFalse(weakRefs2.hasNext());
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifyVersionHistory() throws RepositoryException {
        Session session = createAdminSession();
        try {
            assertTrue(session.nodeExists("/versionable"));
            Node versionable = session.getNode("/versionable");
            assertTrue(versionable.hasNode("child"));
            Node child = versionable.getNode("child");
            assertTrue(child.hasNode("child2"));
            Node child2 = child.getNode("child2");

            assertFalse(versionable.isCheckedOut());
            assertTrue(versionable.hasProperty(JCR_UUID));
            assertFalse(child.isCheckedOut());
            assertTrue(child.hasProperty(JCR_UUID));
            assertFalse(child2.isCheckedOut());
            assertFalse(child2.hasProperty(JCR_UUID));

            VersionManager manager = session.getWorkspace().getVersionManager();
            Version version = manager.getBaseVersion("/versionable");

            Node frozen = version.getFrozenNode();
            assertEquals(
                    versionable.getPrimaryNodeType().getName(),
                    frozen.getProperty(JCR_FROZENPRIMARYTYPE).getString());
            assertEquals(
                    versionable.getMixinNodeTypes()[0].getName(),
                    frozen.getProperty(JCR_FROZENMIXINTYPES).getValues()[0].getString());
            assertEquals(
                    versionable.getIdentifier(),
                    frozen.getProperty(JCR_FROZENUUID).getString());

            Node frozenChild = frozen.getNode("child");
            assertEquals(
                    child.getPrimaryNodeType().getName(),
                    frozenChild.getProperty(JCR_FROZENPRIMARYTYPE).getString());
            assertFalse(frozenChild.hasProperty(JCR_FROZENMIXINTYPES));
            assertEquals(
                    "OAK-1789",
                    child.getIdentifier(),
                    frozenChild.getProperty(JCR_FROZENUUID).getString());

            Node frozenChild2 = frozenChild.getNode("child2");
            assertEquals(
                    child2.getPrimaryNodeType().getName(),
                    frozenChild2.getProperty(JCR_FROZENPRIMARYTYPE).getString());
            assertFalse(frozenChild2.hasProperty(JCR_FROZENMIXINTYPES));
            assertEquals(
                    "OAK-1789",
                    child2.getIdentifier(),
                    frozenChild2.getProperty(JCR_FROZENUUID).getString());

            VersionHistory history = manager.getVersionHistory("/versionable");
            assertTrue(history.isNodeType("rep:VersionablePaths"));
            Property versionablePath = history.getProperty("default");
            assertEquals("/versionable", versionablePath.getString());
        } finally {
            session.logout();
        }
    }

    @Test
    public void verifySNS() throws RepositoryException {
        Set<String> nodeNames = Sets.newHashSet("sibling", "sibling[2]", "sibling[3]");
        Session session = createAdminSession();
        try {
            Node sns = session.getNode("/sns");
            NodeIterator ns = sns.getNodes();
            int c = 0;
            while (ns.hasNext()) {
                Node node = ns.nextNode();
                String name = node.getName();
                assertTrue("Unexpected node: " + name, nodeNames.remove(name));
            }
            assertTrue("Missing nodes: " + nodeNames, nodeNames.isEmpty());
        } finally {
            session.logout();
        }
    }

}
