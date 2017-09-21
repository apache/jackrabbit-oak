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
package org.apache.jackrabbit.oak.jcr;

import static java.util.Arrays.asList;
import static javax.jcr.ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW;
import static javax.jcr.Repository.OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED;
import static org.apache.jackrabbit.commons.JcrUtils.getChildNodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jcr.Binary;
import javax.jcr.GuestCredentials;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Item;
import javax.jcr.ItemExistsException;
import javax.jcr.NamespaceException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeDefinition;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.nodetype.PropertyDefinitionTemplate;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.commons.cnd.ParseException;
import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.spi.QValue;
import org.apache.jackrabbit.spi.commons.conversion.DefaultNamePathResolver;
import org.apache.jackrabbit.spi.commons.value.QValueFactoryImpl;
import org.apache.jackrabbit.spi.commons.value.QValueValue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class RepositoryTest extends AbstractRepositoryTest {
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    public RepositoryTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        ValueFactory valueFactory = session.getValueFactory();
        Node root = session.getRootNode();
        Node foo = root.addNode("foo");
        foo.setProperty("stringProp", "stringVal");
        foo.setProperty("intProp", 42);
        foo.setProperty("mvProp", new Value[]{
                valueFactory.createValue(1),
                valueFactory.createValue(2),
                valueFactory.createValue(3),
        });
        root.addNode("bar");
        root.addNode(TEST_NODE);
        session.save();
    }

    @Test
    public void createRepository() throws RepositoryException {
        Repository repository = getRepository();
        assertNotNull(repository);
    }

    @Test
    public void login() throws RepositoryException {
        assertNotNull(getAdminSession());
    }

    @Test
    public void loginWithAttribute() throws RepositoryException {
        Session session = ((JackrabbitRepository) getRepository()).login(
                new GuestCredentials(), null,
                Collections.<String, Object>singletonMap(RepositoryImpl.REFRESH_INTERVAL, 42));

        String[] attributeNames = session.getAttributeNames();
        assertEquals(1, attributeNames.length);
        assertEquals(RepositoryImpl.REFRESH_INTERVAL, attributeNames[0]);
        assertEquals(42L, session.getAttribute(RepositoryImpl.REFRESH_INTERVAL));
        session.logout();
    }

    @Test
    public void loginWithCredentialsAttribute() throws RepositoryException {
        SimpleCredentials sc = getAdminCredentials();
        sc.setAttribute("attr", "val");
        Session session = null;

        try {
            session = getRepository().login(sc, null);
            String[] attributeNames = session.getAttributeNames();
            assertEquals(1, attributeNames.length);
            assertEquals("attr", attributeNames[0]);
            assertEquals("val", session.getAttribute("attr"));
        } finally {
            if (session != null) {
                session.logout();
            }
        }
    }

    @Test(expected = NoSuchWorkspaceException.class)
    public void loginInvalidWorkspace() throws RepositoryException {
        Repository repository = getRepository();
        repository.login(new GuestCredentials(), "invalid");
    }

    @Ignore("OAK-118") // TODO OAK-118: implement workspace management
    @Test
    public void getWorkspaceNames() throws RepositoryException {
        String[] workspaces = getAdminSession().getWorkspace().getAccessibleWorkspaceNames();

        Set<String> names = new HashSet<String>() {{
            add("default");
        }};

        assertTrue(asList(workspaces).containsAll(names));
        assertTrue(names.containsAll(asList(workspaces)));
    }

    @Ignore("OAK-118") // TODO OAK-118: implement workspace management
    @Test
    public void createDeleteWorkspace() throws RepositoryException {
        getAdminSession().getWorkspace().createWorkspace("new");

        Session session2 = createAdminSession();
        try {
            String[] workspaces = session2.getWorkspace().getAccessibleWorkspaceNames();
            assertTrue(asList(workspaces).contains("new"));
            Session session3 = getRepository().login("new");
            assertEquals("new", session3.getWorkspace().getName());
            session3.logout();
            session2.getWorkspace().deleteWorkspace("new");
        } finally {
            session2.logout();
        }

        Session session4 = createAdminSession();
        try {
            String[] workspaces = session4.getWorkspace().getAccessibleWorkspaceNames();
            assertFalse(asList(workspaces).contains("new"));
        } finally {
            session4.logout();
        }
    }

    @Test
    public void getRoot() throws RepositoryException {
        Node root = getAdminSession().getRootNode();
        assertNotNull(root);
        assertEquals("", root.getName());
        assertEquals("/", root.getPath());
    }

    @Test
    public void getRootFromPath() throws RepositoryException {
        Node root = getNode("/");
        assertEquals("", root.getName());
        assertEquals("/", root.getPath());
    }

    @Test(expected = PathNotFoundException.class)
    public void getPropertyDot() throws RepositoryException {
        Node node = getNode("/foo");
        node.getProperty(".");
    }

    @Test(expected = PathNotFoundException.class)
    public void getPropertyDotDot() throws RepositoryException {
        Node node = getNode("/foo");
        node.getProperty("..");
    }

    @Test
    public void hasPropertyDot() throws RepositoryException {
        Node root = getNode("/");
        assertFalse((root.hasProperty(".")));
        Node node = getNode("/foo");
        assertFalse((node.hasProperty(".")));
    }

    @Test
    public void hasPropertyDotDot() throws RepositoryException {
        Node root = getNode("/");
        assertFalse((root.hasProperty("..")));
        Node node = getNode("/foo");
        assertFalse((node.hasProperty("..")));
    }

    @Test
    public void getNodeDot() throws RepositoryException {
        Node node = getNode("/foo");
        Node same = node.getNode(".");
        assertNotNull(same);
        assertEquals("foo", same.getName());
        assertTrue(same.isSame(node));
    }

    @Test
    public void getNodeDotDot() throws RepositoryException {
        Node node = getNode("/foo");
        Node root = node.getNode("..");
        assertNotNull(root);
        assertEquals("", root.getName());
        assertTrue(root.isSame(node.getParent()));
    }

    @Test(expected = PathNotFoundException.class)
    public void getRootGetDotDot() throws RepositoryException {
        Node root = getNode("/");
        assertNotNull(root);
        root.getNode("..");
    }

    @Test
    public void hasNodeDot() throws RepositoryException {
        Node root = getNode("/");
        assertTrue(root.hasNode("."));
        Node node = getNode("/foo");
        assertTrue(node.hasNode("."));
    }

    @Test
    public void hasNodeDotDot() throws RepositoryException {
        Node root = getNode("/");
        assertFalse(root.hasNode(".."));
        Node node = getNode("/foo");
        assertTrue(node.hasNode(".."));
    }

    @Test(expected = ItemExistsException.class)
    public void testAddNodeDot() throws RepositoryException {
        Node node = getNode("/foo");
        // add a node with '..' should fail...
        node.addNode("..");
    }

    @Test(expected = ItemExistsException.class)
    public void testAddNodeDotDot() throws RepositoryException {
        Node node = getNode("/foo");
        node.addNode(".");
    }

    @Test
    public void getNode() throws RepositoryException {
        Node node = getNode("/foo");
        assertNotNull(node);
        assertEquals("foo", node.getName());
        assertEquals("/foo", node.getPath());
    }

    /**
     * Test SNS 1-based indexed path.
     * JCR 2.0, Chapter 22.2:
     * <em>
     *     A name in a content repository path that does not explicitly specify an index implies an index of 1.
     *     For example, /a/b/c is equivalent to /a[1]/b[1]/c[1].
     * </em>
     *
     * @throws RepositoryException
     */
    @Test
    public void getNodeSNS() throws RepositoryException {
        Node node = getNode("/foo[1]");
        assertNotNull(node);
        assertEquals("foo", node.getName());
        assertEquals("/foo", node.getPath());

        node.addNode("bar");
        Node bar = getNode("/foo[1]/bar[1]");
        assertEquals("/foo/bar", bar.getPath());

        try {
            getNode("/foo[1]/bar[2]");
            fail("retrieving wrong SNS index should throw PathNotFoundException");
        } catch (PathNotFoundException e) {
            // expected.
        }

        try {
            getProperty("/foo[1]/bar[2]/jcr:primaryType");
            fail("retrieving wrong SNS index should throw PathNotFoundException");
        } catch (PathNotFoundException e) {
            // expected.
        }

        assertTrue(getAdminSession().nodeExists("/foo[1]/bar[1]"));
        assertTrue(node.hasNode("bar[1]"));
        assertTrue(node.hasProperty("bar[1]/jcr:primaryType"));
        assertTrue(getAdminSession().propertyExists("/foo[1]/bar[1]/jcr:primaryType"));
        assertFalse(getAdminSession().nodeExists("/foo[1]/bar[2]"));
        assertFalse(node.hasNode("bar[2]"));
        assertFalse(node.hasProperty("bar[2]/jcr:primaryType"));
        assertFalse(getAdminSession().propertyExists("/foo[1]/bar[2]/jcr:primaryType"));
    }

    @Test(expected = RepositoryException.class)
    public void getNodeAbsolutePath() throws RepositoryException {
        Node root = getNode("/");
        root.getNode("/foo");
    }

    @Test
    public void getNodeByIdentifier() throws RepositoryException {
        Node node = getNode("/foo");
        String id = node.getIdentifier();
        Node node2 = getAdminSession().getNodeByIdentifier(id);
        assertTrue(node.isSame(node2));
    }

    @Test
    public void getNodeByUUID() throws RepositoryException {
        Node node = getNode("/foo").addNode("boo");
        node.addMixin(JcrConstants.MIX_REFERENCEABLE);

        assertTrue(node.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        String uuid = node.getUUID();
        assertNotNull(uuid);
        assertEquals(uuid, node.getIdentifier());

        Node nAgain = node.getSession().getNodeByUUID(uuid);
        assertTrue(nAgain.isSame(node));
        assertTrue(nAgain.isSame(node.getSession().getNodeByIdentifier(uuid)));

        Node childNode = node.addNode("boohoo");
        childNode.addMixin(JcrConstants.MIX_REFERENCEABLE);

        assertTrue(childNode.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        String childUuid = childNode.getUUID();
        assertNotNull(childUuid);
        assertEquals(childUuid, childNode.getIdentifier());

        nAgain = childNode.getSession().getNodeByUUID(childUuid);
        assertTrue(nAgain.isSame(childNode));
        assertTrue(nAgain.isSame(childNode.getSession().getNodeByIdentifier(childUuid)));

        node.getSession().move("/foo/boo", "/foo/boohoo");
        node = getNode("/foo/boohoo");
        nAgain = node.getSession().getNodeByUUID(uuid);
        assertTrue(nAgain.isSame(node));
        assertTrue(nAgain.isSame(node.getSession().getNodeByIdentifier(uuid)));
    }

    @Test
    public void getRootChildByUUID() throws RepositoryException {
        Node node = getNode("/").addNode("boo");
        node.addMixin(JcrConstants.MIX_REFERENCEABLE);

        assertTrue(node.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        String uuid = node.getUUID();
        assertNotNull(uuid);
        assertEquals(uuid, node.getIdentifier());

        Node nAgain = node.getSession().getNodeByUUID(uuid);
        assertTrue(nAgain.isSame(node));
        assertTrue(nAgain.isSame(node.getSession().getNodeByIdentifier(uuid)));

        Node childNode = node.addNode("boohoo");
        childNode.addMixin(JcrConstants.MIX_REFERENCEABLE);

        assertTrue(childNode.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        String childUuid = childNode.getUUID();
        assertNotNull(childUuid);
        assertEquals(childUuid, childNode.getIdentifier());

        nAgain = childNode.getSession().getNodeByUUID(childUuid);
        assertTrue(nAgain.isSame(childNode));
        assertTrue(nAgain.isSame(childNode.getSession().getNodeByIdentifier(childUuid)));

        node.getSession().move("/boo", "/boohoo");
        node = getNode("/boohoo");
        nAgain = node.getSession().getNodeByUUID(uuid);
        assertTrue(nAgain.isSame(node));
        assertTrue(nAgain.isSame(node.getSession().getNodeByIdentifier(uuid)));
    }

    @Test
    public void getNodeFromNode() throws RepositoryException {
        Node root = getNode("/");
        Node node = root.getNode("foo");
        assertNotNull(node);
        assertEquals("foo", node.getName());
        assertEquals("/foo", node.getPath());

        Node nodeAgain = getNode("/foo");
        assertTrue(node.isSame(nodeAgain));
    }

    @Test
    public void getNodes() throws RepositoryException {
        Node test = getNode(TEST_PATH);
        test.addNode("foo");
        test.addNode("bar");
        test.addNode("baz");
        getAdminSession().save();

        Set<String> nodeNames = new HashSet<String>() {{
            add("bar");
            add("added");
            add("baz");
        }};

        test = getNode(TEST_PATH);
        test.addNode("added");         // transiently added
        test.getNode("foo").remove();  // transiently removed
        test.getNode("bar").remove();  // transiently removed and...
        test.addNode("bar");           // ... added again

        NodeIterator nodes = test.getNodes();
        assertEquals(3, nodes.getSize());

        while (nodes.hasNext()) {
            String name = nodes.nextNode().getName();
            assertTrue(nodeNames.remove(name));
        }

        assertTrue(nodeNames.isEmpty());
    }

    @Test
    public void getProperties() throws RepositoryException {
        Set<String> propertyNames = new HashSet<String>() {{
            add("intProp");
            add("mvProp");
            add("added");
        }};

        Set<String> values = new HashSet<String>() {{
            add("added");
            add("1");
            add("2");
            add("3");
            add("42");
        }};

        Node node = getNode("/foo");
        node.setProperty("added", "added");        // transiently added
        node.getProperty("stringProp").remove();   // transiently removed
        node.getProperty("intProp").remove();      // transiently removed...
        node.setProperty("intProp", 42);           // ...and added again
        PropertyIterator properties = node.getProperties();
        assertEquals(4, properties.getSize());
        while (properties.hasNext()) {
            Property p = properties.nextProperty();
            if (JcrConstants.JCR_PRIMARYTYPE.equals(p.getName())) {
                continue;
            }
            assertTrue(propertyNames.remove(p.getName()));
            if (p.isMultiple()) {
                for (Value v : p.getValues()) {
                    assertTrue(values.remove(v.getString()));
                }
            } else {
                assertTrue(values.remove(p.getString()));
            }
        }

        assertTrue(propertyNames.isEmpty());
        assertTrue(values.isEmpty());
    }

    @Test(expected = PathNotFoundException.class)
    public void getNonExistingNode() throws RepositoryException {
        getNode("/qoo");
    }

    @Test
    public void getProperty() throws RepositoryException {
        Property property = getProperty("/foo/stringProp");
        assertNotNull(property);
        assertEquals("stringProp", property.getName());
        assertEquals("/foo/stringProp", property.getPath());

        Value value = property.getValue();
        assertNotNull(value);
        assertEquals(PropertyType.STRING, value.getType());
        assertEquals("stringVal", value.getString());
    }

    @Test
    public void getPropertyFromNode() throws RepositoryException {
        Node node = getNode("/foo");
        Property property = node.getProperty("stringProp");
        assertNotNull(property);
        assertEquals("stringProp", property.getName());
        assertEquals("/foo/stringProp", property.getPath());

        Value value = property.getValue();
        assertNotNull(value);
        assertEquals(PropertyType.STRING, value.getType());
        assertEquals("stringVal", value.getString());

        Property propertyAgain = getProperty("/foo/stringProp");
        assertTrue(property.isSame(propertyAgain));
    }

    @Test
    public void addNode() throws RepositoryException {
        Session session = getAdminSession();
        String newNode = TEST_PATH + "/new";
        assertFalse(session.nodeExists(newNode));

        Node node = getNode(TEST_PATH);
        Node added = node.addNode("new");
        assertFalse(node.isNew());
        assertTrue(node.isModified());
        assertTrue(added.isNew());
        assertFalse(added.isModified());
        session.save();

        Session session2 = createAnonymousSession();
        try {
            assertTrue(session2.nodeExists(newNode));
            added = session2.getNode(newNode);
            assertFalse(added.isNew());
            assertFalse(added.isModified());
        } finally {
            session2.logout();
        }
    }

    @Test
    public void testIsNew() throws RepositoryException, InterruptedException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        Node node1 = root.addNode("node1");
        session.save();

        node1.remove();
        Node node2 = root.addNode("node2");
        assertTrue("The Node is just added", node2.isNew());
        Node node1Again = root.addNode("node1");
        assertTrue("The Node is just added but has a remove in same commit", node1Again.isNew());
    }

    @Test
    public void testAddNodeWithExpandedName() throws RepositoryException {
        Session session = getAdminSession();

        session.getWorkspace().getNamespaceRegistry().registerNamespace("foo", "http://foo");

        Node node = getNode(TEST_PATH);
        Node added = node.addNode("{http://foo}new");
        assertEquals("foo:new", added.getName());
        session.save();

        added = session.getNode(TEST_PATH + "/{http://foo}new");
        assertEquals("foo:new", added.getName());
    }

    @Test
    public void addNodeWithSpecialChars() throws RepositoryException {
        Session session = getAdminSession();
        String nodeName = "foo{";

        String newNode = TEST_PATH + '/' + nodeName;
        assertFalse(session.nodeExists(newNode));

        Node node = getNode(TEST_PATH);
        node.addNode(nodeName);
        session.save();

        Session session2 = createAnonymousSession();
        try {
            assertTrue(session2.nodeExists(newNode));
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addNodeWithNodeType() throws RepositoryException {
        Session session = getAdminSession();

        Node node = getNode(TEST_PATH);
        node.addNode("new", "nt:folder");
        session.save();
    }

    @Test
    public void addNodeToRootNode() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        String newNode = "newNodeBelowRoot";
        assertFalse(root.hasNode(newNode));
        root.addNode(newNode);
        session.save();
    }

    @Test
    public void addStringProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "string", getAdminSession().getValueFactory().createValue("string value"));
    }

    @Test
    public void testGetItemReturnsNodeBeforeProperty() throws RepositoryException {
        String snnpSupported = getRepository().getDescriptor(OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED);
        assumeTrue(Boolean.valueOf(snnpSupported));

        String newNodeName = "getItemTest";
        String nodeAndPropertyName = "subnode";

        Session session = getAdminSession();
        Node root = session.getRootNode();
        assertFalse(root.hasNode(newNodeName));
        Node newNode = root.addNode(newNodeName, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        newNode.setProperty(nodeAndPropertyName, "prop value");
        newNode.addNode(nodeAndPropertyName);
        session.save();

        Item item = session.getItem("/" + newNodeName + "/" + nodeAndPropertyName);

        assertTrue("should retrieve Node before property", item.isNode());
    }


    @Test
    public void addMultiValuedString() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue("one");
        values[1] = getAdminSession().getValueFactory().createValue("two");

        parentNode.setProperty("multi string", values);
        parentNode.getSession().save();

        Session session2 = createAdminSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi string");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.STRING, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addLongProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "long", getAdminSession().getValueFactory().createValue(42L));
    }

    @Test
    public void addMultiValuedLong() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue(42L);
        values[1] = getAdminSession().getValueFactory().createValue(84L);

        parentNode.setProperty("multi long", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi long");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.LONG, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addDoubleProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "double", getAdminSession().getValueFactory().createValue(42.2D));
    }

    @Test
    public void addMultiValuedDouble() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue(42.1d);
        values[1] = getAdminSession().getValueFactory().createValue(99.0d);

        parentNode.setProperty("multi double", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi double");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.DOUBLE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addBooleanProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "boolean", getAdminSession().getValueFactory().createValue(true));
    }

    @Test
    public void addMultiValuedBoolean() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue(true);
        values[1] = getAdminSession().getValueFactory().createValue(false);

        parentNode.setProperty("multi boolean", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi boolean");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BOOLEAN, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addDecimalProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "decimal", getAdminSession().getValueFactory().createValue(BigDecimal.valueOf(21)));
    }

    @Test
    public void addMultiValuedDecimal() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue(BigDecimal.valueOf(42));
        values[1] = getAdminSession().getValueFactory().createValue(BigDecimal.valueOf(99));

        parentNode.setProperty("multi decimal", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi decimal");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.DECIMAL, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addDateProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "date", getAdminSession().getValueFactory().createValue(Calendar.getInstance()));
    }

    @Test
    public void addMultiValuedDate() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        Calendar calendar = Calendar.getInstance();
        values[0] = getAdminSession().getValueFactory().createValue(calendar);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        values[1] = getAdminSession().getValueFactory().createValue(calendar);

        parentNode.setProperty("multi date", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi date");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.DATE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addURIProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "uri", getAdminSession().getValueFactory().createValue("http://www.day.com/", PropertyType.URI));
    }

    @Test
    public void addMultiValuedURI() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue("http://www.day.com", PropertyType.URI);
        values[1] = getAdminSession().getValueFactory().createValue("file://var/dam", PropertyType.URI);

        parentNode.setProperty("multi uri", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi uri");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.URI, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addNameProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "name", getAdminSession().getValueFactory().createValue("jcr:something\"", PropertyType.NAME));
    }

    @Test
    public void addMultiValuedName() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue("jcr:foo", PropertyType.NAME);
        values[1] = getAdminSession().getValueFactory().createValue("bar", PropertyType.NAME);

        parentNode.setProperty("multi name", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi name");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.NAME, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addEmptyMultiValue() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[0];

        parentNode.setProperty("multi value", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi value");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.STRING, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(0, values2.length);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addEmptyMultiValueName() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[0];

        parentNode.setProperty("multi name", values, PropertyType.NAME);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi name");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.NAME, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(0, values2.length);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addPathProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "path", getAdminSession().getValueFactory().createValue("/jcr:foo/bar\"", PropertyType.PATH));
    }

    @Test
    public void addMultiValuedPath() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue("/nt:foo/jcr:bar", PropertyType.PATH);
        values[1] = getAdminSession().getValueFactory().createValue("/", PropertyType.PATH);

        parentNode.setProperty("multi path", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi path");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.PATH, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addBinaryProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        InputStream is = new ByteArrayInputStream("foo\"".getBytes());
        Binary bin = getAdminSession().getValueFactory().createBinary(is);
        addProperty(parentNode, "binary", getAdminSession().getValueFactory().createValue(bin));
    }

    @Test
    public void addSmallBinaryProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        InputStream is = new NumberStream(1234);
        Binary bin = getAdminSession().getValueFactory().createBinary(is);
        addProperty(parentNode, "bigBinary", getAdminSession().getValueFactory().createValue(bin));
    }

    @Test
    public void addBigBinaryProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        InputStream is = new NumberStream(123456);
        Binary bin = getAdminSession().getValueFactory().createBinary(is);
        addProperty(parentNode, "bigBinary", getAdminSession().getValueFactory().createValue(bin));
    }

    @Test
    public void addAlienBinaryProperty() throws RepositoryException, IOException {
        Session session = getAdminSession();
        QValue qValue = QValueFactoryImpl.getInstance().create("binaryValue".getBytes());
        Value value = new QValueValue(qValue, new DefaultNamePathResolver(session));
        getNode(TEST_PATH).setProperty("binary", value);
        session.save();

        Value valueAgain = getNode(TEST_PATH).getProperty("binary").getValue();
        assertEqualStream(value.getBinary().getStream(), valueAgain.getBinary().getStream());
    }

    @Test
    public void addMultiValuedBinary() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];

        InputStream is = new ByteArrayInputStream("foo".getBytes());
        Binary bin = getAdminSession().getValueFactory().createBinary(is);
        values[0] = getAdminSession().getValueFactory().createValue(bin);

        is = new ByteArrayInputStream("bar".getBytes());
        bin = getAdminSession().getValueFactory().createBinary(is);
        values[1] = getAdminSession().getValueFactory().createValue(bin);

        parentNode.setProperty("multi binary", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi binary");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BINARY, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addReferenceProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getAdminSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getAdminSession().save();

        addProperty(parentNode, "reference", getAdminSession().getValueFactory().createValue(referee));
    }

    @Test
    public void addMultiValuedReference() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getAdminSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getAdminSession().save();

        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue(referee);
        values[1] = getAdminSession().getValueFactory().createValue(referee);

        parentNode.setProperty("multi reference", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi reference");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.REFERENCE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addWeakReferenceProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getAdminSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getAdminSession().save();

        addProperty(parentNode, "weak reference", getAdminSession().getValueFactory().createValue(referee, true));
    }

    @Test
    public void addMultiValuedWeakReference() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getAdminSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getAdminSession().save();

        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue(referee, true);
        values[1] = getAdminSession().getValueFactory().createValue(referee, true);

        parentNode.setProperty("multi weak reference", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi weak reference");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.WEAKREFERENCE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addEmptyMultiValuedProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[0];

        parentNode.setProperty("multi empty", values, PropertyType.BOOLEAN);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi empty");
            assertTrue(property.isMultiple());
            Value[] values2 = property.getValues();
            assertEquals(0, values2.length);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void addMultiValuedStringWithNull() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[3];
        values[0] = getAdminSession().getValueFactory().createValue(true);
        values[2] = getAdminSession().getValueFactory().createValue(false);

        parentNode.setProperty("multi with null", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi with null");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BOOLEAN, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length - 1, values2.length);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void transientChanges() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);

        Node node = parentNode.addNode("test");

        assertFalse(node.hasProperty("p"));
        node.setProperty("p", "pv");
        assertTrue(node.hasProperty("p"));

        assertFalse(node.hasNode("n"));
        node.addNode("n");
        assertTrue(node.hasNode("n"));

        assertTrue(node.hasProperties());
        assertTrue(node.hasNodes());
    }

    @Test
    public void setStringProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "string", getAdminSession().getValueFactory().createValue("string \" value"));

        Property property = parentNode.getProperty("string");
        property.setValue("new value");
        assertTrue(parentNode.isModified());
        assertTrue(property.isModified());
        assertFalse(property.isNew());
        property.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property2 = session2.getProperty(TEST_PATH + "/string");
            assertEquals("new value", property2.getString());
        } finally {
            session2.logout();
        }
    }

    @Test
    public void setPropertyAgain() throws RepositoryException {
        Session session = getAdminSession();
        Property p1 = session.getProperty("/foo/stringProp");

        Property p2 = p1.getParent().setProperty("stringProp", "newValue");
        Property p3 = session.getProperty("/foo/stringProp");

        assertEquals("newValue", p1.getString());
        assertEquals("newValue", p2.getString());
        assertEquals("newValue", p3.getString());
    }

    // OAK-6410
    @Test
    public void setInexistentProperty() throws RepositoryException {
        Node node = getNode(TEST_PATH);
        node.addMixin(JcrConstants.MIX_VERSIONABLE);
        node.getSession().save();
        node.getSession().getWorkspace().getVersionManager().checkin(TEST_PATH);
        node.setProperty("inexistent", (Value) null);
    }

    @Test
    public void setDoubleNaNProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "NaN", getAdminSession().getValueFactory().createValue(Double.NaN));

        Session session2 = createAnonymousSession();
        try {
            Property property2 = session2.getProperty(TEST_PATH + "/NaN");
            assertTrue(Double.isNaN(property2.getDouble()));
        } finally {
            session2.logout();
        }
    }

    @Test
    public void setMultiValuedProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getAdminSession().getValueFactory().createValue("one");
        values[1] = getAdminSession().getValueFactory().createValue("two");

        parentNode.setProperty("multi string2", values);
        parentNode.getSession().save();

        values[0] = getAdminSession().getValueFactory().createValue("eins");
        values[1] = getAdminSession().getValueFactory().createValue("zwei");
        parentNode.setProperty("multi string2", values);
        parentNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi string2");
            assertTrue(property.isMultiple());
            Value[] values2 = property.getValues();
            assertEquals(2, values.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        } finally {
            session2.logout();
        }
    }

    @Test
    public void nullProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        parentNode.setProperty("newProperty", "some value");
        parentNode.getSession().save();

        Session session2 = createAdminSession();
        try {
            session2.getProperty(TEST_PATH + "/newProperty").setValue((String) null);
            session2.save();
        } finally {
            session2.logout();
        }

        Session session3 = createAnonymousSession();
        try {
            assertFalse(session3.propertyExists(TEST_PATH + "/newProperty"));
        } finally {
            session3.logout();
        }
    }

    @Test
    public void setPropertyWithConversion() throws RepositoryException {
        Node n = getNode(TEST_PATH);
        Node file = n.addNode("file", "nt:file");
        Node content = file.addNode("jcr:content", "nt:resource");

        long value = System.currentTimeMillis();
        Property property = content.setProperty("jcr:lastModified", value);
        assertEquals(value, property.getDate().getTimeInMillis());

        content.setProperty("jcr:data", new ByteArrayInputStream("foo".getBytes()));
        content.setProperty("jcr:mimeType", "text/plain");
        n.getSession().save();
    }

    @Test
    public void removeProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        parentNode.setProperty("newProperty", "some value");
        parentNode.getSession().save();

        Session session2 = createAdminSession();
        try {
            session2.getProperty(TEST_PATH + "/newProperty").remove();
            session2.save();
        } finally {
            session2.logout();
        }

        Session session3 = createAnonymousSession();
        try {
            assertFalse(session3.propertyExists(TEST_PATH + "/newProperty"));
        } finally {
            session3.logout();
        }
    }

    @Test
    public void removeNode() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        parentNode.addNode("newNode");
        parentNode.getSession().save();

        Session session2 = createAdminSession();
        try {
            Node removeNode = session2.getNode(TEST_PATH + "/newNode");
            removeNode.remove();

            try {
                removeNode.getParent();
                fail("Cannot retrieve the parent from a transiently removed item.");
            } catch (InvalidItemStateException expected) {
            }

            assertTrue(session2.getNode(TEST_PATH).isModified());
            session2.save();
        } finally {
            session2.logout();
        }

        Session session3 = createAnonymousSession();
        try {
            assertFalse(session3.nodeExists(TEST_PATH + "/newNode"));
            assertFalse(session3.getNode(TEST_PATH).isModified());
        } finally {
            session3.logout();
        }
    }

    @Test
    public void removeNode2() throws RepositoryException {
        Node foo = getNode("/foo");
        getAdminSession().removeItem(foo.getPath());
        try {
            foo.getParent();
            fail("Cannot retrieve the parent from a transiently removed item.");
        } catch (InvalidItemStateException e) {
            // success
        }
    }

    @Test
    public void accessRemovedItem() throws RepositoryException {
        Node foo = getNode("/foo");
        Node bar = foo.addNode("bar");
        Property p = bar.setProperty("name", "value");
        foo.remove();
        try {
            bar.getPath();
            fail("Expected InvalidItemStateException");
        }
        catch (InvalidItemStateException expected) {
        }
        try {
            p.getPath();
            fail("Expected InvalidItemStateException");
        }
        catch (InvalidItemStateException expected) {
        }
    }

    @Test
    public void accessRemovedProperty() throws RepositoryException {
        Node foo = getNode("/foo");
        Property p = foo.setProperty("name", "value");
        p.remove();
        try {
            p.getPath();
            fail("Expected InvalidItemStateException");
        }
        catch (InvalidItemStateException expected) {
        }
    }

    @Test
    public void getReferences() throws RepositoryException {
        Session session = getAdminSession();
        Node referee = getNode("/foo");
        referee.addMixin("mix:referenceable");
        getNode(TEST_PATH).setProperty("reference", session.getValueFactory().createValue(referee));
        session.save();

        PropertyIterator refs = referee.getReferences();
        assertTrue(refs.hasNext());
        Property p = refs.nextProperty();
        assertEquals("reference", p.getName());
        assertFalse(refs.hasNext());
    }

    @Test
    public void getNamedReferences() throws RepositoryException {
        Session session = getAdminSession();
        Node referee = getNode("/foo");
        referee.addMixin("mix:referenceable");
        Value value = session.getValueFactory().createValue(referee);
        getNode(TEST_PATH).setProperty("reference1", value);
        getNode("/bar").setProperty("reference2", value);
        session.save();

        PropertyIterator refs = referee.getReferences("reference1");
        assertTrue(refs.hasNext());
        Property p = refs.nextProperty();
        assertEquals("reference1", p.getName());
        assertFalse(refs.hasNext());
    }

    @Test
    public void getWeakReferences() throws RepositoryException {
        Session session = getAdminSession();
        Node referee = getNode("/foo");
        referee.addMixin("mix:referenceable");
        getNode(TEST_PATH).setProperty("weak-reference", session.getValueFactory().createValue(referee, true));
        session.save();

        PropertyIterator refs = referee.getWeakReferences();
        assertTrue(refs.hasNext());
        Property p = refs.nextProperty();
        assertEquals("weak-reference", p.getName());
        assertFalse(refs.hasNext());
    }

    @Test
    public void getNamedWeakReferences() throws RepositoryException {
        Session session = getAdminSession();
        Node referee = getNode("/foo");
        referee.addMixin("mix:referenceable");
        Value value = session.getValueFactory().createValue(referee, true);
        getNode(TEST_PATH).setProperty("weak-reference1", value);
        getNode("/bar").setProperty("weak-reference2", value);
        session.save();

        PropertyIterator refs = referee.getWeakReferences("weak-reference1");
        assertTrue(refs.hasNext());
        Property p = refs.nextProperty();
        assertEquals("weak-reference1", p.getName());
        assertFalse(refs.hasNext());
    }

    @Test
    public void sessionSave() throws RepositoryException {
        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        try {
            // Add some items and ensure they are accessible through this session
            session1.getNode("/").addNode("node1");
            session1.getNode("/node1").addNode("node2");
            session1.getNode("/").addNode("node1/node3");

            Node node1 = session1.getNode("/node1");
            assertEquals("/node1", node1.getPath());

            Node node2 = session1.getNode("/node1/node2");
            assertEquals("/node1/node2", node2.getPath());

            Node node3 = session1.getNode("/node1/node3");
            assertEquals("/node1/node3", node3.getPath());

            node3.setProperty("property1", "value1");
            Item property1 = session1.getProperty("/node1/node3/property1");
            assertFalse(property1.isNode());
            assertEquals("value1", ((Property) property1).getString());

            // Make sure these items are not accessible through another session
            assertFalse(session2.itemExists("/node1"));
            assertFalse(session2.itemExists("/node1/node2"));
            assertFalse(session2.itemExists("/node1/node3"));
            assertFalse(session2.itemExists("/node1/node3/property1"));

            session1.save();

            // Make sure they are accessible through another session
            assertTrue(session2.itemExists("/node1"));
            assertTrue(session2.itemExists("/node1/node2"));
            assertTrue(session2.itemExists("/node1/node3"));
            assertTrue(session2.itemExists("/node1/node3/property1"));

            session2.refresh(false);

            // Make sure they are accessible through another session after refresh
            assertTrue(session2.itemExists("/node1"));
            assertTrue(session2.itemExists("/node1/node2"));
            assertTrue(session2.itemExists("/node1/node3"));
            assertTrue(session2.itemExists("/node1/node3/property1"));
        } finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void sessionRefresh() throws RepositoryException {
        Session session = getAdminSession();
        // Add some items and ensure they are accessible through this session
        session.getNode("/").addNode("node1");
        session.getNode("/node1").addNode("node2");
        session.getNode("/").addNode("node1/node3");

        Node node1 = session.getNode("/node1");
        assertEquals("/node1", node1.getPath());

        Node node2 = session.getNode("/node1/node2");
        assertEquals("/node1/node2", node2.getPath());

        Node node3 = session.getNode("/node1/node3");
        assertEquals("/node1/node3", node3.getPath());

        node3.setProperty("property1", "value1");
        Item property1 = session.getProperty("/node1/node3/property1");
        assertFalse(property1.isNode());
        assertEquals("value1", ((Property) property1).getString());

        // Make sure these items are still accessible after refresh(true);
        session.refresh(true);
        assertTrue(session.itemExists("/node1"));
        assertTrue(session.itemExists("/node1/node2"));
        assertTrue(session.itemExists("/node1/node3"));
        assertTrue(session.itemExists("/node1/node3/property1"));

        session.refresh(false);
        // Make sure these items are not accessible after refresh(false);
        assertFalse(session.itemExists("/node1"));
        assertFalse(session.itemExists("/node1/node2"));
        assertFalse(session.itemExists("/node1/node3"));
        assertFalse(session.itemExists("/node1/node3/property1"));
    }

    @Test
    public void sessionRefreshFalse() throws RepositoryException {
        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        try {
            Node foo = session1.getNode("/foo");
            foo.addNode("added");

            session2.getNode("/foo").addNode("bar");
            session2.save();

            session1.refresh(false);
            assertFalse(foo.hasNode("added"));
            assertTrue(foo.hasNode("bar"));
        } finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void sessionRefreshTrue() throws RepositoryException {
        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        try {
            Node foo = session1.getNode("/foo");
            Node added = foo.addNode("added");
            assertTrue(added.isNew());

            session2.getNode("/foo").addNode("bar");
            session2.save();

            session1.refresh(true);
            assertTrue(foo.hasNode("added"));
            assertTrue(foo.getNode("added").isNew());
            assertTrue(foo.hasNode("bar"));
        } finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void sessionIsolation() throws RepositoryException {
        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        try {
            session1.getRootNode().addNode("node1");
            session2.getRootNode().addNode("node2");
            assertTrue(session1.getRootNode().hasNode("node1"));
            assertTrue(session2.getRootNode().hasNode("node2"));
            assertFalse(session1.getRootNode().hasNode("node2"));
            assertFalse(session2.getRootNode().hasNode("node1"));

            session1.save();
            session2.save();
            assertTrue(session1.getRootNode().hasNode("node1"));
            assertTrue(session1.getRootNode().hasNode("node2"));
            assertTrue(session2.getRootNode().hasNode("node1")); // save refreshes
            assertTrue(session2.getRootNode().hasNode("node2"));
        } finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void inThreadSessionSynchronisation() throws RepositoryException {
        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        Session session3 = createAdminSession();
        try {
            session1.getRootNode().addNode("newNode");
            session1.save();
            assertTrue(session2.nodeExists("/newNode"));
            assertTrue(session3.nodeExists("/newNode"));
        } finally {
            session1.logout();
            session2.logout();
            session3.logout();
        }
    }

    @Test
    public void saveRefreshConflict() throws RepositoryException {
        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        try {
            session1.getRootNode().addNode("node").setProperty("p", "v1");
            session2.getRootNode().addNode("node").setProperty("p", "v2");
            assertTrue(session1.getRootNode().hasNode("node"));
            assertTrue(session2.getRootNode().hasNode("node"));

            session1.save();
            assertTrue(session1.getRootNode().hasNode("node"));
            assertTrue(session2.getRootNode().hasNode("node"));

            session2.refresh(true);
            assertTrue(session1.getRootNode().hasNode("node"));
            assertTrue(session2.getRootNode().hasNode("node"));

            try {
                session2.save();
                fail("Expected InvalidItemStateException");
            } catch (InvalidItemStateException expected) {
            }
        } finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void saveConflict() throws RepositoryException {
        getAdminSession().getRootNode().addNode("node");
        getAdminSession().save();

        Session session1 = createAdminSession();
        Session session2 = createAdminSession();
        try {
            session1.getNode("/node").remove();
            session2.getNode("/node").addNode("2");
            assertFalse(session1.getRootNode().hasNode("node"));
            assertTrue(session2.getRootNode().hasNode("node"));
            assertTrue(session2.getRootNode().getNode("node").hasNode("2"));

            session1.save();
            assertFalse(session1.getRootNode().hasNode("node"));
            assertFalse(session2.getRootNode().hasNode("node"));

            try {
                session2.save();
                fail("Expected InvalidItemStateException");
            } catch (InvalidItemStateException expected) {
            }
        } finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void liveNodes() throws RepositoryException {
        Session session = getAdminSession();
        Node n1 = (Node) session.getItem(TEST_PATH);
        Node n2 = (Node) session.getItem(TEST_PATH);

        Node c1 = n1.addNode("c1");
        Node c2 = n2.addNode("c2");
        assertTrue(n1.hasNode("c1"));
        assertTrue(n1.hasNode("c2"));
        assertTrue(n2.hasNode("c1"));
        assertTrue(n2.hasNode("c2"));

        c1.remove();
        assertFalse(n1.hasNode("c1"));
        assertTrue(n1.hasNode("c2"));
        assertFalse(n2.hasNode("c1"));
        assertTrue(n2.hasNode("c2"));

        c2.remove();
        assertFalse(n1.hasNode("c1"));
        assertFalse(n1.hasNode("c2"));
        assertFalse(n2.hasNode("c1"));
        assertFalse(n2.hasNode("c2"));
    }

    @Test
    public void move() throws RepositoryException {
        Session session = getAdminSession();

        Node node = getNode(TEST_PATH);
        Node source = node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

        session.refresh(true);
        session.move(TEST_PATH + "/source/node", TEST_PATH + "/target/moved");
        assertEquals(TEST_PATH + "/target/moved", source.getPath());

        assertFalse(node.hasNode("source/node"));
        assertTrue(node.hasNode("source"));
        assertTrue(node.hasNode("target/moved"));

        session.save();

        assertFalse(node.hasNode("source/node"));
        assertTrue(node.hasNode("source"));
        assertTrue(node.hasNode("target/moved"));
    }

    @Test
    public void renameNonOrderable() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        Node parent = root.addNode("parent", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        parent.addNode("fo");
        Node foo = parent.addNode("foo");
        session.save();

        ((JackrabbitNode) foo).rename("renamed");
        assertEquals("renamed", foo.getName());
        assertFalse(session.nodeExists("/parent/foo"));
        assertTrue(session.nodeExists("/parent/renamed"));
    }

    @Test(expected = RepositoryException.class)
    public void moveToDescendant() throws RepositoryException {
        Session session = getAdminSession();

        Node node = getNode(TEST_PATH);
        Node source = node.addNode("s");
        session.save();

        session.refresh(true);
        session.move(TEST_PATH + "/s", TEST_PATH + "/s/t");
    }

    @Test
    public void oak962() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode().addNode("root");
        root.addNode("N3");
        root.addNode("N6").addNode("N7");
        session.save();
        session.move("/root/N6/N7", "/root/N3/N12");
        root.getNode("N3").getNode("N12").remove();
        root.getNode("N6").remove();
        session.save();
    }

    @Test
    public void moveReferenceable() throws RepositoryException {
        Session session = getAdminSession();

        Node node = getNode(TEST_PATH);
        node.addNode("source").addNode("node").addMixin("mix:referenceable");
        node.addNode("target");
        session.save();

        session.refresh(true);
        session.move(TEST_PATH + "/source/node", TEST_PATH + "/target/moved");

        assertFalse(node.hasNode("source/node"));
        assertTrue(node.hasNode("source"));
        assertTrue(node.hasNode("target/moved"));

        session.save();

        assertFalse(node.hasNode("source/node"));
        assertTrue(node.hasNode("source"));
        assertTrue(node.hasNode("target/moved"));
    }

    @Test
    public void workspaceMove() throws RepositoryException {
        Session session = getAdminSession();

        Node node = getNode(TEST_PATH);
        node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

        session.getWorkspace().move(TEST_PATH + "/source/node", TEST_PATH + "/target/moved");

        assertFalse(node.hasNode("source/node"));
        assertTrue(node.hasNode("source"));
        assertTrue(node.hasNode("target/moved"));
    }

    @Test
    public void invalidItemStateExceptionOnRemovedNode() throws Exception {
        Session session = getAdminSession();

        for (String parentPath : new String[] {"/", TEST_PATH}) {
            Node parent = session.getNode(parentPath);
            Node child = parent.addNode("child");
            String childPath = child.getPath();

            child.remove();
            try {
                child.getPath();
                fail();
            }
            catch (InvalidItemStateException expected) { }

            session.save();
            try {
                child.getPath();
                fail();
            }
            catch (InvalidItemStateException expected) { }

            parent.addNode("child");
            assertEquals(childPath, child.getPath());
        }
    }

    @Test
    public void setPrimaryType() throws RepositoryException {
        Node testNode = getNode(TEST_PATH);
        assertEquals("nt:unstructured", testNode.getPrimaryNodeType().getName());
        assertEquals("nt:unstructured", testNode.getProperty("jcr:primaryType").getString());

        testNode.setPrimaryType("nt:folder");
        getAdminSession().save();

        Session session2 = createAnonymousSession();
        try {
            testNode = session2.getNode(TEST_PATH);
            assertEquals("nt:folder", testNode.getPrimaryNodeType().getName());
            assertEquals("nt:folder", testNode.getProperty("jcr:primaryType").getString());
        } finally {
            session2.logout();
        }
    }

    @Test
    public void setPrimaryTypeShouldFail() throws RepositoryException {
        Node testNode = getNode(TEST_PATH);
        assertEquals("nt:unstructured", testNode.getPrimaryNodeType().getName());
        assertEquals("nt:unstructured", testNode.getProperty("jcr:primaryType").getString());

        // create subnode that would not be allowed with nt:folder
        testNode.addNode("unstructured_child", NodeType.NT_UNSTRUCTURED);
        getAdminSession().save();

        testNode.setPrimaryType("nt:folder");
        try {
            getAdminSession().save();
            fail("Changing the primary type to nt:folder should fail.");
        } catch (RepositoryException e) {
            // ok
            getAdminSession().refresh(false);
        }

        Session session2 = createAnonymousSession();
        try {
            testNode = session2.getNode(TEST_PATH);
            assertEquals("nt:unstructured", testNode.getPrimaryNodeType().getName());
            assertEquals("nt:unstructured", testNode.getProperty("jcr:primaryType").getString());
        } finally {
            session2.logout();
        }
    }

    @Test
    public void nodeTypeRegistry() throws RepositoryException {
        NodeTypeManager ntMgr = getAdminSession().getWorkspace().getNodeTypeManager();
        assertFalse(ntMgr.hasNodeType("foo"));

        NodeTypeTemplate ntd = ntMgr.createNodeTypeTemplate();
        ntd.setName("foo");
        ntMgr.registerNodeType(ntd, false);
        assertTrue(ntMgr.hasNodeType("foo"));

        ntMgr.unregisterNodeType("foo");
        assertFalse(ntMgr.hasNodeType("foo"));
    }

    @Test
    public void testNamespaceRegistry() throws RepositoryException {
        NamespaceRegistry nsReg =
                getAdminSession().getWorkspace().getNamespaceRegistry();
        assertFalse(asList(nsReg.getPrefixes()).contains("foo"));
        assertFalse(asList(nsReg.getURIs()).contains("file:///foo"));

        nsReg.registerNamespace("foo", "file:///foo");
        assertTrue(asList(nsReg.getPrefixes()).contains("foo"));
        assertTrue(asList(nsReg.getURIs()).contains("file:///foo"));

        nsReg.unregisterNamespace("foo");
        assertFalse(asList(nsReg.getPrefixes()).contains("foo"));
        assertFalse(asList(nsReg.getURIs()).contains("file:///foo"));
    }

    @Test
    public void sessionRemappedNamespace() throws RepositoryException {
        NamespaceRegistry nsReg =
                getAdminSession().getWorkspace().getNamespaceRegistry();
        nsReg.registerNamespace("foo", "file:///foo");

        getAdminSession().getRootNode().addNode("foo:test");
        getAdminSession().save();

        Session s = createAdminSession();
        s.setNamespacePrefix("bar", "file:///foo");
        assertTrue(s.getRootNode().hasNode("bar:test"));
        Node n = s.getRootNode().getNode("bar:test");
        assertEquals("bar:test", n.getName());
        s.logout();

        getAdminSession().getRootNode().getNode("foo:test").remove();
        getAdminSession().save();
        nsReg.unregisterNamespace("foo");
    }

    @Test
    public void registryRemappedNamespace() throws RepositoryException {
        NamespaceRegistry nsReg =
                getAdminSession().getWorkspace().getNamespaceRegistry();
        nsReg.registerNamespace("foo", "file:///foo");

        getAdminSession().getRootNode().addNode("foo:test");
        getAdminSession().save();

        try {
            nsReg.registerNamespace("bar", "file:///foo");
            fail("Remapping namespace through NamespaceRegistry must not be allowed");
        } catch (NamespaceException e) {
            // expected
        } finally {
            getAdminSession().getRootNode().getNode("foo:test").remove();
            getAdminSession().save();

            nsReg.unregisterNamespace("foo");
        }
    }

    @Test  // Regression test for OAK-299
    public void importNodeType() throws RepositoryException, IOException, ParseException {
        Session session = getAdminSession();
        NodeTypeManager manager = session.getWorkspace().getNodeTypeManager();
        if (!manager.hasNodeType("myNodeType")) {
            StringBuilder defs = new StringBuilder();
            defs.append("[\"myNodeType\"]\n");
            defs.append("  - prop1\n");
            defs.append("  + * (nt:base) = nt:unstructured \n");
            Reader cndReader = new InputStreamReader(new ByteArrayInputStream(defs.toString().getBytes()));
            CndImporter.registerNodeTypes(cndReader, session);
        }

        NodeType myNodeType = manager.getNodeType("myNodeType");
        assertTrue(myNodeType.isNodeType("nt:base"));
    }

    @Test
    public void mixin() throws RepositoryException {
        NodeTypeManager ntMgr = getAdminSession().getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixTest = ntMgr.createNodeTypeTemplate();
        mixTest.setName("mix:test");
        mixTest.setMixin(true);
        ntMgr.registerNodeType(mixTest, false);

        Node testNode = getNode(TEST_PATH);
        NodeType[] mix = testNode.getMixinNodeTypes();
        assertEquals(0, mix.length);

        testNode.addMixin("mix:test");
        testNode.getSession().save();

        Session session2 = createAnonymousSession();
        try {
            mix = session2.getNode(TEST_PATH).getMixinNodeTypes();
            assertEquals(1, mix.length);
            assertEquals("mix:test", mix[0].getName());
        } finally {
            session2.logout();
        }

        testNode.removeMixin("mix:test");
    }

    @Test
    public void liveNode() throws RepositoryException {
        Session session = getAdminSession();

        Node n1 = session.getNode(TEST_PATH);
        Node n2 = session.getNode(TEST_PATH);
        assertTrue(n1.isSame(n2));

        Node c1 = n1.addNode("c1");
        n1.setProperty("p1", "v1");
        c1.setProperty("pc1", "vc1");

        Node c2 = n2.addNode("c2");
        n2.setProperty("p2", "v2");
        c2.setProperty("pc2", "vc2");

        assertTrue(c1.isSame(n2.getNode("c1")));
        assertTrue(c2.isSame(n1.getNode("c2")));

        assertTrue(n1.hasNode("c1"));
        assertTrue(n1.hasNode("c2"));
        assertTrue(n1.hasProperty("p1"));
        assertTrue(n1.hasProperty("p2"));
        assertTrue(c1.hasProperty("pc1"));
        assertFalse(c1.hasProperty("pc2"));

        assertTrue(n2.hasNode("c1"));
        assertTrue(n2.hasNode("c2"));
        assertTrue(n2.hasProperty("p1"));
        assertTrue(n2.hasProperty("p2"));
        assertFalse(c2.hasProperty("pc1"));
        assertTrue(c2.hasProperty("pc2"));
    }

    @Test
    public void expandedName() throws RepositoryException {
        Session session = getAdminSession();
        session.setNamespacePrefix("foo", "http://example.com/");
        session.getRootNode().addNode("{0} test");
        session.save();
        assertTrue(session.nodeExists("/{0} test"));
    }

    @Test
    public void testGetDefinitionWithSNS() throws RepositoryException, IOException {
        Session session = getAdminSession();
        Node node = session.getNode("/jcr:system/jcr:nodeTypes/nt:file");
        // TODO: use getNode("jcr:childNodeDefinition[1]") once that works
        for (Node child : getChildNodes(node, "jcr:childNodeDefinition")) {
            NodeDefinition definition = child.getDefinition(); // OAK-829
            definition.getDefaultPrimaryType();                // OAK-826
            definition.getRequiredPrimaryTypes();              // OAK-826
        }
    }

    @Test
    public void workspaceCopyWithReferences() throws RepositoryException {
        Session session = getAdminSession();
        ValueFactory vf = session.getValueFactory();
        Node root = session.getRootNode();
        Node other = root.addNode("other");
        other.addMixin("mix:referenceable");
        Node src = root.addNode("src");
        Node test = src.addNode("test");
        test.addMixin("mix:referenceable");
        src.setProperty("test", test);
        src.setProperty("other", other);
        src.setProperty("multi", new Value[]{vf.createValue(test), vf.createValue(other)});
        session.save();
        session.getWorkspace().copy("/src", "/dest");
        Node dest = root.getNode("dest");
        assertEquals("/dest/test", dest.getProperty("test").getNode().getPath());
        assertEquals("/other", dest.getProperty("other").getNode().getPath());
        Value[] refs = dest.getProperty("multi").getValues();
        assertEquals(2, refs.length);
        Set<String> paths = new HashSet<String>();
        for (Value v : refs) {
            paths.add(session.getNodeByIdentifier(v.getString()).getPath());
        }
        assertTrue(paths.contains("/other"));
        assertTrue(paths.contains("/dest/test"));
    }

    @Test // OAK-1244
    public void importUUIDCreateNew() throws Exception {
        Session session = getAdminSession();
        Node node = session.getRootNode().addNode("node");
        node.addMixin("mix:referenceable");
        session.save();
        String uuid = node.getIdentifier();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        session.exportSystemView("/node", out, true, false);
        node.remove();
        session.save();
        session.importXML("/", new ByteArrayInputStream(out.toByteArray()),
                IMPORT_UUID_CREATE_NEW);
        session.save();
        node = session.getNode("/node");
        assertFalse(uuid.equals(node.getIdentifier()));
    }

    @Test
    public void testReferenceBinary() throws RepositoryException {
        ValueFactory valueFactory = getAdminSession().getValueFactory();
        Binary binary = valueFactory.createBinary(new RandomInputStream(1, 256 * 1024));

        String reference = binary instanceof ReferenceBinary
            ? ((ReferenceBinary) binary).getReference()
            : null;

        assumeTrue(reference != null);
        Session session = createAdminSession();
        try {
            valueFactory = session.getValueFactory();
            assertEquals(binary, valueFactory.createValue(
                    new SimpleReferenceBinary(reference)).getBinary());
        } finally {
            session.logout();
        }
    }

    @Test
    public void manyTransientChanges() throws RepositoryException {
        // test for OAK-1670, run with -Dupdate.limit=100
        Session session = getAdminSession();
        Node test = session.getRootNode().getNode(TEST_NODE);
        Node foo = test.addNode("foo");
        session.save();
        test.setProperty("p", "value");
        for (int i = 0; i < 76; i++) {
            test.addNode("n" + i);
        }
        Node t = foo.addNode("test");
        t.setProperty("prop", "value");
        session.getNode(TEST_PATH + "/foo/test").setProperty("prop", "value");
        session.save();

        session.logout();
    }

    @Test // OAK-2038
    public void importWithRegisteredType() throws Exception {
        Session session = getAdminSession();
        NodeTypeManager ntMgr = getAdminSession().getWorkspace().getNodeTypeManager();
        NodeTypeTemplate ntd = ntMgr.createNodeTypeTemplate();
        ntd.setName("fooType");
        PropertyDefinitionTemplate propDefTemplate = ntMgr.createPropertyDefinitionTemplate();
        propDefTemplate.setName("fooProp");
        propDefTemplate.setRequiredType(PropertyType.STRING);
        ntd.getPropertyDefinitionTemplates().add(propDefTemplate);
        ntMgr.registerNodeType(ntd, false);

        Node node = session.getRootNode().addNode("node", "fooType");
        node.setProperty("fooProp", "fooValue");
        session.save();
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        session.exportDocumentView("/node", out, true, false);
        node.remove();
        session.save();

        session.getWorkspace().importXML(
                "/", new ByteArrayInputStream(out.toByteArray()), IMPORT_UUID_CREATE_NEW);
        session.save();
        assertEquals("fooValue", session.getProperty("/node/fooProp").getString());
    }

    @Test
    public void largeMultiValueProperty() throws Exception{
        final List<String> logMessages = Lists.newArrayList();
        Appender<ILoggingEvent> a = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent e) {
                if (Level.WARN.isGreaterOrEqual(e.getLevel())){
                    logMessages.add(e.getFormattedMessage());
                }
            }
        };
        a.start();
        rootLogger().addAppender(a);
        Session session = getAdminSession();
        Node node = session.getRootNode().addNode("largeMultiValueProperty", "nt:unstructured");
        String[] largeArray = new String[1000+1]; //ItemImpl.MV_PROPERTY_WARN_THRESHOLD - 1000
        Arrays.fill(largeArray, "x");
        Property p = node.setProperty("fooProp", largeArray);
        Property p2 = node.setProperty("barProp", new String[] {"x"});
        p2.setValue(largeArray);
        session.save();

        rootLogger().detachAppender(a);
        a.stop();

        assertTrue(logMessages.size() >= 2);
        assertThat("Warn log message must contains a reference to the large array property path",
                logMessages.toString(), containsString(p.getPath()));
        assertThat("Warn log message must contains a reference to the large array property path",
                logMessages.toString(), containsString(p2.getPath()));
    }

    private static ch.qos.logback.classic.Logger rootLogger() {
        return ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    }

    //------------------------------------------------------------< private >---

    private Node getNode(String path) throws RepositoryException {
        return getAdminSession().getNode(path);
    }

    private Property getProperty(String path) throws RepositoryException {
        return getAdminSession().getProperty(path);
    }

    private void addProperty(Node parentNode, String name, Value value) throws RepositoryException, IOException {
        String propertyPath = parentNode.getPath() + '/' + name;
        assertFalse(getAdminSession().propertyExists(propertyPath));

        Property added = parentNode.setProperty(name, value);
        assertTrue(parentNode.isModified());
        assertFalse(added.isModified());
        assertTrue(added.isNew());
        getAdminSession().save();

        Session session2 = createAnonymousSession();
        try {
            assertTrue(session2.propertyExists(propertyPath));
            Value value2 = session2.getProperty(propertyPath).getValue();
            assertEquals(value.getType(), value2.getType());
            if (value.getType() == PropertyType.BINARY) {
                assertEqualStream(value.getStream(), value2.getStream());
            } else {
                assertEquals(value.getString(), value2.getString());
            }

            if (value2.getType() == PropertyType.REFERENCE || value2.getType() == PropertyType.WEAKREFERENCE) {
                String ref = value2.getString();
                assertNotNull(getAdminSession().getNodeByIdentifier(ref));
            }
        } finally {
            session2.logout();
        }
    }

    private static void assertEqualStream(InputStream is1, InputStream is2) throws IOException {
        byte[] buf1 = new byte[65536];
        byte[] buf2 = new byte[65536];

        int c = 0;
        while (c != -1) {
            assertEquals(c = is1.read(buf1), is2.read(buf2));
            for (int i = 0; i < c; i++) {
                assertEquals(buf1[i], buf2[i]);
            }
        }
    }

    //------------------------------------------------------------< NumberStream >---

    /**
     * Dummy stream class used by the binary property tests.
     */
    private static class NumberStream extends InputStream {

        private final int limit;

        private int counter;

        public NumberStream(int limit) {
            this.limit = limit;
        }

        @Override
        public int read() throws IOException {
            return counter < limit
                ? counter++ & 0xff
                : -1;
        }

    }

}
