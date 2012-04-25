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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import util.NumberStream;

import javax.jcr.Binary;
import javax.jcr.Item;
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
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.ObservationManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.jcr.util.Arrays.contains;
import static org.apache.jackrabbit.oak.jcr.util.Arrays.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RepositoryTest extends AbstractRepositoryTest {
    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    @Before
    public void setup() throws RepositoryException {
        Session session = getRepository().login();
        ValueFactory valueFactory = session.getValueFactory();
        try {
            Node root = session.getRootNode();
            Node foo = root.addNode("foo");
            foo.setProperty("stringProp", "stringVal");
            foo.setProperty("intProp", 42);
            foo.setProperty("mvProp", new Value[] {
                valueFactory.createValue(1),
                valueFactory.createValue(2),
                valueFactory.createValue(3),
            });
            root.addNode("bar");
            root.addNode(TEST_NODE);
            session.save();
        }
        finally {
            session.logout();
        }
    }

    @After
    public void tearDown() throws RepositoryException {
        logout();
    }

    @Test
    public void createRepository() throws RepositoryException {
        Repository repository = getRepository();
        assertNotNull(repository);
    }

    @Test
    public void login() throws RepositoryException {
        assertNotNull(getSession());
    }

    @Test(expected = NoSuchWorkspaceException.class)
    public void loginInvalidWorkspace() throws RepositoryException {
        Repository repository = getRepository();
        repository.login("invalid");
    }

    @Ignore("WIP") // TODO implement workspace management
    @Test
    public void getWorkspaceNames() throws RepositoryException {
        String[] workspaces = getSession().getWorkspace().getAccessibleWorkspaceNames();

        Set<String> names = new HashSet<String>() {{
            add("default");
        }};

        assertTrue(asList(workspaces).containsAll(names));
        assertTrue(names.containsAll(asList(workspaces)));
    }

    @Ignore("WIP") // TODO implement workspace management
    @Test
    public void createDeleteWorkspace() throws RepositoryException {
        getSession().getWorkspace().createWorkspace("new");

        Session session2 = getRepository().login();
        try {
            String[] workspaces = session2.getWorkspace().getAccessibleWorkspaceNames();
            assertTrue(asList(workspaces).contains("new"));
            Session session3 = getRepository().login("new");
            assertEquals("new", session3.getWorkspace().getName());
            session3.logout();
            session2.getWorkspace().deleteWorkspace("new");
        }
        finally {
            session2.logout();
        }

        Session session4 = getRepository().login();
        try {
            String[] workspaces = session4.getWorkspace().getAccessibleWorkspaceNames();
            assertFalse(asList(workspaces).contains("new"));
        }
        finally {
            session4.logout();
        }
    }

    @Test
    public void getRoot() throws RepositoryException {
        Node root = getSession().getRootNode();
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

    @Test
    public void getNode() throws RepositoryException {
        Node node = getNode("/foo");
        assertNotNull(node);
        assertEquals("foo", node.getName());
        assertEquals("/foo", node.getPath());
    }

    @Test
    public void getNodeByIdentifier() throws RepositoryException {
        Node node = getNode("/foo");
        String id = node.getIdentifier();
        Node node2 = getSession().getNodeByIdentifier(id);
        assertTrue(node.isSame(node2));
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
        Set<String> nodeNames = new HashSet<String>() {{
            add("bar");
            add("added");
            add(TEST_NODE);
        }};

        Node root = getNode("/");
        root.addNode("added");         // transiently added
        root.getNode("foo").remove();  // transiently removed
        root.getNode("bar").remove();  // transiently removed and...
        root.addNode("bar");           // ... added again
        NodeIterator nodes = root.getNodes();
        while (nodes.hasNext()) {
            assertTrue(nodeNames.remove(nodes.nextNode().getName()));
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
        while (properties.hasNext()) {
            Property p = properties.nextProperty();
            assertTrue(propertyNames.remove(p.getName()));
            if (p.isMultiple()) {
                for (Value v : p.getValues()) {
                    assertTrue(values.remove(v.getString()));
                }
            }
            else {
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
        Session session = getSession();
        String newNode = TEST_PATH + "/new";
        assertFalse(session.nodeExists(newNode));

        Node node = getNode(TEST_PATH);
        Node added = node.addNode("new");
        assertFalse(node.isNew());
        assertTrue(node.isModified());
        assertTrue(added.isNew());
        assertFalse(added.isModified());
        session.save();

        Session session2 = getRepository().login();
        try {
            assertTrue(session2.nodeExists(newNode));
            added = session2.getNode(newNode);
            assertFalse(added.isNew());
            assertFalse(added.isModified());
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addNodeWithSpecialChars() throws RepositoryException {
        Session session = getSession();
        String nodeName = "foo{";

        String newNode = TEST_PATH + '/' + nodeName;
        assertFalse(session.nodeExists(newNode));

        Node node = getNode(TEST_PATH);
        node.addNode(nodeName);
        session.save();

        Session session2 = getRepository().login();
        try {
            assertTrue(session2.nodeExists(newNode));
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addNodeWithNodeType() throws RepositoryException {
        Session session = getSession();

        Node node = getNode(TEST_PATH);
        node.addNode("new", "nt:folder");
        session.save();
    }

    @Test
    public void addNodeToRootNode() throws RepositoryException {
        Session session = getSession();
        Node root = session.getRootNode();
        String newNode = "newNodeBelowRoot";
        assertFalse(root.hasNode(newNode));
        root.addNode(newNode);
        session.save();
    }

    @Test
    public void addStringProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "string", getSession().getValueFactory().createValue("string value"));
    }

    @Test
    public void addMultiValuedString() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue("one");
        values[1] = getSession().getValueFactory().createValue("two");

        parentNode.setProperty("multi string", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi string");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.STRING, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addLongProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "long", getSession().getValueFactory().createValue(42L));
    }

    @Test
    public void addMultiValuedLong() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue(42L);
        values[1] = getSession().getValueFactory().createValue(84L);

        parentNode.setProperty("multi long", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi long");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.LONG, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addDoubleProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "double", getSession().getValueFactory().createValue(42.2D));
    }

    @Test
    public void addMultiValuedDouble() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue(42.1d);
        values[1] = getSession().getValueFactory().createValue(99.0d);

        parentNode.setProperty("multi double", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi double");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.DOUBLE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addBooleanProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "boolean", getSession().getValueFactory().createValue(true));
    }

    @Test
    public void addMultiValuedBoolean() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue(true);
        values[1] = getSession().getValueFactory().createValue(false);

        parentNode.setProperty("multi boolean", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi boolean");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BOOLEAN, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addDecimalProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "decimal", getSession().getValueFactory().createValue(BigDecimal.valueOf(21)));
    }

    @Test
    public void addMultiValuedDecimal() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue(BigDecimal.valueOf(42));
        values[1] = getSession().getValueFactory().createValue(BigDecimal.valueOf(99));

        parentNode.setProperty("multi decimal", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi decimal");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.DECIMAL, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addDateProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "date", getSession().getValueFactory().createValue(Calendar.getInstance()));
    }

    @Test
    public void addMultiValuedDate() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        Calendar calendar = Calendar.getInstance();
        values[0] = getSession().getValueFactory().createValue(calendar);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        values[1] = getSession().getValueFactory().createValue(calendar);

        parentNode.setProperty("multi date", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi date");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.DATE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addURIProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "uri", getSession().getValueFactory().createValue("http://www.day.com/", PropertyType.URI));
    }

    @Test
    public void addMultiValuedURI() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue("http://www.day.com", PropertyType.URI);
        values[1] = getSession().getValueFactory().createValue("file://var/dam", PropertyType.URI);

        parentNode.setProperty("multi uri", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi uri");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.URI, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addNameProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "name", getSession().getValueFactory().createValue("jcr:something\"", PropertyType.NAME));
    }

    @Test
    public void addMultiValuedName() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue("jcr:foo", PropertyType.NAME);
        values[1] = getSession().getValueFactory().createValue("bar", PropertyType.NAME);

        parentNode.setProperty("multi name", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi name");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.NAME, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addPathProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "path", getSession().getValueFactory().createValue("/jcr:foo/bar\"", PropertyType.PATH));
    }

    @Test
    public void addMultiValuedPath() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue("/nt:foo/jcr:bar", PropertyType.PATH);
        values[1] = getSession().getValueFactory().createValue("/", PropertyType.PATH);

        parentNode.setProperty("multi path", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi path");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.PATH, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addBinaryProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        InputStream is = new ByteArrayInputStream("foo\"".getBytes());
        Binary bin = getSession().getValueFactory().createBinary(is);
        addProperty(parentNode, "binary", getSession().getValueFactory().createValue(bin));
    }

    @Test
    public void addSmallBinaryProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        InputStream is = new NumberStream(1234);
        Binary bin = getSession().getValueFactory().createBinary(is);
        addProperty(parentNode, "bigBinary", getSession().getValueFactory().createValue(bin));
    }

    @Test
    public void addBigBinaryProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        InputStream is = new NumberStream(123456);
        Binary bin = getSession().getValueFactory().createBinary(is);
        addProperty(parentNode, "bigBinary", getSession().getValueFactory().createValue(bin));
    }

    @Test
    public void addMultiValuedBinary() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];

        InputStream is = new ByteArrayInputStream("foo".getBytes());
        Binary bin = getSession().getValueFactory().createBinary(is);
        values[0] = getSession().getValueFactory().createValue(bin);

        is = new ByteArrayInputStream("bar".getBytes());
        bin = getSession().getValueFactory().createBinary(is);
        values[1] = getSession().getValueFactory().createValue(bin);

        parentNode.setProperty("multi binary", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi binary");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BINARY, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Ignore // TODO implement node type support
    @Test
    public void addReferenceProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getSession().save();

        addProperty(parentNode, "reference", getSession().getValueFactory().createValue(referee));
    }

    @Ignore // TODO implement node type support
    @Test
    public void addMultiValuedReference() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getSession().save();

        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue(referee);
        values[1] = getSession().getValueFactory().createValue(referee);

        parentNode.setProperty("multi reference", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi reference");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.REFERENCE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Ignore // TODO implement node type support
    @Test
    public void addWeakReferenceProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getSession().save();

        addProperty(parentNode, "weak reference", getSession().getValueFactory().createValue(referee, true));
    }

    @Ignore // TODO implement node type support
    @Test
    public void addMultiValuedWeakReference() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Node referee = getSession().getNode("/foo");
        referee.addMixin("mix:referenceable");
        getSession().save();

        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue(referee, true);
        values[1] = getSession().getValueFactory().createValue(referee, true);

        parentNode.setProperty("multi weak reference", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi weak reference");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.WEAKREFERENCE, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length, values2.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Ignore // TODO implement value coding in ValueConverter
    @Test
    public void addEmptyMultiValuedProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[0];

        parentNode.setProperty("multi empty", values, PropertyType.BOOLEAN);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi empty");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BOOLEAN, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(0, values2.length);
        }
        finally {
            session2.logout();
        }
    }

    @Ignore // TODO implement value coding in ValueConverter
    @Test
    public void addEmptyMultiValuedProperty_JCR_2992_WorkaroundTest() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[0];

        parentNode.setProperty("multi empty", values, PropertyType.BOOLEAN);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi empty");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.STRING, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(0, values2.length);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void addMultiValuedStringWithNull() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[3];
        values[0] = getSession().getValueFactory().createValue(true);
        values[2] = getSession().getValueFactory().createValue(false);

        parentNode.setProperty("multi with null", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi with null");
            assertTrue(property.isMultiple());
            assertEquals(PropertyType.BOOLEAN, property.getType());
            Value[] values2 = property.getValues();
            assertEquals(values.length - 1, values2.length);
        }
        finally {
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
        addProperty(parentNode, "string", getSession().getValueFactory().createValue("string \" value"));

        Property property = parentNode.getProperty("string");
        property.setValue("new value");
        assertTrue(parentNode.isModified());
        assertTrue(property.isModified());
        assertFalse(property.isNew());
        property.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property2 = session2.getProperty(TEST_PATH + "/string");
            assertEquals("new value", property2.getString());
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void setDoubleNaNProperty() throws RepositoryException, IOException {
        Node parentNode = getNode(TEST_PATH);
        addProperty(parentNode, "NaN", getSession().getValueFactory().createValue(Double.NaN));

        Session session2 = getRepository().login();
        try {
            Property property2 = session2.getProperty(TEST_PATH + "/NaN");
            assertTrue(Double.isNaN(property2.getDouble()));
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void setMultiValuedProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        Value[] values = new Value[2];
        values[0] = getSession().getValueFactory().createValue("one");
        values[1] = getSession().getValueFactory().createValue("two");

        parentNode.setProperty("multi string2", values);
        parentNode.getSession().save();

        values[0] = getSession().getValueFactory().createValue("eins");
        values[1] = getSession().getValueFactory().createValue("zwei");
        parentNode.setProperty("multi string2", values);
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            Property property = session2.getProperty(TEST_PATH + "/multi string2");
            assertTrue(property.isMultiple());
            Value[] values2 = property.getValues();
            assertEquals(2, values.length);
            assertEquals(values[0], values2[0]);
            assertEquals(values[1], values2[1]);
        }
        finally {
            session2.logout();
        }
    }

    @Test
    public void nullProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        parentNode.setProperty("newProperty", "some value");
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            session2.getProperty(TEST_PATH + "/newProperty").setValue((String) null);
            session2.save();
        }
        finally {
            session2.logout();
        }

        Session session3 = getRepository().login();
        try {
            assertFalse(session3.propertyExists(TEST_PATH + "/newProperty"));
        }
        finally {
            session3.logout();
        }
    }

    @Test
    public void removeProperty() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        parentNode.setProperty("newProperty", "some value");
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            session2.getProperty(TEST_PATH + "/newProperty").remove();
            session2.save();
        }
        finally {
            session2.logout();
        }

        Session session3 = getRepository().login();
        try {
            assertFalse(session3.propertyExists(TEST_PATH + "/newProperty"));
        }
        finally {
            session3.logout();
        }
    }

    @Test
    public void removeNode() throws RepositoryException {
        Node parentNode = getNode(TEST_PATH);
        parentNode.addNode("newNode");
        parentNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            session2.getNode(TEST_PATH + "/newNode").remove();
            assertTrue(session2.getNode(TEST_PATH).isModified());
            session2.save();
        }
        finally {
            session2.logout();
        }

        Session session3 = getRepository().login();
        try {
            assertFalse(session3.nodeExists(TEST_PATH + "/newNode"));
            assertFalse(session3.getNode(TEST_PATH).isModified());
        }
        finally {
            session3.logout();
        }
    }

    @Test
    public void sessionSave() throws RepositoryException {
        Session session1 = getRepository().login();
        Session session2 = getRepository().login();
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

            // Make sure these items are still not accessible through another session until refresh
            assertFalse(session2.itemExists("/node1"));
            assertFalse(session2.itemExists("/node1/node2"));
            assertFalse(session2.itemExists("/node1/node3"));
            assertFalse(session2.itemExists("/node1/node3/property1"));

            session2.refresh(false);
            assertTrue(session2.itemExists("/node1"));
            assertTrue(session2.itemExists("/node1/node2"));
            assertTrue(session2.itemExists("/node1/node3"));
            assertTrue(session2.itemExists("/node1/node3/property1"));
        }
        finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void sessionRefresh() throws RepositoryException {
        Session session = getRepository().login();
        try {
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
        finally {
            session.logout();
        }
    }

    @Test
    public void sessionRefreshFalse() throws RepositoryException {
        Session session = getRepository().login();
        try {
            Node foo = session.getNode("/foo");
            foo.addNode("added");
            NodeIterator it = foo.getNodes();
            assertTrue(it.hasNext());

            session.refresh(false);
            it = foo.getNodes();
            assertFalse(it.hasNext());
        }
        finally {
            session.logout();
        }
    }

    @Test(expected = RepositoryException.class)
    public void refreshConflict() throws RepositoryException {
        Session session1 = getRepository().login();
        Session session2 = getRepository().login();
        try {
            session1.getRootNode().addNode("node");
            session2.getRootNode().addNode("node");

            session1.save();
            session2.refresh(true);
            session2.save();
        }
        finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test(expected = RepositoryException.class)
    public void refreshConflict2() throws RepositoryException {
        getSession().getRootNode().addNode("node");
        getSession().save();

        Session session1 = getRepository().login();
        Session session2 = getRepository().login();
        try {
            session1.getNode("/node").remove();
            session2.getNode("/node").addNode("2");

            session1.save();
            session2.save();
        }
        finally {
            session1.logout();
            session2.logout();
        }
    }

    @Test
    public void liveNodes() throws RepositoryException {
        Session session = getRepository().login();
        try {
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
        finally {
            session.logout();
        }
    }

    @Test
    public void move() throws RepositoryException {
        Session session = getSession();

        Node node = getNode(TEST_PATH);
        node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

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
        Session session = getSession();

        Node node = getNode(TEST_PATH);
        node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

        session.getWorkspace().move(TEST_PATH + "/source/node", TEST_PATH + "/target/moved");

        // Move must not be visible in session
        assertTrue(node.hasNode("source/node"));
        assertFalse(node.hasNode("target/moved"));

        session.refresh(false);

        // Move must be visible in session after refresh
        assertFalse(node.hasNode("source/node"));
        assertTrue(node.hasNode("source"));
        assertTrue(node.hasNode("target/moved"));
    }

    @Test
    public void workspaceCopy() throws RepositoryException {
        Session session = getSession();

        Node node = getNode(TEST_PATH);
        node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        // Copy must not be visible in session
        assertTrue(node.hasNode("source/node"));
        assertFalse(node.hasNode("target/copied"));

        session.refresh(false);

        // Copy must be visible in session after refresh
        assertTrue(node.hasNode("source/node"));
        assertTrue(node.hasNode("target/copied"));
    }

    @Ignore // TODO implement node type support
    @Test
    public void setPrimaryType() throws RepositoryException {
        Node testNode = getNode(TEST_PATH);
        assertEquals("nt:unstructured", testNode.getPrimaryNodeType().getName());
        assertEquals("nt:unstructured", testNode.getProperty("jcr:primaryType").getString());

        testNode.setPrimaryType("nt:folder");
        getSession().save();

        Session session2 = getRepository().login();
        try {
            testNode = session2.getNode(TEST_PATH);
            assertEquals("nt:folder", testNode.getPrimaryNodeType().getName());
            assertEquals("nt:folder", testNode.getProperty("jcr:primaryType").getString());
        }
        finally {
            session2.logout();
        }
    }

    @Ignore // TODO implement orderBefore, orderable child nodes
    @Test
    public void reorderTest() throws RepositoryException {
        Node testNode = getNode(TEST_PATH);
        testNode.addNode("a");
        testNode.addNode("b");
        testNode.addNode("c");
        getSession().save();

        NodeIterator it = testNode.getNodes();
        // todo: check order

        testNode.orderBefore("a", "c");
        getSession().save();

        it = testNode.getNodes();
        // todo: check order
    }

    @Ignore // TODO implement node type support
    @Test
    public void nodeTypeRegistry() throws RepositoryException {
        NodeTypeManager ntMgr = getSession().getWorkspace().getNodeTypeManager();
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
                getSession().getWorkspace().getNamespaceRegistry();
        assertFalse(contains(nsReg.getPrefixes(), "foo"));
        assertFalse(contains(nsReg.getURIs(), "file:///foo"));

        nsReg.registerNamespace("foo", "file:///foo");
        assertTrue(contains(nsReg.getPrefixes(), "foo"));
        assertTrue(contains(nsReg.getURIs(), "file:///foo"));

        nsReg.unregisterNamespace("foo");
        assertFalse(contains(nsReg.getPrefixes(), "foo"));
        assertFalse(contains(nsReg.getURIs(), "file:///foo"));
    }

    @Ignore // TODO implement node type support
    @Test
    public void mixin() throws RepositoryException {
        NodeTypeManager ntMgr = getSession().getWorkspace().getNodeTypeManager();
        NodeTypeTemplate mixTest = ntMgr.createNodeTypeTemplate();
        mixTest.setName("mix:test");
        mixTest.setMixin(true);
        ntMgr.registerNodeType(mixTest, false);

        Node testNode = getNode(TEST_PATH);
        NodeType[] mix = testNode.getMixinNodeTypes();
        assertEquals(0, mix.length);

        testNode.addMixin("mix:test");
        testNode.getSession().save();

        Session session2 = getRepository().login();
        try {
            mix = session2.getNode(TEST_PATH).getMixinNodeTypes();
            assertEquals(1, mix.length);
            assertEquals("mix:test", mix[0].getName());
        }
        finally {
            session2.logout();
        }

        testNode.removeMixin("mix:test");
        testNode.getSession().save();

        session2 = getRepository().login();
        try {
            mix = session2.getNode(TEST_PATH).getMixinNodeTypes();
            assertEquals(0, mix.length);
        }
        finally {
            session2.logout();
        }
    }

    @Ignore // TODO implement observation
    @Test
    public void observation() throws RepositoryException {
        final Set<String> addNodes = toSet(
                TEST_PATH + "/1",
                TEST_PATH + "/2",
                TEST_PATH + "/3");

        final Set<String> removeNodes = toSet(
                TEST_PATH + "/2");

        final Set<String> addProperties = toSet(
                TEST_PATH + "/property",
                TEST_PATH + "/prop0",
                TEST_PATH + "/1/prop1",
                TEST_PATH + "/1/prop2",
                TEST_PATH + "/1/jcr:primaryType",
                TEST_PATH + "/2/jcr:primaryType",
                TEST_PATH + "/3/jcr:primaryType",
                TEST_PATH + "/3/prop3");

        final Set<String> setProperties = toSet(
                TEST_PATH + "/1/prop1");

        final Set<String> removeProperties = toSet(
                TEST_PATH + "/1/prop2");

        final List<Event> failedEvents = new ArrayList<Event>();

        ObservationManager obsMgr = getSession().getWorkspace().getObservationManager();
        obsMgr.setUserData("my user data");
        obsMgr.addEventListener(new EventListener() {
                @Override
                public void onEvent(EventIterator events) {
                    while (events.hasNext()) {
                        Event event = events.nextEvent();
                        try {
                            switch (event.getType()) {
                                case Event.NODE_ADDED:
                                    if (!addNodes.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                case Event.NODE_REMOVED:
                                    if (!removeNodes.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                case Event.PROPERTY_ADDED:
                                    if (!addProperties.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                case Event.PROPERTY_CHANGED:
                                    if (!setProperties.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                case Event.PROPERTY_REMOVED:
                                    if (!removeProperties.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                default:
                                    failedEvents.add(event);
                            }
                        }
                        catch (RepositoryException e) {
                            failedEvents.add(event);
                        }
                    }
                }
            },
            Event.NODE_ADDED | Event.NODE_REMOVED | Event.NODE_MOVED | Event.PROPERTY_ADDED |
            Event.PROPERTY_REMOVED | Event.PROPERTY_CHANGED | Event.PERSIST, "/", true, null, null, false);

        Node n = getNode(TEST_PATH);
        n.setProperty("prop0", "val0");
        Node n1 = n.addNode("1");
        n1.setProperty("prop1", "val1");
        n1.setProperty("prop2", "val2");
        n.addNode("2");
        getSession().save();

        n.setProperty("property", 42);
        n.addNode("3").setProperty("prop3", "val3");
        n1.setProperty("prop1", "val1 new");
        n1.getProperty("prop2").remove();
        n.getNode("2").remove();
        getSession().save();

        assertTrue(failedEvents.isEmpty());
        assertTrue(addNodes.isEmpty());
        assertTrue(removeNodes.isEmpty());
        assertTrue(addProperties.isEmpty());
        assertTrue(removeProperties.isEmpty());
        assertTrue(setProperties.isEmpty());
    }

    @Ignore // TODO implement observation
    @Test
    public void observation2() throws RepositoryException {
        final Set<String> addNodes = toSet(
                TEST_PATH + "/1",
                TEST_PATH + "/2");

        final Set<String> removeNodes = toSet(
                TEST_PATH + "/1");

        final Set<String> addProperties = toSet(
                TEST_PATH + "/1/jcr:primaryType",
                TEST_PATH + "/2/jcr:primaryType");


        final List<Event> failedEvents = new ArrayList<Event>();

        ObservationManager obsMgr = getSession().getWorkspace().getObservationManager();
        obsMgr.setUserData("my user data");
        obsMgr.addEventListener(new EventListener() {
                @Override
                public void onEvent(EventIterator events) {
                    while (events.hasNext()) {
                        Event event = events.nextEvent();
                        try {
                            switch (event.getType()) {
                                case Event.NODE_ADDED:
                                    if (!addNodes.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                case Event.NODE_REMOVED:
                                    if (!removeNodes.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                case Event.PROPERTY_ADDED:
                                    if (!addProperties.remove(event.getPath())) {
                                        failedEvents.add(event);
                                    }
                                    break;
                                default:
                                    failedEvents.add(event);
                            }
                        }
                        catch (RepositoryException e) {
                            failedEvents.add(event);
                        }
                    }
                }
            },
            Event.NODE_ADDED | Event.NODE_REMOVED | Event.NODE_MOVED | Event.PROPERTY_ADDED |
            Event.PROPERTY_REMOVED | Event.PROPERTY_CHANGED | Event.PERSIST, "/", true, null, null, false);

        Node n = getNode(TEST_PATH);
        n.addNode("1");
        getSession().save();

        n.addNode("2");
        n.getNode("1").remove();
        getSession().save();

        assertTrue(failedEvents.isEmpty());
        assertTrue(addNodes.isEmpty());
        assertTrue(removeNodes.isEmpty());
        assertTrue(addProperties.isEmpty());
    }

    @Ignore // TODO implement observation
    @Test
    public void observationNoEvents() throws RepositoryException, InterruptedException {
        final List<Event> failedEvents = new ArrayList<Event>();

        ObservationManager obsMgr = getSession().getWorkspace().getObservationManager();
        obsMgr.setUserData("my user data");
        obsMgr.addEventListener(new EventListener() {
                @Override
                public void onEvent(EventIterator events) {
                    while (events.hasNext()) {
                        failedEvents.add(events.nextEvent());
                    }
                }
            },
            Event.NODE_ADDED | Event.NODE_REMOVED | Event.NODE_MOVED | Event.PROPERTY_ADDED |
            Event.PROPERTY_REMOVED | Event.PROPERTY_CHANGED | Event.PERSIST, "/", true, null, null, false);

        Thread.sleep(5000);
        assertTrue(failedEvents.isEmpty());
    }

    @Ignore // TODO implement observation
    @Test
    public void observationDispose() throws RepositoryException, ExecutionException, TimeoutException,
                InterruptedException {

        final List<Event> failedEvents = new ArrayList<Event>();

        final ObservationManager obsMgr = getSession().getWorkspace().getObservationManager();
        obsMgr.setUserData("my user data");
        final EventListener listener = new EventListener() {
            @Override
            public void onEvent(EventIterator events) {
                while (events.hasNext()) {
                    failedEvents.add(events.nextEvent());
                }
            }
        };
        obsMgr.addEventListener(listener, Event.NODE_ADDED | Event.NODE_REMOVED | Event.NODE_MOVED |
            Event.PROPERTY_ADDED | Event.PROPERTY_REMOVED | Event.PROPERTY_CHANGED | Event.PERSIST,
            "/", true, null, null, false);

        FutureTask<Object> disposer = new FutureTask<Object>(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                obsMgr.removeEventListener(listener);
                return null;
            }
        });

        Thread.sleep(250);
        Executors.newSingleThreadExecutor().execute(disposer);
        disposer.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(failedEvents.isEmpty());
    }

    @Test
    public void liveNode() throws RepositoryException {
        Session session = getSession();

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

    //------------------------------------------< private >---

    private void addProperty(Node parentNode, String name, Value value) throws RepositoryException, IOException {
        String propertyPath = parentNode.getPath() + '/' + name;
        assertFalse(getSession().propertyExists(propertyPath));

        Property added = parentNode.setProperty(name, value);
        assertTrue(parentNode.isModified());
        assertFalse(added.isModified());
        assertTrue(added.isNew());
        getSession().save();

        Session session2 = getRepository().login();
        try {
            assertTrue(session2.propertyExists(propertyPath));
            Value value2 = session2.getProperty(propertyPath).getValue();
            assertEquals(value.getType(), value2.getType());
            assertEqualStream(value.getStream(), value2.getStream());
            assertEquals(value.getString(), value2.getString());

            if (value2.getType() == PropertyType.REFERENCE || value2.getType() == PropertyType.WEAKREFERENCE) {
                String ref = value2.getString();
                assertNotNull(getSession().getNodeByIdentifier(ref));
            }
        }
        finally {
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

}
