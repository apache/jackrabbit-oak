/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

public class CRUDTest extends AbstractRepositoryTest {

    public CRUDTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2488">OAK-2488</a>
     */
    @Test
    public void testMixins() throws Exception {
        Session session = getAdminSession();
        String nodename = "mixintest";
        Node mixinTest = session.getRootNode().addNode(nodename, "nt:folder");
        NodeType[] types;
        types = mixinTest.getMixinNodeTypes();
        assertEquals(Arrays.toString(types), 0, types.length);
        mixinTest.addMixin("mix:versionable");
        types = mixinTest.getMixinNodeTypes();
        assertEquals(Arrays.toString(types), 1, types.length);
        session.save();
        mixinTest = session.getRootNode().getNode(nodename);
        mixinTest.remove();
        mixinTest = session.getRootNode().addNode(nodename, "nt:folder");
        types = mixinTest.getMixinNodeTypes();
        assertEquals(Arrays.toString(types), 0, types.length);
    }

    @Test
    public void testCRUD() throws RepositoryException {
        Session session = getAdminSession();
        // Create
        Node hello = session.getRootNode().addNode("hello");
        hello.setProperty("world",  "hello world");
        session.save();

        // Read
        assertEquals(
                "hello world",
                session.getProperty("/hello/world").getString());

        // Update
        session.getNode("/hello").setProperty("world", "Hello, World!");
        session.save();
        assertEquals(
                "Hello, World!",
                session.getProperty("/hello/world").getString());

        // Delete
        session.getNode("/hello").remove();
        session.save();
        assertTrue(!session.propertyExists("/hello/world"));
    }

    @Test
    public void testRemoveBySetProperty() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        try {
            root.setProperty("test", "abc");
            assertNotNull(root.setProperty("test", (String) null));
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testRemoveBySetMVProperty() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        try {
            root.setProperty("test", new String[] {"abc", "def"});
            assertNotNull(root.setProperty("test", (String[]) null));
        } catch (PathNotFoundException e) {
            // success
        }
    }

    @Test
    public void testRemoveMissingProperty() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        Property p = root.setProperty("missing", (String) null);
        assertNotNull(p);
        try {
            p.getValue();
            fail("must throw InvalidItemStateException");
        } catch (InvalidItemStateException e) {
            // expected
        }
    }

    @Test
    public void testRemoveMissingMVProperty() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        Property p = root.setProperty("missing", (String[]) null);
        assertNotNull(p);
        try {
            p.getValues();
            fail("must throw InvalidItemStateException");
        } catch (InvalidItemStateException e) {
            // expected
        }
    }

    @Test
    public void testRootPropertyPath() throws RepositoryException {
        Property property = getAdminSession().getRootNode().getProperty("jcr:primaryType");
        assertEquals("/jcr:primaryType", property.getPath());
    }
    
    @Test(expected = ConstraintViolationException.class)
    public void nodeType() throws RepositoryException {
            Session s = getAdminSession();
            s.getRootNode().addNode("a", "nt:folder").addNode("b");
            s.save();        
    }

    @Test
    public void getNodeWithRelativePath() throws RepositoryException {
        Session s = getAdminSession();
        try {
            s.getNode("some-relative-path");
            fail("Session.getNode() with relative path must throw a RepositoryException");
        } catch (RepositoryException e) {
            // expected
        }
    }

    @Test
    public void getPropertyWithRelativePath() throws RepositoryException {
        Session s = getAdminSession();
        try {
            s.getProperty("some-relative-path");
            fail("Session.getProperty() with relative path must throw a RepositoryException");
        } catch (RepositoryException e) {
            // expected
        }
    }

    @Test
    public void getItemWithRelativePath() throws RepositoryException {
        Session s = getAdminSession();
        try {
            s.getItem("some-relative-path");
            fail("Session.getItem() with relative path must throw a RepositoryException");
        } catch (RepositoryException e) {
            // expected
        }
    }

    @Test
    public void testSetPropertyDateWithTimeZone() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();

        final Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("America/Chicago"));
        cal.setTimeInMillis(1239902100000L);

        root.setProperty("start", cal);
        session.save();

        assertEquals(12, root.getProperty("start").getDate().get(Calendar.HOUR_OF_DAY));

        root.getProperty("start").remove();
        session.save();
    }

    @Test
    public void checkPathInInvalidItemStateException() throws Exception {
        Session s1 = getAdminSession();
        Node root = s1.getRootNode();
        Node a = root.addNode("a");
        String path = a.getPath();
        s1.save();

        Session s2 = getAdminSession();
        s2.getRootNode().getNode("a").remove();
        s2.save();

        s1.refresh(false);

        try {
            a.getPath();
        } catch (InvalidItemStateException e) {
            assertThat(e.getMessage(), containsString(path));
        }
    }

}
