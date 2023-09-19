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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.jcr.AccessDeniedException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE;

/**
 * @since OAK 1.0 : jcr:read is an aggregation of read property and node privileges.
 */
public class ReadPropertyTest extends AbstractEvaluationTest {

    @Test
    public void testReadProperty() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        assertFalse(testSession.nodeExists(path));
        assertFalse(testSession.itemExists(path));
        assertFalse(testSession.nodeExists(childNPath));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            assertTrue(testSession.itemExists(pPath));
            assertTrue(testSession.propertyExists(pPath));
            Property p = testSession.getProperty(pPath);
            assertEquals(pPath, p.getPath());
        }
    }

    @Test
    public void testReadProperty2() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_NODES));

        assertFalse(testSession.nodeExists(path));
        assertFalse(testSession.itemExists(path));
        assertFalse(testSession.nodeExists(childNPath));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            assertTrue(testSession.itemExists(pPath));
            assertTrue(testSession.propertyExists(pPath));
            Property p = testSession.getProperty(pPath);
        }
    }

    @Test
    public void testDenyReadProperties() throws Exception {
        allow(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        assertTrue(testSession.nodeExists(path));
        assertTrue(testSession.itemExists(path));
        assertTrue(testSession.nodeExists(childNPath));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            assertFalse(testSession.itemExists(pPath));
            assertFalse(testSession.propertyExists(pPath));
        }

        Node target = testSession.getNode(path);
        assertFalse(target.getProperties().hasNext());
    }

    @Test
    public void testDenySingleProperty() throws Exception {
        allow(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES), createGlobRestriction("*/" +propertyName1));

        assertTrue(testSession.nodeExists(path));
        assertTrue(testSession.itemExists(path));
        assertTrue(testSession.nodeExists(childNPath));

        List<String> allowed = new ArrayList<String>();
        allowed.add(path + "/jcr:primaryType");
        allowed.add(childNPath + "/jcr:primaryType");
        for (String pPath : allowed) {
            assertTrue(testSession.itemExists(pPath));
            assertTrue(testSession.propertyExists(pPath));
        }

        List<String> denied = new ArrayList<String>();
        denied.add(childPPath);
        denied.add(childchildPPath);
        for (String pPath : denied) {
            assertFalse(testSession.itemExists(pPath));
            assertFalse(testSession.propertyExists(pPath));
        }

        Node target = testSession.getNode(path);
        PropertyIterator pit = target.getProperties();
        while (pit.hasNext()) {
            assertFalse(propertyName1.equals(pit.nextProperty().getName()));
        }
    }

    @Test
    public void testGetParent() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_NODES));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            Property p = testSession.getProperty(pPath);
            try {
                Node n = p.getParent();
                fail();
            } catch (AccessDeniedException e) {
                // success
            }
        }
    }

    @Test
    public void testGetPath() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            Property p = testSession.getProperty(pPath);
            assertEquals(pPath, p.getPath());
        }
    }

    @Test
    public void testGetStatus() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        List<String> propertyPaths = new ArrayList<String>();
        propertyPaths.add(childPPath);
        propertyPaths.add(childchildPPath);
        propertyPaths.add(path + "/jcr:primaryType");

        for (String pPath : propertyPaths) {
            Property p = testSession.getProperty(pPath);
            assertFalse(p.isModified());
            assertFalse(p.isNew());
        }
    }

    @Test
    public void testAddMixinType() throws Exception {
        superuser.getNode(path).addMixin(MIX_REFERENCEABLE);
        superuser.save();

        deny(path, privilegesFromName(PrivilegeConstants.JCR_READ));
        allow(path, privilegesFromName(PrivilegeConstants.REP_READ_NODES));
        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));

        assertMixinTypes(superuser.getNode(path), MIX_REFERENCEABLE, MIX_REP_ACCESS_CONTROLLABLE);

        Node node = testSession.getNode(path);
        assertFalse(node.hasProperty(JcrConstants.JCR_MIXINTYPES));
        node.addMixin("mix:title");
        testSession.save();

        superuser.refresh(false);
        // OAK-10334 - FIXME: fails, because it only returns mix:title
        // assertMixinTypes(superuser.getNode(path), MIX_REFERENCEABLE, MIX_REP_ACCESS_CONTROLLABLE, "mix:title");
    }

    private void assertMixinTypes(Node node, String... mixins)
            throws RepositoryException {
        Set<String> expected = Arrays.stream(mixins).collect(Collectors.toSet());
        Set<String> actual = new HashSet<>();
        if (node.hasProperty(JcrConstants.JCR_MIXINTYPES)) {
            for (Value v : node.getProperty(JcrConstants.JCR_MIXINTYPES).getValues()) {
                actual.add(v.getString());
            }
        }
        assertEquals(expected, actual);
    }
}