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
package org.apache.jackrabbit.oak.jcr.session;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.security.Principal;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import javax.jcr.GuestCredentials;
import javax.jcr.Item;
import javax.jcr.NamespaceException;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class JackrabbitSessionTest extends AbstractJCRTest {
    
    private JackrabbitSession s;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (superuser instanceof JackrabbitSession) {
            s = (JackrabbitSession) superuser;
        } else {
            throw new NotExecutableException("JackrabbitSession expected");
        }
    }

    public void testGetParentOrNullRootNode() throws Exception {
        assertNull(s.getParentOrNull(s.getRootNode()));
    }

    public void testGetParentOrNull() throws Exception {
        Node n = s.getNode(testRoot);
        assertEquivalentNode(n, s.getParentOrNull(n.getProperty(Property.JCR_PRIMARY_TYPE)));
        assertEquivalentNode(n.getParent(), s.getParentOrNull(n));
    }
    
    private static void assertEquivalentNode(@NotNull Node expected, @Nullable Node result) throws Exception {
        assertNotNull(result);
        assertEquals(expected.getPath(), result.getPath());
    }
    
    public void testGetParentOrNullSessionMismatch() throws Exception {
        JackrabbitSession guest = (JackrabbitSession) getHelper().getRepository().login(new GuestCredentials());
        try {
            guest.getParentOrNull(s.getNode(testRoot));
            fail("RepositoryException expected");
        } catch (RepositoryException e) {
            // success
        } finally {
            guest.logout();
        }
    }

    public void testGetParentOrNullImplMismatch() {
        try {
            Item item = mock(Item.class);
            s.getParentOrNull(item);
            fail("RepositoryException expected");
        } catch (RepositoryException e) {
            // success
        }
    }

    public void testGetExpandedName() throws RepositoryException {
        // empty namespace uri
        assertEquals("{}testroot", s.getExpandedName(testRootNode));
        Node n = testRootNode.addNode("test:bar");
        assertEquals("{http://www.apache.org/jackrabbit/test}bar", s.getExpandedName(n));
        // now remap namespace uri - should not affect expanded name
        assertEquals("prefix 'test' has unexpected mapping",
                "http://www.apache.org/jackrabbit/test", s.getNamespaceURI("test"));
        s.setNamespacePrefix("test", "urn:foo");
        assertEquals("{http://www.apache.org/jackrabbit/test}bar", s.getExpandedName(n));
        // use special namespace uri
        n = testRootNode.addNode("rep:bar");
        assertEquals("{internal}bar", s.getExpandedName(n));
    }

    public void testGetExpandedNameBrokenNamespace() throws RepositoryException {
        // empty namespace uri
        assertEquals("{}testroot", s.getExpandedName(testRootNode));

        String randomNamespacePrefix = "prefix-" + UUID.randomUUID();
        // below is not a valid namespace a.k.a. namespace URI
        String randomNamespaceName = "name-" + UUID.randomUUID();

        // register broken namespace prefix/name mapping
        s.getWorkspace().getNamespaceRegistry().registerNamespace(randomNamespacePrefix, randomNamespaceName);

        try {
            Node n = testRootNode.addNode(randomNamespacePrefix + ":qux");

            // there is no expanded name, thus we expect an exception here
            String result = s.getExpandedName(n);
            fail("there is no expanded name in this case, so we expect the call to fail, however we get: " + result);
        } catch (NamespaceException ex) {
            // expected
        } finally {
            s.getWorkspace().getNamespaceRegistry().unregisterNamespace(randomNamespacePrefix);
        }
    }

    public void testGetExpandedPath() throws RepositoryException {
        assertEquals("/{}testroot", s.getExpandedPath(testRootNode));
        Node n = testRootNode.addNode("test:bar").addNode("rep:bar");
        assertEquals("/{}testroot/{http://www.apache.org/jackrabbit/test}bar/{internal}bar", s.getExpandedPath(n));
        // now remap namespace uri - should not affect expanded name
        s.setNamespacePrefix("test", "urn:foo");
        assertEquals("/{}testroot/{http://www.apache.org/jackrabbit/test}bar/{internal}bar", s.getExpandedPath(n));
    }

    public void testGetPrincipalsForAdminSession() throws RepositoryException {
        Set<Principal> principals = s.getBoundPrincipals();
        assertEquals("Principals returned via getBoundPrincipals and session attribute must be equal", principals, s.getAttribute(RepositoryImpl.BOUND_PRINCIPALS));
        assertImmutablePrincipals(principals, "admin", EveryonePrincipal.getInstance().getName());
    }

    public void testGetPrincipalsForCustomUser() throws RepositoryException {
        // add test user being member of one group directly and another group transitively
        UserManager uMgr = s.getUserManager();
        // create the testUser
        String uid = generateId("testUser");
        SimpleCredentials creds = new SimpleCredentials(uid, uid.toCharArray());
        User testUser = uMgr.createUser(uid, uid);
        String gid = generateId("testGroup");
        Group testGroup = uMgr.createGroup(gid);
        testGroup.addMember(testUser);
        String gid2 = generateId("testGroup2");
        Group testGroup2 = uMgr.createGroup(gid2);
        testGroup2.addMember(testGroup);
        s.save();
        JackrabbitSession guest = (JackrabbitSession) getHelper().getRepository().login(creds);
        try {
            Set<Principal> principals = guest.getBoundPrincipals();
            assertEquals("Principals returned via getBoundPrincipals and session attribute must be equal", principals, guest.getAttribute(RepositoryImpl.BOUND_PRINCIPALS));
            assertImmutablePrincipals(principals, EveryonePrincipal.getInstance().getName(), gid, uid, gid2);
            assertFalse("Admin principal not expected", principals.contains(s.getPrincipalManager().getPrincipal("admin")));
        } finally {
            guest.logout();
        }
    }

    protected static String generateId(@NotNull String hint) {
        return hint + UUID.randomUUID();
    }

    public static void assertImmutablePrincipals(Collection<Principal> actualPrincipals, String... expectedPrincipalNames) {
        assertNotNull(actualPrincipals);
        for (String expectedPrincipalName  : expectedPrincipalNames) {
            assertTrue("Given collection did not contain expected principal name \'" + expectedPrincipalName + "'", actualPrincipals.stream().anyMatch(p -> p.getName().equals(expectedPrincipalName) ));
        }
        // make sure it is not modifiable
        assertThrows(UnsupportedOperationException.class, actualPrincipals::clear);
    }
}