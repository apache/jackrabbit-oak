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

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Group;
import org.junit.Test;

public class ReadWithGlobRestrictionTest extends AbstractEvaluationTest {

    @Test
    public void testGlobRestriction() throws Exception {
        deny(path, readPrivileges, createGlobRestriction("*/" + jcrPrimaryType));

        assertTrue(testAcMgr.hasPrivileges(path, readPrivileges));
        assertTrue(testSession.hasPermission(path, javax.jcr.Session.ACTION_READ));
        testSession.getNode(path);

        assertTrue(testAcMgr.hasPrivileges(childNPath, readPrivileges));
        assertTrue(testSession.hasPermission(childNPath, javax.jcr.Session.ACTION_READ));
        testSession.getNode(childNPath);

        String propPath = path + '/' + jcrPrimaryType;
        assertFalse(testSession.hasPermission(propPath, javax.jcr.Session.ACTION_READ));
        assertFalse(testSession.propertyExists(propPath));

        propPath = childNPath + '/' + jcrPrimaryType;
        assertFalse(testSession.hasPermission(propPath, javax.jcr.Session.ACTION_READ));
        assertFalse(testSession.propertyExists(propPath));
    }

    @Test
    public void testGlobRestriction2() throws Exception {
        Group group2 = getUserManager(superuser).createGroup(generateId("group2_"));
        Group group3 = getUserManager(superuser).createGroup(generateId("group3_"));
        superuser.save();

        try {
            Privilege[] readPrivs = privilegesFromName(Privilege.JCR_READ);

            modify(path, getTestGroup().getPrincipal(), readPrivs, true, createGlobRestriction("/*"));
            allow(path, group2.getPrincipal(), readPrivs);
            deny(path, group3.getPrincipal(), readPrivs);

            Set<Principal> principals = new HashSet<Principal>();
            principals.add(getTestGroup().getPrincipal());
            principals.add(group2.getPrincipal());
            principals.add(group3.getPrincipal());

            assertFalse(((JackrabbitAccessControlManager) acMgr).hasPrivileges(path, principals, readPrivs));
            assertFalse(((JackrabbitAccessControlManager) acMgr).hasPrivileges(childNPath, principals, readPrivs));
        } finally {
            group2.remove();
            group3.remove();
            superuser.save();
        }
    }

    @Test
    public void testGlobRestriction3() throws Exception {
        Group group2 = getUserManager(superuser).createGroup(generateId("group2_"));
        Group group3 = getUserManager(superuser).createGroup(generateId("group3_"));
        superuser.save();

        try {
            Privilege[] readPrivs = privilegesFromName(Privilege.JCR_READ);

            allow(path, group2.getPrincipal(), readPrivs);
            deny(path, group3.getPrincipal(), readPrivs);
            modify(path, getTestGroup().getPrincipal(), readPrivs, true, createGlobRestriction("/*"));

            Set<Principal> principals = new HashSet<Principal>();
            principals.add(getTestGroup().getPrincipal());
            principals.add(group2.getPrincipal());
            principals.add(group3.getPrincipal());

            assertFalse(((JackrabbitAccessControlManager) acMgr).hasPrivileges(path, principals, readPrivs));
            assertTrue(((JackrabbitAccessControlManager) acMgr).hasPrivileges(childNPath, principals, readPrivs));
        } finally {
            group2.remove();
            group3.remove();
            superuser.save();
        }
    }

    @Test
    public void testGlobRestriction4()throws Exception{
        Node a = superuser.getNode(path).addNode("a");
        allow(path, readPrivileges);
        deny(path, readPrivileges, createGlobRestriction("*/anotherpath"));

        String aPath = a.getPath();
        assertTrue(testSession.nodeExists(aPath));
        Node n = testSession.getNode(aPath);

        Node test = testSession.getNode(path);
        assertTrue(test.hasNode("a"));
        Node n2 = test.getNode("a");
        assertTrue(n.isSame(n2));
    }

    @Test
    public void testGlobRestriction5()throws Exception{
        Node a = superuser.getNode(path).addNode("a");
        allow(path, readPrivileges);
        deny(path, readPrivileges, createGlobRestriction("*/anotherpath"));
        allow(a.getPath(), repWritePrivileges);

        String aPath = a.getPath();
        assertTrue(testSession.nodeExists(aPath));
        Node n = testSession.getNode(aPath);

        Node test = testSession.getNode(path);
        assertTrue(test.hasNode("a"));
        Node n2 = test.getNode("a");
        assertTrue(n.isSame(n2));
    }

    @Test
    public void testGlobRestriction6() throws Exception {
        Privilege[] readPrivs = privilegesFromName(Privilege.JCR_READ);

        allow(path, readPrivs);
        deny(path, readPrivs, createGlobRestriction("/*"));

        assertTrue(testSession.nodeExists(path));
        assertFalse(testSession.propertyExists(path + '/' + JcrConstants.JCR_PRIMARYTYPE));
        assertFalse(testSession.nodeExists(childNPath));
        assertFalse(testSession.propertyExists(childPPath));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2412">OAK-2412</a>
     */
    @Test
    public void testEmptyGlobRestriction() throws Exception{
        Node grandchild = superuser.getNode(childNPath).addNode("child");
        String ccPath = grandchild.getPath();
        superuser.save();

        // first deny access to 'path' (read-access is granted in the test setup)
        deny(path, readPrivileges);
        assertFalse(canReadNode(testSession, path));
        assertFalse(canReadNode(testSession, childNPath));
        assertFalse(canReadNode(testSession, ccPath));
        assertFalse(testSession.propertyExists(childchildPPath));

        allow(childNPath, readPrivileges, createGlobRestriction(""));
        assertFalse(canReadNode(testSession, path));
        assertTrue(canReadNode(testSession, childNPath));
        assertFalse(canReadNode(testSession, ccPath));
        assertFalse(testSession.propertyExists(childchildPPath));
        assertFalse(testSession.propertyExists(childNPath + '/' + JcrConstants.JCR_PRIMARYTYPE));

        allow(ccPath, readPrivileges);
        assertTrue(canReadNode(testSession, ccPath));
        assertTrue(testSession.propertyExists(ccPath + '/' + JcrConstants.JCR_PRIMARYTYPE));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2412">OAK-2412</a>
     */
    @Test
    public void testEmptyGlobRestriction2() throws Exception{
        Node grandchild = superuser.getNode(childNPath).addNode("child");
        String ccPath = grandchild.getPath();
        superuser.save();

        // first deny access to 'path' (read-access is granted in the test setup)
        deny(path, readPrivileges);
        assertFalse(canReadNode(testSession, path));
        assertFalse(canReadNode(testSession, childNPath));
        assertFalse(canReadNode(testSession, ccPath));
        assertFalse(testSession.propertyExists(childchildPPath));

        allow(path, readPrivileges, createGlobRestriction(""));
        assertTrue(canReadNode(testSession, path));
        assertFalse(canReadNode(testSession, childNPath));
        assertFalse(canReadNode(testSession, ccPath));
        assertFalse(testSession.propertyExists(childchildPPath));
        assertFalse(testSession.propertyExists(childNPath + '/' + JcrConstants.JCR_PRIMARYTYPE));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2412">OAK-2412</a>
     */
    @Test
    public void testEmptyGlobRestriction3() throws Exception {
        Group group1 = getTestGroup();
        Group group2 = getUserManager(superuser).createGroup(generateId("group2_"));
        group2.addMember(testUser);
        Group group3 = getUserManager(superuser).createGroup(generateId("group3_"));
        superuser.save();
        try {

            assertTrue(group1.isDeclaredMember(testUser));
            assertTrue(group2.isDeclaredMember(testUser));
            assertFalse(group3.isDeclaredMember(testUser));

            deny(path, group1.getPrincipal(), readPrivileges);
            modify(path, group1.getPrincipal(), readPrivileges, true, createGlobRestriction(""));

            deny(childNPath, group2.getPrincipal(), readPrivileges);
            modify(childNPath, group2.getPrincipal(), readPrivileges, true, createGlobRestriction(""));

            deny(childNPath2, group3.getPrincipal(), readPrivileges);
            modify(childNPath2, group3.getPrincipal(), readPrivileges, true, createGlobRestriction(""));

            // need to recreate testUser session to force subject being populated
            // with membership that has been added _after_ the testSession creation.
            Session userSession = getHelper().getRepository().login(creds);
            assertTrue(canReadNode(userSession, path));
            assertTrue(canReadNode(userSession, childNPath));
            assertFalse(canReadNode(userSession, childNPath2));
        } finally {
            group2.remove();
            group3.remove();
            superuser.save();
        }
    }

    @Test
    public void testTwoWildCards() throws Exception {
        Node n = superuser.getNode(childNPath);
        Node n100 = n.addNode("100");

        Node n110 = n100.addNode("110");
        Node n120 = n100.addNode("120");

        Node n111 = n110.addNode("111");
        Node n112 = n110.addNode("112");

        Node n121 = n120.addNode("121");
        Node n122 = n120.addNode("122");

        deny(childNPath, privilegesFromName(Privilege.JCR_ALL), createGlobRestriction("/*/110/*"));
        superuser.save();

        assertTrue(canReadNode(testSession, n100.getPath()));

        assertTrue(canReadNode(testSession, n110.getPath()));
        assertFalse(canReadNode(testSession, n111.getPath()));
        assertFalse(canReadNode(testSession, n112.getPath()));

        assertTrue(canReadNode(testSession, n120.getPath()));
        assertTrue(canReadNode(testSession, n121.getPath()));
        assertTrue(canReadNode(testSession, n122.getPath()));
    }
}