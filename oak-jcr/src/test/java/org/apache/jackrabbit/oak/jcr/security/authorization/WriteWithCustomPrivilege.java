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

import javax.jcr.Session;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.junit.Test;

public class WriteWithCustomPrivilege extends AbstractEvaluationTest {

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        PrivilegeManager privilegeManager = ((JackrabbitWorkspace) superuser.getWorkspace()).getPrivilegeManager();
        try {
            privilegeManager.getPrivilege("replicate");
        } catch (AccessControlException e) {
            privilegeManager.registerPrivilege("replicate", false, null);
        }
    }

    @Test
    public void testWriteAndCustomPrivilege() throws Exception {
        Privilege[] privs = privilegesFromNames(new String[] {
                Privilege.JCR_VERSION_MANAGEMENT, Privilege.JCR_LOCK_MANAGEMENT,
                "replicate", "rep:write"});
        allow(path, testGroup.getPrincipal(), privs);

        assertTrue(testAcMgr.hasPrivileges(path, privilegesFromName("replicate")));

        assertTrue(testSession.hasPermission(path + "/newNode", Session.ACTION_ADD_NODE));
        assertTrue(testSession.hasPermission(childPPath, Session.ACTION_SET_PROPERTY));
        assertTrue(testSession.hasPermission(path + "/newProperty", Session.ACTION_SET_PROPERTY));
        assertTrue(testSession.hasPermission(path + "/newProperty", Permissions.getString(Permissions.ADD_PROPERTY)));

        testSession.getNode(path).setProperty("newProperty", "value");
        testSession.save();

        deny(path, testUser.getPrincipal(), privilegesFromName("replicate"));

        assertFalse(testAcMgr.hasPrivileges(path, privilegesFromName("replicate")));

        assertTrue(testSession.hasPermission(childPPath, Session.ACTION_SET_PROPERTY));
        assertTrue(testSession.hasPermission(path + "/newProperty2", Session.ACTION_SET_PROPERTY));
        assertTrue(testSession.hasPermission(path + "/newProperty2", Permissions.getString(Permissions.ADD_PROPERTY)));

        testSession.getNode(path).setProperty("newProperty2", "value");
        testSession.save();
    }

    @Test
    public void testWriteAndCustomPrivilege2() throws Exception {
        Privilege[] privs = privilegesFromNames(new String[] {
                Privilege.JCR_VERSION_MANAGEMENT, Privilege.JCR_LOCK_MANAGEMENT,
                "replicate", "rep:write"});
        allow(path, testGroup.getPrincipal(), privs);

        assertTrue(testAcMgr.hasPrivileges(path, privilegesFromName("replicate")));
        assertTrue(testSession.hasPermission(path + "/newNode", Session.ACTION_ADD_NODE));
        testSession.getNode(path).addNode("newNode");
        testSession.save();

        deny(path, testUser.getPrincipal(), privilegesFromName("replicate"));

        assertFalse(testAcMgr.hasPrivileges(path, privilegesFromName("replicate")));
        assertTrue(testSession.hasPermission(path + "/newNode2", Session.ACTION_ADD_NODE));
        testSession.getNode(path).addNode("newNode2");
        testSession.save();
    }
}
