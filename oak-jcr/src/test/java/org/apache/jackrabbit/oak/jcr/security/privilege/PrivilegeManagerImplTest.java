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
package org.apache.jackrabbit.oak.jcr.security.privilege;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Before;
import org.junit.Test;

/**
 * PrivilegeManagerTest...
 */
public class PrivilegeManagerImplTest extends AbstractJCRTest {

    private PrivilegeManager privilegeMgr;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        privilegeMgr = ((JackrabbitWorkspace) superuser.getWorkspace()).getPrivilegeManager();
    }

    @Test
    public void testRegisteredPrivileges() throws RepositoryException {
        Privilege[] ps = privilegeMgr.getRegisteredPrivileges();

        List<Privilege> l = new ArrayList<Privilege>(Arrays.asList(ps));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_READ)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_ADD_CHILD_NODES)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_REMOVE_CHILD_NODES)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_MODIFY_PROPERTIES)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_REMOVE_NODE)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_READ_ACCESS_CONTROL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_MODIFY_ACCESS_CONTROL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_WRITE)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_ALL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_LIFECYCLE_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_LOCK_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_NODE_TYPE_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_RETENTION_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_VERSION_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:write")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:addProperties")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:alterProperties")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:removeProperties")));
        // including repo-level operation privileges
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:namespaceManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:nodeTypeDefinitionManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:workspaceManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:privilegeManagement")));

        assertTrue(l.isEmpty());
    }

    @Test
    public void testAllPrivilege() throws RepositoryException {
        Privilege p = privilegeMgr.getPrivilege(Privilege.JCR_ALL);
        assertEquals("jcr:all",p.getName());
        assertTrue(p.isAggregate());
        assertFalse(p.isAbstract());

        List<Privilege> l = new ArrayList<Privilege>(Arrays.asList(p.getAggregatePrivileges()));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_READ)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_ADD_CHILD_NODES)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_REMOVE_CHILD_NODES)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_MODIFY_PROPERTIES)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_REMOVE_NODE)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_READ_ACCESS_CONTROL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_MODIFY_ACCESS_CONTROL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_LIFECYCLE_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_LOCK_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_NODE_TYPE_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_RETENTION_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_VERSION_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_WRITE)));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:write")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:addProperties")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:alterProperties")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:removeProperties")));
        // including repo-level operation privileges
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:namespaceManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:nodeTypeDefinitionManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:workspaceManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:privilegeManagement")));
        assertTrue(l.isEmpty());

        l = new ArrayList<Privilege>(Arrays.asList(p.getDeclaredAggregatePrivileges()));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_READ)));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:write")));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_READ_ACCESS_CONTROL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_MODIFY_ACCESS_CONTROL)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_LIFECYCLE_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_LOCK_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_RETENTION_MANAGEMENT)));
        assertTrue(l.remove(privilegeMgr.getPrivilege(Privilege.JCR_VERSION_MANAGEMENT)));
        // including repo-level operation privileges
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:namespaceManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:nodeTypeDefinitionManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("jcr:workspaceManagement")));
        assertTrue(l.remove(privilegeMgr.getPrivilege("rep:privilegeManagement")));

        assertTrue(l.isEmpty());
    }

    @Test
    public void testGetPrivilegeFromName() throws AccessControlException, RepositoryException {
        Privilege p = privilegeMgr.getPrivilege(Privilege.JCR_READ);

        assertTrue(p != null);
        assertEquals("jcr:read", p.getName());
        assertFalse(p.isAggregate());

        p = privilegeMgr.getPrivilege(Privilege.JCR_WRITE);

        assertTrue(p != null);
        assertEquals("jcr:write", p.getName());
        assertTrue(p.isAggregate());
    }

    @Test
    public void testGetPrivilegesFromInvalidName() throws RepositoryException {
        try {
            privilegeMgr.getPrivilege("unknown");
            fail("invalid privilege name");
        } catch (AccessControlException e) {
            // OK
        }
    }

    @Test
    public void testGetPrivilegesFromEmptyNames() {
        try {
            privilegeMgr.getPrivilege("");
            fail("invalid privilege name array");
        } catch (AccessControlException e) {
            // OK
        } catch (RepositoryException e) {
            // OK
        }
    }

    @Test
    public void testGetPrivilegesFromNullNames() {
        try {
            privilegeMgr.getPrivilege(null);
            fail("invalid privilege name (null)");
        } catch (Exception e) {
            // OK
        }
    }

    // TODO test privilege registration
}