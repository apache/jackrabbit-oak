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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * PrivilegeManagerTest...
 */
public class PrivilegeManagerTest extends AbstractPrivilegeTest {

    private PrivilegeManager privilegeManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        privilegeManager = getPrivilegeManager(superuser);
    }

    @After
    public void tearDown() throws Exception {
        privilegeManager = null;
        super.tearDown();
    }

    @Test
    public void testGetRegisteredPrivileges() throws RepositoryException {
        Privilege[] registered = privilegeManager.getRegisteredPrivileges();
        Set<Privilege> set = new HashSet<Privilege>();
        Privilege all = privilegeManager.getPrivilege(Privilege.JCR_ALL);
        set.add(all);
        set.addAll(Arrays.asList(all.getAggregatePrivileges()));

        for (Privilege p : registered) {
            assertTrue(p.getName(), set.remove(p));
        }
        assertTrue(set.isEmpty());
    }
    
    @Test
    public void testGetPrivilege() throws RepositoryException {
        Set<String> aggregatedPrivilegeNames = ImmutableSet.of("jcr:read",
                "jcr:modifyProperties", "jcr:write", "rep:write", "jcr:all");

        for (Privilege priv : privilegeManager.getRegisteredPrivileges()) {
            String privName = priv.getName();
            boolean isAggregate = aggregatedPrivilegeNames.contains(privName);
            assertPrivilege(priv, privName, isAggregate, false);
        }
    }

    @Test
    public void testJcrAll() throws RepositoryException {
        Privilege all = privilegeManager.getPrivilege(Privilege.JCR_ALL);
        assertPrivilege(all, "jcr:all", true, false);

        List<Privilege> decl = Arrays.asList(all.getDeclaredAggregatePrivileges());
        List<Privilege> aggr = new ArrayList<Privilege>(Arrays.asList(all.getAggregatePrivileges()));

        assertFalse(decl.contains(all));
        assertFalse(aggr.contains(all));

        // declared and aggregated privileges are the same for jcr:all
        assertTrue(decl.containsAll(aggr));

        // test individual built-in privileges are listed in the aggregates
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_READ)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_ADD_CHILD_NODES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_REMOVE_CHILD_NODES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_MODIFY_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_REMOVE_NODE)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_READ_ACCESS_CONTROL)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_MODIFY_ACCESS_CONTROL)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_LIFECYCLE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_LOCK_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_NODE_TYPE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_RETENTION_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_VERSION_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(Privilege.JCR_WRITE)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_WRITE)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_READ_NODES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_READ_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_ADD_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_ALTER_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_REMOVE_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_USER_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT)));

        // there may be no privileges left
        assertTrue(aggr.isEmpty());
    }

    @Test
    public void testGetPrivilegeFromName() throws AccessControlException, RepositoryException {
        Privilege p = privilegeManager.getPrivilege(Privilege.JCR_VERSION_MANAGEMENT);

        assertTrue(p != null);
        assertEquals(PrivilegeConstants.JCR_VERSION_MANAGEMENT, p.getName());
        assertFalse(p.isAggregate());

        p = privilegeManager.getPrivilege(Privilege.JCR_WRITE);

        assertTrue(p != null);
        assertEquals(PrivilegeConstants.JCR_WRITE, p.getName());
        assertTrue(p.isAggregate());
    }

    @Test
    public void testGetPrivilegesFromInvalidName() throws RepositoryException {
        try {
            privilegeManager.getPrivilege("unknown");
            fail("invalid privilege name");
        } catch (AccessControlException e) {
            // OK
        }
    }

    @Test
    public void testGetPrivilegesFromInvalidName2() throws RepositoryException {
    	String nonExistingPrivilegeName = "{http://www.nonexisting.com/1.0}nonexisting";
    	try{
    		privilegeManager.getPrivilege(nonExistingPrivilegeName);
    	} catch(AccessControlException e){
    		//expected
    	}
    }

    @Test
    public void testGetPrivilegesFromEmptyNames() {
        try {
            privilegeManager.getPrivilege("");
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
            privilegeManager.getPrivilege(null);
            fail("invalid privilege name (null)");
        } catch (Exception e) {
            // OK
        }
    }
}
