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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.AccessDeniedException;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.NamespaceException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.Workspace;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.jcr.RepositoryImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * PrivilegeManagerTest...
 *
 * TODO: more tests for cyclic aggregation
 */
public class PrivilegeManagerImplTest implements PrivilegeConstants {

    private static final Credentials ADMIN =
            new SimpleCredentials("admin", "admin".toCharArray());

    private Repository repository;

    private PrivilegeManager privilegeManager;

    @Before
    public void setUp() throws RepositoryException {
        repository = new RepositoryImpl();
        privilegeManager = getPrivilegeManager(ADMIN);
    }

    @After
    public void tearDown() {
        privilegeManager = null;
        repository = null;
    }

    private PrivilegeManager getPrivilegeManager(Credentials credentials)
            throws RepositoryException {
        Workspace workspace = repository.login(credentials).getWorkspace();
        // FIXME workaround to ensure built in node types are registered
        workspace.getNodeTypeManager();
        return ((JackrabbitWorkspace) workspace).getPrivilegeManager();
    }

    private static String[] getAggregateNames(String... names) {
        return names;
    }

    private static void assertContainsDeclared(Privilege privilege, String aggrName) {
        boolean found = false;
        for (Privilege p : privilege.getDeclaredAggregatePrivileges()) {
            if (aggrName.equals(p.getName())) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }

    private void assertPrivilege(Privilege priv, String name, boolean isAggregate, boolean isAbstract) {
        assertNotNull(priv);
        assertEquals(name, priv.getName());
        assertEquals(isAggregate, priv.isAggregate());
        assertEquals(isAbstract, priv.isAbstract());
    }

    public void testGetRegisteredPrivileges() throws RepositoryException {
        Privilege[] registered = privilegeManager.getRegisteredPrivileges();
        Set<Privilege> set = new HashSet<Privilege>();
        Privilege all = privilegeManager.getPrivilege(Privilege.JCR_ALL);
        set.add(all);
        set.addAll(Arrays.asList(all.getAggregatePrivileges()));

        for (Privilege p : registered) {
            assertTrue(set.remove(p));
        }
        assertTrue(set.isEmpty());
    }
    
    public void testGetPrivilege() throws RepositoryException {
        for (String privName : NON_AGGR_PRIVILEGES) {
            Privilege p = privilegeManager.getPrivilege(privName);
            assertPrivilege(p, privName, false, false);
        }

        for (String privName : AGGR_PRIVILEGES) {
            Privilege p = privilegeManager.getPrivilege(privName);
            assertPrivilege(p, privName, true, false);
        }
    }

    public void testJcrAll() throws RepositoryException {
        Privilege all = privilegeManager.getPrivilege(Privilege.JCR_ALL);
        assertPrivilege(all, JCR_ALL, true, false);

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
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_ADD_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_ALTER_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_REMOVE_PROPERTIES)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT)));
        assertTrue(aggr.remove(privilegeManager.getPrivilege(PrivilegeConstants.REP_PRIVILEGE_MANAGEMENT)));

        // there may be no privileges left
        assertTrue(aggr.isEmpty());
    }

    @Test
    public void testGetPrivilegeFromName() throws AccessControlException, RepositoryException {
        Privilege p = privilegeManager.getPrivilege(Privilege.JCR_READ);

        assertTrue(p != null);
        assertEquals("jcr:read", p.getName());
        assertFalse(p.isAggregate());

        p = privilegeManager.getPrivilege(Privilege.JCR_WRITE);

        assertTrue(p != null);
        assertEquals("jcr:write", p.getName());
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

    @Test
    public void testRegisterPrivilegeWithIllegalName() {
        Map<String, String[]> illegal = new HashMap<String, String[]>();
        // invalid privilege name
        illegal.put(null, new String[0]);
        illegal.put("", new String[0]);
        illegal.put("invalid:privilegeName", new String[0]);
        illegal.put(".e:privilegeName", new String[0]);
        // invalid aggregate names
        illegal.put("newPrivilege", new String[] {"invalid:privilegeName"});
        illegal.put("newPrivilege", new String[] {".e:privilegeName"});
        illegal.put("newPrivilege", new String[] {null});
        illegal.put("newPrivilege", new String[] {""});

        for (String illegalName : illegal.keySet()) {
            try {
                privilegeManager.registerPrivilege(illegalName, true, illegal.get(illegalName));
                fail("Illegal name -> Exception expected");
            } catch (NamespaceException e) {
                // success
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRegisterReservedName() {
        Map<String, String[]> illegal = new HashMap<String, String[]>();
        // invalid privilege name
        illegal.put(null, new String[0]);
        illegal.put("jcr:privilegeName", new String[0]);
        illegal.put("rep:privilegeName", new String[0]);
        illegal.put("nt:privilegeName", new String[0]);
        illegal.put("mix:privilegeName", new String[0]);
        illegal.put("sv:privilegeName", new String[0]);
        illegal.put("xml:privilegeName", new String[0]);
        illegal.put("xmlns:privilegeName", new String[0]);
        // invalid aggregate names
        illegal.put("newPrivilege", new String[] {"jcr:privilegeName"});

        for (String illegalName : illegal.keySet()) {
            try {
                privilegeManager.registerPrivilege(illegalName, true, illegal.get(illegalName));
                fail("Illegal name -> Exception expected");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRegisterPrivilegeWithReadOnly() throws RepositoryException {
        try {
            getPrivilegeManager(new GuestCredentials()).registerPrivilege("test", true, new String[0]);
            fail("Only admin is allowed to register privileges.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testCustomDefinitionsWithCyclicReferences() throws RepositoryException {
        try {
            privilegeManager.registerPrivilege("cycl-1", false, new String[] {"cycl-1"});
            fail("Cyclic definitions must be detected upon registry startup.");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testCustomEquivalentDefinitions() throws RepositoryException {
        privilegeManager.registerPrivilege("custom4", false, new String[0]);
        privilegeManager.registerPrivilege("custom5", false, new String[0]);
        privilegeManager.registerPrivilege("custom2", false, new String[] {"custom4", "custom5"});

        List<String[]> equivalent = new ArrayList<String[]>();
        equivalent.add(new String[] {"custom4", "custom5"});
        equivalent.add(new String[] {"custom2", "custom4"});
        equivalent.add(new String[] {"custom2", "custom5"});
        int cnt = 6;
        for (String[] aggrNames : equivalent) {
            try {
                // the equivalent definition to 'custom1'
                String name = "custom"+(cnt++);
                privilegeManager.registerPrivilege(name, false, aggrNames);
                fail("Equivalent '"+name+"' definitions must be detected.");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRegisterBuiltInPrivilege() throws RepositoryException {
        Map<String, String[]> builtIns = new HashMap<String, String[]>();
        builtIns.put(PrivilegeConstants.JCR_READ, new String[0]);
        builtIns.put(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT, new String[] {PrivilegeConstants.JCR_ADD_CHILD_NODES});
        builtIns.put(PrivilegeConstants.REP_WRITE, new String[0]);
        builtIns.put(PrivilegeConstants.JCR_ALL, new String[0]);

        for (String builtInName : builtIns.keySet()) {
            try {
                privilegeManager.registerPrivilege(builtInName, false, builtIns.get(builtInName));
                fail("Privilege name " +builtInName+ " already in use -> Exception expected");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRegisterInvalidNewAggregate() throws RepositoryException {
        Map<String, String[]> newAggregates = new LinkedHashMap<String, String[]>();
        // same as jcr:read
        newAggregates.put("jcrReadAggregate", getAggregateNames(PrivilegeConstants.JCR_READ));
        // aggregated combining built-in and an unknown privilege
        newAggregates.put("newAggregate2", getAggregateNames(PrivilegeConstants.JCR_READ, "unknownPrivilege"));
        // aggregate containing unknown privilege
        newAggregates.put("newAggregate3", getAggregateNames("unknownPrivilege"));
        // custom aggregated contains itself
        newAggregates.put("newAggregate4", getAggregateNames("newAggregate"));
        // same as rep:write
        newAggregates.put("repWriteAggregate", getAggregateNames(PrivilegeConstants.JCR_MODIFY_PROPERTIES, PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT, PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_NODE));
        // aggregated combining built-in and unknown custom
        newAggregates.put("newAggregate5", getAggregateNames(PrivilegeConstants.JCR_READ, "unknownPrivilege"));

        for (String name : newAggregates.keySet()) {
            try {
                privilegeManager.registerPrivilege(name, true, newAggregates.get(name));
                fail("New aggregate "+ name +" referring to unknown Privilege  -> Exception expected");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRegisterInvalidNewAggregate2() throws RepositoryException {
        Map<String, String[]> newCustomPrivs = new LinkedHashMap<String, String[]>();
        newCustomPrivs.put("new", new String[0]);
        newCustomPrivs.put("new2", new String[0]);
        Set<String> decl = new HashSet<String>();
        decl.add("new");
        decl.add("new2");
        newCustomPrivs.put("new3", getAggregateNames("new", "new2"));

        for (String name : newCustomPrivs.keySet()) {
            boolean isAbstract = true;
            String[] aggrNames = newCustomPrivs.get(name);
            privilegeManager.registerPrivilege(name, isAbstract, aggrNames);
        }

        Map<String, String[]> newAggregates = new LinkedHashMap<String, String[]>();
         // other illegal aggregates already represented by registered definition.
        newAggregates.put("newA2", getAggregateNames("new"));
        newAggregates.put("newA3", getAggregateNames("new2"));

        for (String name : newAggregates.keySet()) {
            boolean isAbstract = false;
            String[] aggrNames = newAggregates.get(name);

            try {
                privilegeManager.registerPrivilege(name, isAbstract, aggrNames);
                fail("Invalid aggregation in definition '"+ name.toString()+"' : Exception expected");
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testRegisterCustomPrivileges() throws RepositoryException {
        Workspace workspace = repository.login(ADMIN).getWorkspace();
        workspace.getNamespaceRegistry().registerNamespace(
                "test", "http://www.apache.org/jackrabbit/test");

        Map<String, String[]> newCustomPrivs = new HashMap<String, String[]>();
        newCustomPrivs.put("new", new String[0]);
        newCustomPrivs.put("test:new", new String[0]);

        for (String name : newCustomPrivs.keySet()) {
            boolean isAbstract = true;
            String[] aggrNames = newCustomPrivs.get(name);

            Privilege registered = privilegeManager.registerPrivilege(name, isAbstract, aggrNames);

            // validate definition
            Privilege privilege = privilegeManager.getPrivilege(name);
            assertNotNull(privilege);
            assertEquals(name, privilege.getName());
            assertTrue(privilege.isAbstract());
            assertEquals(0, privilege.getDeclaredAggregatePrivileges().length);
            assertContainsDeclared(privilegeManager.getPrivilege(PrivilegeConstants.JCR_ALL), name);
        }

        Map<String, String[]> newAggregates = new HashMap<String, String[]>();
        // a new aggregate of custom privileges
        newAggregates.put("newA2", getAggregateNames("test:new", "new"));
        // a new aggregate of custom and built-in privilege
        newAggregates.put("newA1", getAggregateNames("new", PrivilegeConstants.JCR_READ));
        // aggregating built-in privileges
        newAggregates.put("aggrBuiltIn", getAggregateNames(PrivilegeConstants.JCR_MODIFY_PROPERTIES, PrivilegeConstants.JCR_READ));

        for (String name : newAggregates.keySet()) {
            boolean isAbstract = false;
            String[] aggrNames = newAggregates.get(name);
            privilegeManager.registerPrivilege(name, isAbstract, aggrNames);
            Privilege p = privilegeManager.getPrivilege(name);

            assertNotNull(p);
            assertEquals(name, p.getName());
            assertFalse(p.isAbstract());

            for (String n : aggrNames) {
                assertContainsDeclared(p, n);
            }
            assertContainsDeclared(privilegeManager.getPrivilege(PrivilegeConstants.JCR_ALL), name);
        }
    }

    @Test
    public void testCustomPrivilegeVisibleToNewSession() throws RepositoryException {
        boolean isAbstract = false;
        String privName = "testCustomPrivilegeVisibleToNewSession";
        privilegeManager.registerPrivilege(privName, isAbstract, new String[0]);

        PrivilegeManager pm = getPrivilegeManager(ADMIN);
        Privilege priv = pm.getPrivilege(privName);
        assertEquals(privName, priv.getName());
        assertEquals(isAbstract, priv.isAbstract());
        assertFalse(priv.isAggregate());
    }

//    FIXME: Session#refresh must refresh privilege definitions
//    @Test
//    public void testCustomPrivilegeVisibleAfterRefresh() throws RepositoryException {
//        Session s2 = getHelper().getSuperuserSession();
//        try {
//            boolean isAbstract = false;
//            String privName = "testCustomPrivilegeVisibleAfterRefresh";
//            privilegeManager.registerPrivilege(privName, isAbstract, new String[0]);
//
//            // before refreshing: privilege not visible
//            PrivilegeManager pm = getPrivilegeManager(s2);
//            try {
//                Privilege priv = pm.getPrivilege(privName);
//                fail("Custom privilege must show up after Session#refresh()");
//            } catch (AccessControlException e) {
//                // success
//            }
//
//            // after refresh privilege manager must be updated
//            s2.refresh(true);
//            Privilege priv = pm.getPrivilege(privName);
//            assertEquals(privName, priv.getName());
//            assertEquals(isAbstract, priv.isAbstract());
//            assertFalse(priv.isAggregate());
//        } finally {
//            s2.logout();
//        }
//    }
}