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

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.AccessDeniedException;
import javax.jcr.InvalidItemStateException;
import javax.jcr.NamespaceException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Workspace;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;

/**
 * Test privilege registration.
 */
public class PrivilegeRegistrationTest extends AbstractPrivilegeTest {

    private Repository repository;
    private Session session;
    private PrivilegeManager privilegeManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // create a separate repository in order to be able to remove registered privileges.
        repository = new Jcr().createRepository();
        session = getAdminSession();
        privilegeManager = getPrivilegeManager(session);

        // make sure the guest session has read access
        try {
            AccessControlUtils.addAccessControlEntry(session, "/", EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, true);
            session.save();
        } catch (RepositoryException e) {
            // failed to initialize
        }
    }
    @After
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            session.logout();
            repository = dispose(repository);
            privilegeManager = null;
        }
    }

    private Session getReadOnlySession() throws RepositoryException {
        return repository.login(getHelper().getReadOnlyCredentials());
    }

    private Session getAdminSession() throws RepositoryException {
        return repository.login(getHelper().getSuperuserCredentials());
    }

    @Test
    public void testRegisterPrivilegeWithReadOnly() throws RepositoryException {
        Session readOnly = getReadOnlySession();
        try {
            getPrivilegeManager(readOnly).registerPrivilege("test", true, new String[0]);
            fail("Only admin is allowed to register privileges.");
        } catch (AccessDeniedException e) {
            // success
        } finally {
            readOnly.logout();
        }
    }

    @Test
    public void testCustomDefinitionsWithCyclicReferences() throws RepositoryException {
        try {
            privilegeManager.registerPrivilege("cycl-1", false, new String[] {"cycl-1"});
            fail("Cyclic definitions must be detected upon registration.");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testCustomEquivalentDefinitions() throws RepositoryException {
        privilegeManager.registerPrivilege("custom4", false, new String[0]);
        privilegeManager.registerPrivilege("custom5", false, new String[0]);
        privilegeManager.registerPrivilege("custom2", false, new String[]{"custom4", "custom5"});

        List<String[]> equivalent = new ArrayList<String[]>();
        equivalent.add(new String[]{"custom4", "custom5"});
        equivalent.add(new String[] {"custom2", "custom4"});
        equivalent.add(new String[]{"custom2", "custom5"});
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

        for (String builtInName : builtIns.keySet()) {
            try {
                privilegeManager.registerPrivilege(builtInName, true, builtIns.get(builtInName));
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
    public void testRegisterCustomPrivileges() throws RepositoryException {
        Workspace workspace = session.getWorkspace();
        workspace.getNamespaceRegistry().registerNamespace("test", "http://www.apache.org/jackrabbit/test");

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

    /**
     * @since oak
     */
    @Test
    public void testRegisterCustomPrivilegesVisibleInContent() throws RepositoryException {
        Workspace workspace = session.getWorkspace();
        workspace.getNamespaceRegistry().registerNamespace("test", "http://www.apache.org/jackrabbit/test");

        Map<String, String[]> newCustomPrivs = new HashMap<String, String[]>();
        newCustomPrivs.put("new", new String[0]);
        newCustomPrivs.put("test:new", new String[0]);

        for (String name : newCustomPrivs.keySet()) {
            boolean isAbstract = true;
            String[] aggrNames = newCustomPrivs.get(name);

            Privilege registered = privilegeManager.registerPrivilege(name, isAbstract, aggrNames);

            Node privilegeRoot = session.getNode(PrivilegeConstants.PRIVILEGES_PATH);
            assertTrue(privilegeRoot.hasNode(name));
            Node privNode = privilegeRoot.getNode(name);
            assertTrue(privNode.getProperty(PrivilegeConstants.REP_IS_ABSTRACT).getBoolean());
            assertFalse(privNode.hasProperty(PrivilegeConstants.REP_AGGREGATES));
        }
    }

    /**
     * @since oak
     */
    @Test
    public void testCustomPrivilegeVisibleToNewSession() throws RepositoryException {
        boolean isAbstract = false;
        String privName = "testCustomPrivilegeVisibleToNewSession";
        privilegeManager.registerPrivilege(privName, isAbstract, new String[0]);

        Session s2 = getAdminSession();
        try {
            PrivilegeManager pm = getPrivilegeManager(s2);
            Privilege priv = pm.getPrivilege(privName);
            assertEquals(privName, priv.getName());
            assertEquals(isAbstract, priv.isAbstract());
            assertFalse(priv.isAggregate());
        } finally {
            s2.logout();
        }
    }

    /**
     * @since oak
     */
    @Test
    public void testCustomPrivilegeVisibleAfterRefresh() throws RepositoryException {
        Session s2 = getAdminSession();
        PrivilegeManager pm = getPrivilegeManager(s2);
        try {
            boolean isAbstract = false;
            String privName = "testCustomPrivilegeVisibleAfterRefresh";
            privilegeManager.registerPrivilege(privName, isAbstract, new String[0]);

            // before refreshing: privilege not visible
            try {
                Privilege priv = pm.getPrivilege(privName);
                fail("Custom privilege will show up after Session#refresh()");
            } catch (AccessControlException e) {
                // success
            }

            // latest after refresh privilege manager must be updated
            s2.refresh(true);
            Privilege priv = pm.getPrivilege(privName);
            assertEquals(privName, priv.getName());
            assertEquals(isAbstract, priv.isAbstract());
            assertFalse(priv.isAggregate());
        } finally {
            s2.logout();
        }
    }

    /**
     * @since oak
     */
    @Test
    public void testRegisterPrivilegeWithPendingChanges() throws RepositoryException {
        try {
            session.getRootNode().addNode("test");
            assertTrue(session.hasPendingChanges());
            privilegeManager.registerPrivilege("new", true, new String[0]);
            fail("Privileges may not be registered while there are pending changes.");
        } catch (InvalidItemStateException e) {
            // success
        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2015">OAK-2015</a>
     */
    @Test
    public void testJcrAllWithCustomPrivileges() throws Exception {
        Node testNode = session.getRootNode().addNode("test");
        String testPath = testNode.getPath();

        AccessControlUtils.grantAllToEveryone(session, testPath);
        session.save();

        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) session.getAccessControlManager();
        Privilege[] allPrivileges = AccessControlUtils.privilegesFromNames(session, Privilege.JCR_ALL);
        Set<Principal> principalSet = ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());

        assertTrue(acMgr.hasPrivileges(testPath, principalSet, allPrivileges));

        privilegeManager.registerPrivilege("customPriv", false, null);

        assertTrue(acMgr.hasPrivileges(testPath, principalSet, allPrivileges));
    }

    @Test
    public void testRegisterPrivilegeAggregatingJcrAll() throws Exception {
        privilegeManager.registerPrivilege("customPriv", false, null);

        try {
            privilegeManager.registerPrivilege("customPriv2", false, new String[]{"customPriv", Privilege.JCR_ALL});
            fail("Aggregation containing jcr:all is invalid.");
        } catch (RepositoryException e) {
            // success
            Throwable cause = e.getCause();
            assertTrue(cause instanceof CommitFailedException);
            assertEquals(53, ((CommitFailedException) cause).getCode());
        } finally {
            superuser.refresh(false);
        }
    }
}