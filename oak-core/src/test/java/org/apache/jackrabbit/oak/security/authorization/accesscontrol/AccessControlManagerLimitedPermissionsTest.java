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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.AccessDeniedException;
import javax.jcr.NamespaceRegistry;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.security.authorization.accesscontrol.AbstractAccessControlTest.assertPolicies;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_RESTRICTIONS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AccessControlManagerLimitedPermissionsTest extends AbstractSecurityTest {

    private static final String TEST_PREFIX = "jr";
    private static final String TEST_URI = "http://jackrabbit.apache.org";

    private static final String TEST_NAME = TEST_PREFIX + ":testRoot";
    private String testPath;
    private String childPath;

    private NamePathMapper npMapper;

    private Root testRoot;
    private AccessControlManagerImpl testAcMgr;

    private Privilege[] testPrivileges;
    private Principal testPrincipal;

    @Before
    public void before() throws Exception {
        super.before();

        NamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        nsRegistry.registerNamespace(TEST_PREFIX, TEST_URI);

        NameMapper nameMapper = new GlobalNameMapper(root);
        npMapper = new NamePathMapperImpl(nameMapper);

        Tree testTree = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), TEST_NAME, JcrConstants.NT_UNSTRUCTURED);
        testPath = testTree.getPath();
        Tree child = TreeUtil.addChild(testTree, "child", JcrConstants.NT_UNSTRUCTURED);
        childPath = child.getPath();
        root.commit();

        testRoot = createTestSession().getLatestRoot();
        testAcMgr = new AccessControlManagerImpl(testRoot, getNamePathMapper(), getSecurityProvider());
        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_READ);
        testPrincipal = getTestUser().getPrincipal();    }

    @After
    public void after() throws Exception {
        try {
            testRoot.getContentSession().close();
        } finally {
            super.after();
        }
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return npMapper;
    }

    private void setupPolicy(@Nullable String path, @Nullable Privilege... privileges) throws RepositoryException {
        Privilege[] privs = (privileges == null || privileges.length == 0) ? testPrivileges : privileges;
        TestUtility.setupPolicy(getAccessControlManager(root), path, testPrincipal, privs, true, TestUtility.getGlobRestriction("*", getValueFactory(root)), null);
    }

    @NotNull
    private List<String> getAcContentPaths() throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);
        ACL policy = TestUtility.getApplicablePolicy(acMgr, testPath);
        policy.addEntry(testPrincipal, testPrivileges, true, TestUtility.getGlobRestriction("*", getValueFactory(root)));
        acMgr.setPolicy(testPath, policy);

        String aclPath = testPath + '/' + REP_POLICY;
        Tree acl = root.getTree(aclPath);
        assertTrue(acl.exists());
        Iterator<Tree> aces = acl.getChildren().iterator();
        assertTrue(aces.hasNext());
        Tree ace = aces.next();
        assertNotNull(ace);

        List<String> acContentPath = new ArrayList<>();
        acContentPath.add(aclPath);
        acContentPath.add(ace.getPath());

        Tree rest = ace.getChild(REP_RESTRICTIONS);
        if (rest.exists()) {
            acContentPath.add(rest.getPath());
        }
        return acContentPath;
    }

    /**
     * @since OAK 1.0 As of OAK AccessControlManager#hasPrivilege will throw
     * PathNotFoundException in case the node associated with a given path is
     * not readable to the editing session (compatibility with the specification
     * which was missing in jackrabbit).
     */
    @Test
    public void testHasPrivilegesNotAccessiblePath() throws Exception {
        List<String> notAccessible = new ArrayList<>();
        notAccessible.add(PathUtils.ROOT_PATH);
        notAccessible.addAll(getAcContentPaths());

        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, getPrincipals(root.getContentSession()), privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, getPrincipals(testRoot.getContentSession()), privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
        for (String path : notAccessible) {
            try {
                testAcMgr.hasPrivileges(path, Set.of(), privs);
                fail("AccessControlManager#hasPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }
    }

    @Test
    public void testTestSessionHasRepoPrivileges() throws Exception {
        assertFalse(testAcMgr.hasPrivileges(null, testPrivileges));
        assertFalse(testAcMgr.hasPrivileges(null, getPrincipals(testRoot.getContentSession()), testPrivileges));
    }

    @Test
    public void testHasRepoPrivilegesNoAccessToPrincipals() throws Exception {
        // the test-session doesn't have sufficient permissions to read privilege set for admin session.
        try {
            testAcMgr.getPrivileges(null, getPrincipals(adminSession));
            fail("testSession doesn't have sufficient permission to read access control information");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test(expected = AccessDeniedException.class)
    public void testHasRepoPrivilegesForEmptyPrincipalSet() throws Exception {
        // the test-session doesn't have sufficient permissions to read privilege set.
        testAcMgr.getPrivileges(null, Collections.emptySet());
    }

    @Test
    public void testHasPrivileges() throws Exception {
        setupPolicy(testPath);
        root.commit();

        testRoot.refresh();

        // granted privileges
        List<Privilege[]> granted = new ArrayList<>();
        granted.add(privilegesFromNames(PrivilegeConstants.JCR_READ));
        granted.add(privilegesFromNames(PrivilegeConstants.REP_READ_NODES));
        granted.add(privilegesFromNames(PrivilegeConstants.REP_READ_PROPERTIES));
        granted.add(privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
        granted.add(testPrivileges);

        for (Privilege[] privileges : granted) {
            assertTrue(testAcMgr.hasPrivileges(testPath, privileges));
            assertTrue(testAcMgr.hasPrivileges(testPath, getPrincipals(testRoot.getContentSession()), privileges));
        }

        // denied privileges
        List<Privilege[]> denied = new ArrayList<>();
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_ALL));
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_WRITE));
        denied.add(privilegesFromNames(PrivilegeConstants.JCR_LOCK_MANAGEMENT));

        for (Privilege[] privileges : denied) {
            assertFalse(testAcMgr.hasPrivileges(testPath, privileges));
            assertFalse(testAcMgr.hasPrivileges(testPath, getPrincipals(testRoot.getContentSession()), privileges));
        }
    }

    @Test(expected = AccessDeniedException.class)
    public void testHasPrivilegesForPrincipals() throws Exception {
        setupPolicy(testPath);
        root.commit();

        testRoot.refresh();

        // but for 'admin' the test-session doesn't have sufficient privileges
        testAcMgr.getPrivileges(testPath, getPrincipals(adminSession));
    }

    /**
     * @since OAK 1.0 As of OAK AccessControlManager#hasPrivilege will throw
     * PathNotFoundException in case the node associated with a given path is
     * not readable to the editing session.
     */
    @Test
    public void testGetPrivilegesNotAccessiblePath() throws Exception {
        List<String> notAccessible = new ArrayList<>();
        notAccessible.add("/");
        notAccessible.addAll(getAcContentPaths());

        for (String path : notAccessible) {
            try {
                testAcMgr.getPrivileges(path);
                fail("AccessControlManager#getPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }

        for (String path : notAccessible) {
            try {
                testAcMgr.getPrivileges(path, getPrincipals(adminSession));
                fail("AccessControlManager#getPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }

        for (String path : notAccessible) {
            try {
                testAcMgr.getPrivileges(path, Collections.singleton(testPrincipal));
                fail("AccessControlManager#getPrivileges for node that is not accessible should fail.");
            } catch (PathNotFoundException e) {
                // success
            }
        }

    }

    @Test
    public void testGetPrivileges() throws Exception {
        setupPolicy(testPath);
        root.commit();

        testRoot.refresh();

        Set<Principal> testPrincipals = getPrincipals(testRoot.getContentSession());

        assertArrayEquals(new Privilege[0], testAcMgr.getPrivileges(null));
        assertArrayEquals(new Privilege[0], testAcMgr.getPrivileges(null, testPrincipals));

        Privilege[] privs = testAcMgr.getPrivileges(testPath);
        assertEquals(Set.of(testPrivileges), Set.of(privs));

        privs = testAcMgr.getPrivileges(testPath, testPrincipals);
        assertEquals(Set.of(testPrivileges), Set.of(privs));

        // but for 'admin' the test-session doesn't have sufficient privileges
        try {
            testAcMgr.getPrivileges(testPath, getPrincipals(adminSession));
            fail("testSession doesn't have sufficient permission to read access control information at testPath");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        setupPolicy(testPath);
        root.commit();

        testRoot.refresh();
        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            // testRoot can't read access control content -> doesn't see
            // the existing policies and creates a new applicable policy.
            AccessControlPolicy[] applicable = testAcMgr.getApplicablePolicies(principal);
            assertPolicies(applicable, 1);
            assertTrue(applicable[0] instanceof ACL);
        }
    }



    @Test
    public void testGetPolicies() throws Exception {
        setupPolicy(testPath);
        root.commit();

        testRoot.refresh();
        PrincipalManager testPrincipalMgr = getPrincipalManager(testRoot);

        List<Principal> principals = ImmutableList.of(testPrincipal, EveryonePrincipal.getInstance());
        for (Principal principal : principals) {
            if (testPrincipalMgr.hasPrincipal(principal.getName())) {
                // testRoot can't read access control content -> doesn't see
                // the existing policies and creates a new applicable policy.
                AccessControlPolicy[] policies = testAcMgr.getPolicies(principal);
                assertPolicies(policies, 0);
            } else {
                // testRoot can't read principal -> no policies for that principal
                assertPolicies(testAcMgr.getPolicies(principal), 0);
            }
        }
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testGetEffectivePolicies() throws Exception {
        // grant 'testUser' READ + READ_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testPath, privileges);
        root.commit();

        testRoot.refresh();
        assertTrue(testAcMgr.hasPrivileges(testPath, privileges));

        // diff to jr core: getEffectivePolicies will just return the policies
        // accessible for the editing session but not throw an exception.
        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(testPath);
        assertPolicies(effective, 1);
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testGetEffectivePolicies2() throws Exception {
        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ));
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        testRoot.refresh();
        assertTrue(testAcMgr.hasPrivileges(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));

        // diff to jr core: getEffectivePolicies will just return the policies
        // accessible for the editing session but not throw an exception.
        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(childPath);
        assertPolicies(effective, 1);
    }

    @Test
    public void testGetEffectivePoliciesWithoutPrivilege() throws Exception {
        // grant 'testUser' READ + READ_AC privileges at 'path'
        Privilege[] privileges = privilegesFromNames(PrivilegeConstants.JCR_READ);
        setupPolicy(testPath, privileges);
        root.commit();

        testRoot.refresh();

        List<String> paths = ImmutableList.of(testPath, NodeTypeConstants.NODE_TYPES_PATH);
        for (String path : paths) {
            assertFalse(testAcMgr.hasPrivileges(path, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));
            try {
                testAcMgr.getEffectivePolicies(path);
                fail("READ_ACCESS_CONTROL is not granted at " + path);
            } catch (AccessDeniedException e) {
                // success
            }
        }
    }

    @Test
    public void testGetEffectivePoliciesByPrincipal() throws Exception {
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testPath, privs);
        setupPolicy(childPath, privs);
        root.commit();

        testRoot.refresh();
        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertPolicies(effective, 2);
    }

    /**
     * @since OAK 1.0 Policy at testPath not accessible -> getEffectivePolicies
     * only returns the readable policy but doesn't fail.
     */
    @Test
    public void testGetEffectivePoliciesByPrincipal2() throws Exception {
        // policy at testPath: ac content was visible but the policy can't be
        // retrieved from AcMgr as the accesscontrolled node is not visible.
        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        // policy at childPath: will be found by the getEffectivePolicies
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        testRoot.refresh();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertPolicies(effective, 1);
    }

    /**
     * @since OAK 1.0 Policy at testPath not accessible -> getEffectivePolicies
     * only returns the readable policy but doesn't fail.
     */
    @Test
    public void testGetEffectivePoliciesByPrincipal3() throws Exception {
        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ));
        setupPolicy(childPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        testRoot.refresh();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        assertPolicies(effective, 1);
    }

    @Test
    public void testGetEffectivePoliciesByPrincipals() throws Exception {
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testPath, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = TestUtility.getApplicablePolicy(acMgr, childPath);
        acl.addEntry(EveryonePrincipal.getInstance(), privs, true);
        acMgr.setPolicy(childPath, acl);

        root.commit();

        testRoot.refresh();
        Set<Principal> principals = Set.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        assertPolicies(policies, 2);
    }

    /**
     * @since OAK 1.0 : only accessible policies are returned but not exception
     * is raised.
     */
    @Test
    public void testGetEffectivePoliciesByPrincipals2() throws Exception {
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);

        // create policy on testPath -> but deny access to test session
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = TestUtility.getApplicablePolicy(acMgr, testPath);
        acl.addEntry(testPrincipal, privs, false);
        acMgr.setPolicy(testPath, acl);

        // grant access at childpath
        setupPolicy(childPath, privs);
        root.commit();

        testRoot.refresh();

        Set<Principal> principals = Set.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        assertPolicies(policies, 1);
    }

    @Test
    public void testGetEffectivePoliciesByPrincipalsReadPolicy1() throws Exception {
        // grant test-principal READ access at root node (but not READ_ACCESS_CONTROL)
        setupPolicy(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_READ));
        root.commit();

        testRoot.refresh();

        // effective policies must NOT include ReadPolicy 
        Set<Principal> principals = Set.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        assertPolicies(policies, 0, false);
    }

    @Test
    public void testGetEffectivePoliciesByPrincipalsReadPolicy2() throws Exception {
        // grant test-principal READ_ACCESS_CONTROL access at root node (but not READ)
        setupPolicy(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        testRoot.refresh();

        // effective policies must include ReadPolicy 
        // but no path-not-found must be raised if the readable-path is not accessible
        Set<Principal> principals = Set.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        // since no other ac-setup exists for 'everyone' principal -> only ReadPolicy is found
        assertPolicies(policies, 1, true);
    }
    
    @Test
    public void testGetEffectivePoliciesByPrincipalsIncludesReadPolicy3() throws Exception {
        // grant test-principal READ and READ_ACCESS_CONTROL at root node 
        setupPolicy(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        root.commit();

        testRoot.refresh();

        // effective policies must include ReadPolicy 
        Set<Principal> principals = Set.of(testPrincipal, EveryonePrincipal.getInstance());
        AccessControlPolicy[] policies = testAcMgr.getEffectivePolicies(principals);
        // since no other ac-setup exists for 'everyone' principal -> only ReadPolicy is found
        assertPolicies(policies, 1, true);
    }
}
