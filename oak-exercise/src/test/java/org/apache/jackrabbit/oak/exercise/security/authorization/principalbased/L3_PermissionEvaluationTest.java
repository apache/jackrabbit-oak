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
package org.apache.jackrabbit.oak.exercise.security.authorization.principalbased;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_ADD_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_REMOVE_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: Principal-Based Authorization
 * =============================================================================
 *
 * Title: Permission Evaluation with Principal-Based Authorization
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn how effective permissions are being evaluation in a composite setup that includes both principal-based and
 * path based authorization.
 *
 * Reading:
 * http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#details_permission_eval
 * http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#details_aggregationfilter
 *
 * Exercises:
 *
 * - {@link #testPermissions()}: defined the list of principals whose effective permission make the test pass
 *   Question: Are the other combinations of principals with the same effect?
 *             Can you explain the test and the permission evaluation for this test case?
 *
 * - {@link #testPermissions2()}: fix the assertions for the given content session and explain the result
 *
 * - {@link #testPermissions3()}: defined the list of principals whose effective permission make the test pass
 * - {@link #testPermissions4()}: defined the list of principals whose effective permission make the test pass
 *   Question: Can you explain the main difference between the two tests?
 *
 * - {@link #testPermissions5()}:
 *
 * - {@link #testReadablePaths()}: fill in a path that with the current setup is always readable.
 *   Question: How many alternative paths would make the test pass?
 *   Question: Discuss what changes to the authorization setup would be needed to have a different set of paths marked as readable.
 *
 * </pre>
 */
public class L3_PermissionEvaluationTest extends AbstractPrincipalBasedTest {

    private Principal systemUserPrincipal1;
    private Principal systemUserPrincipal2;
    private Principal userPrincipal;
    private Principal groupPrincipal;

    private String testPath;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        systemUserPrincipal1 = getSystemUserPrincipal("systemUser1", getSupportedIntermediatePath());
        systemUserPrincipal2 = getSystemUserPrincipal("systemUser2", getSupportedIntermediatePath());
        userPrincipal = getRegularUserPrincipal();
        groupPrincipal = getGroupPrincipal();

        Tree testTree = TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        testTree.setProperty("prop", "value");

        testPath = getNamePathMapper().getJcrPath(testTree.getPath());

        setupAccessControl();
        root.commit();
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            root.getTree(testPath).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private void setupAccessControl() throws Exception {
        JackrabbitAccessControlManager compositeAcMgr = (JackrabbitAccessControlManager) getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper());

        PrincipalAccessControlList pacl = checkNotNull(getApplicablePrincipalAccessControlList(compositeAcMgr, systemUserPrincipal1));
        pacl.addEntry(testPath, privilegesFromNames(REP_READ_NODES));
        compositeAcMgr.setPolicy(pacl.getPath(), pacl);

        pacl = checkNotNull(getApplicablePrincipalAccessControlList(compositeAcMgr, systemUserPrincipal2));
        pacl.addEntry(testPath, privilegesFromNames(REP_READ_PROPERTIES, REP_ADD_PROPERTIES));
        compositeAcMgr.setPolicy(pacl.getPath(), pacl);

        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(compositeAcMgr, testPath);
        acl.addAccessControlEntry(groupPrincipal, privilegesFromNames(JCR_READ));
        acl.addAccessControlEntry(systemUserPrincipal1, privilegesFromNames(JCR_WRITE));
        acl.addAccessControlEntry(systemUserPrincipal2, privilegesFromNames(JCR_READ_ACCESS_CONTROL));
        acl.addEntry(userPrincipal, privilegesFromNames(REP_REMOVE_PROPERTIES), false);
        compositeAcMgr.setPolicy(acl.getPath(), acl);
    }

    @NotNull
    private PermissionProvider getPermissionProvider(@NotNull ContentSession cs) {
        return getConfig(AuthorizationConfiguration.class).getPermissionProvider(cs.getLatestRoot(), cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals());
    }

    @Test
    public void testPermissions() throws Exception {
        try (ContentSession cs = getTestSession(/*EXERCISE: add principals such that the test passes */)) {
            Root r = cs.getLatestRoot();
            assertTrue(r.getTree(testPath).exists());
            assertFalse(r.getTree(testPath).hasProperty("prop"));
        }
    }

    @Test
    public void testPermissions2() throws Exception {
        try (ContentSession cs = getTestSession(systemUserPrincipal1, systemUserPrincipal2)) {
            PermissionProvider pp = getPermissionProvider(cs);

            Tree t = cs.getLatestRoot().getTree(testPath);

            // EXERCISE: fix the assertions and explain the result
            assertEquals(null /*EXERCISE */, t.exists());
            assertEquals(null /*EXERCISE */, t.hasChild(AccessControlConstants.REP_POLICY));
            assertEquals(null /*EXERCISE */, t.hasProperty("prop"));

            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, JCR_READ_ACCESS_CONTROL));
            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, JCR_WRITE));
            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, JCR_MODIFY_PROPERTIES));
            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, REP_ADD_PROPERTIES));
        }
    }

    @Test
    public void testPermissions3() throws Exception {
        try (ContentSession cs = getTestSession(/*EXERCISE: add principals such that the test passes */)) {
            Tree t = cs.getLatestRoot().getTree(testPath);
            assertTrue(t.exists());
            assertTrue(t.hasProperty("prop"));
            assertTrue(t.hasChild(AccessControlConstants.REP_POLICY));
            assertFalse(getPermissionProvider(cs).hasPrivileges(t, JCR_WRITE));
        }
    }

    @Test
    public void testPermissions4() throws Exception {
        try (ContentSession cs = getTestSession(/*EXERCISE: add principals such that the test passes */)) {
            Tree t = cs.getLatestRoot().getTree(testPath);
            assertTrue(t.exists());
            assertTrue(t.hasProperty("prop"));
            assertFalse(t.hasChild(AccessControlConstants.REP_POLICY));

            PermissionProvider pp = getPermissionProvider(cs);
            assertTrue(pp.hasPrivileges(t, REP_ADD_PROPERTIES));
            assertFalse(pp.hasPrivileges(t, REP_REMOVE_PROPERTIES));
        }
    }

    @Test
    public void testPermissions5() throws Exception {
        try (ContentSession cs = getTestSession(systemUserPrincipal1, groupPrincipal)) {
            PermissionProvider pp = getPermissionProvider(cs);

            Tree t = cs.getLatestRoot().getTree(testPath);

            // EXERCISE: fix the assertions and explain the result
            assertEquals(null /*EXERCISE */, t.exists());
            assertEquals(null /*EXERCISE */, t.hasChild(AccessControlConstants.REP_POLICY));
            assertEquals(null /*EXERCISE */, t.hasProperty("prop"));

            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, JCR_READ_ACCESS_CONTROL));
            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, JCR_WRITE));
            assertEquals(null /*EXERCISE */, pp.hasPrivileges(t, REP_REMOVE_PROPERTIES));
        }
    }

    @Test
    public void testReadablePaths() throws Exception {
        try (ContentSession cs = getTestSession(systemUserPrincipal1, systemUserPrincipal2)) {
            String readablePath = null; // EXERCISE: enter a path that is always readable for the given 2 principals
            assertTrue(cs.getLatestRoot().getTree(readablePath).exists());
        }
    }
}
