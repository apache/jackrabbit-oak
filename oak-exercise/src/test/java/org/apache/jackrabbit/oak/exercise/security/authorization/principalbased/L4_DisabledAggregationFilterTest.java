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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.ContentSession;
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

import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_ADD_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.junit.Assert.assertEquals;

/**
 * <pre>
 * Module: Principal-Based Authorization
 * =============================================================================
 *
 * Title: Effect of AggregationFilter
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * All previous exercises had the AggregationFilter enabled. The following test will illustrate the behavior of the
 * composite permission provider, if the filter was disabled.
 *
 * Reading:
 * https://jackrabbit.apache.org/oak/docs/security/authorization/composite.html#AggregationFilter
 * http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#details_aggregationfilter
 *
 * Exercises:
 *
 * - {@link #testPermissions()}: complete the assertions to make the test case pass and compare the results with the
 *                               previous lession. Explain the difference and discuss the benefits of enabling the filter.
 *
 * - {@link #testEffectivePolicies()}: fix the assertion and explain the result (compared to previous exercises)
 *   Question: How do the effective policies explain the outcome of the permission evaluation?
 *
 * Advanced Exercise:
 * -----------------------------------------------------------------------------
 *
 * - Change the {@link #getCompositionType()} to OR and observe how it changes the effective permissions.
 *   Explain your findings.
 *
 * </pre>
 */
public class L4_DisabledAggregationFilterTest extends AbstractPrincipalBasedTest {

    private Principal supportedPrincipal1;
    private Principal supportedPrincipal2;

    private String testPath;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        supportedPrincipal1 = getSystemUserPrincipal("systemUser1", getSupportedIntermediatePath());
        supportedPrincipal2 = getSystemUserPrincipal("systemUser2", getSupportedIntermediatePath());

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

    @Override
    boolean enableAggregationFilter() {
        return false;
    }

    private void setupAccessControl() throws Exception {
        JackrabbitAccessControlManager compositeAcMgr = (JackrabbitAccessControlManager) getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper());

        PrincipalAccessControlList pacl = checkNotNull(getApplicablePrincipalAccessControlList(compositeAcMgr, supportedPrincipal1));
        pacl.addEntry(testPath, privilegesFromNames(REP_READ_NODES));
        compositeAcMgr.setPolicy(pacl.getPath(), pacl);

        pacl = checkNotNull(getApplicablePrincipalAccessControlList(compositeAcMgr, supportedPrincipal2));
        pacl.addEntry(testPath, privilegesFromNames(REP_READ_PROPERTIES, REP_ADD_PROPERTIES));
        compositeAcMgr.setPolicy(pacl.getPath(), pacl);

        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(compositeAcMgr, testPath);
        acl.addAccessControlEntry(supportedPrincipal1, privilegesFromNames(JCR_WRITE));
        acl.addAccessControlEntry(supportedPrincipal2, privilegesFromNames(JCR_READ_ACCESS_CONTROL));
        compositeAcMgr.setPolicy(acl.getPath(), acl);
    }

    @NotNull
    private PermissionProvider getPermissionProvider(@NotNull ContentSession cs) {
        return getConfig(AuthorizationConfiguration.class).getPermissionProvider(cs.getLatestRoot(), cs.getWorkspaceName(), cs.getAuthInfo().getPrincipals());
    }

    @Test
    public void testPermissions() throws Exception {
        try (ContentSession cs = getTestSession(supportedPrincipal1, supportedPrincipal2)) {
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

            assertEquals(ImmutableSet.of(/*EXERCISE */), pp.getPrivileges(t));
        }
    }

    @Test
    public void testEffectivePolicies() throws Exception {
        JackrabbitAccessControlManager compositeAcMgr = (JackrabbitAccessControlManager) getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper());
        AccessControlPolicy[] effectivePolicies = compositeAcMgr.getEffectivePolicies(ImmutableSet.of(supportedPrincipal1, supportedPrincipal2));

        // EXERCISE: inspect the effective policies and explain the result
        assertEquals(-1 /*EXERCISE*/, effectivePolicies.length);
    }
}