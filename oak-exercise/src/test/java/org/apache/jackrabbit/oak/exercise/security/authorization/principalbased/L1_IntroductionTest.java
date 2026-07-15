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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.PrincipalBasedAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: Principal-Based Authorization
 * =============================================================================
 *
 * Title: Introduction to Principal-Based Authorization
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Recap extensions to JCR access control management defined by Jackrabbit API and get a basic understanding of
 * the additional authorization model 'oak-authorization-principalbased'.
 *
 * Exercises:
 *
 * - Recap extensions to JCR access control management API defined in Jackrabbit API and compare getting/setting
 *   access control policies by path and by principal.
 *
 *   http://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/api/security/JackrabbitAccessControlManager.html
 *   http://jackrabbit.apache.org/oak/docs/security/accesscontrol.html#jackrabbit_api
 *   http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#jackrabbit_api
 *
 *   Question: What does binding a policy to a node means in this context?
 *   Question: Where does a principal-based policy take effect? see also
 *   https://s.apache.org/jcr-2.0-spec/16_Access_Control_Management.html#16.3%20Access%20Control%20Policies
 *   Question: How does the PrincipalAccessControlList allow to specify where a given entry takes effect and how
 *   restrictions could be used to achieve the same effect?
 *
 * - Repository setup with principal-based authorization:
 *   The subsequent exercises will use a default repository setup including principal-based authorization. Get the basics
 *   of the security setup and the configuration options.
 *
 *   - {@link #testAuthorizationConfiguration()}:
 *     Inspect the authorization configuration present with the exercise setup and fix the test case.
 *     Discuss, why the order of the aggregated configurations matters. Can you explain it?
 *
 * - Supported Principals
 *   The principal-based authorization module is designed to apply to a limited set of supported principals:
 *
 *   http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#api_extensions
 *   http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#details_filterprovider
 *
 *   - {@link #testSupportedPrincipals()}
 *     Distribute the different principals to the 2 set of principals (unsupported and supported) and verify that the test passes.
 *     Try out different combinations and explain, which principals are supported and why.
 *
 *     Question: what change in the setup would be needed, if you wished to support other paths?
 *     Question: what change in the setup would be needed, if you wished to support other types of principals?
 *
 * - Access Control Mangement and Permission Evaluation
 *   Before starting with the detailed tests let's take another look at the impact of injecting the principal based
 *   authorization module into the Oak security setup:
 *
 *   - {@link #testAccessControlManager()}: compare the access control manager obtained from the principal-based
 *      authorization configuration with the manager obtained from the complete security setup.
 *
 *      Question: how are the different manager aggregated?
 *      Question: can you explain the order?
 *
 *   Read http://jackrabbit.apache.org/oak/docs/security/authorization/principalbased.html#details_permission_eval and
 *   complete the following 2 tests to get an idea how supported vs unsupported principals impact the permission evaluation.
 *
 *   - {@link #testPermissionProviderSupportedPrincipals()}: create a set of supported principals to make sure
 *     the principal-based module contributes to the evaluation
 *   - {@link #testPermissionProviderUnsupportedPrincipals()}: create a principal set that is not supported.
 *
 *     Question: can you explain the usage of {@link EmptyPermissionProvider}
 *     Question: can you explain why the composite-PermissionProvider looks different for supported vs unsupported principals?
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_AccessControlManagementTest}
 * - {@link L3_PermissionEvaluationTest}
 * - {@link L4_DisabledAggregationFilterTest}
 *
 * </pre>
 */
public class L1_IntroductionTest extends AbstractPrincipalBasedTest {

    @Test
    public void testAuthorizationConfiguration() {
        AuthorizationConfiguration ac = getConfig(AuthorizationConfiguration.class);
        assertTrue(ac instanceof CompositeConfiguration);

        CompositeConfiguration<AuthorizationConfiguration> composite = (CompositeConfiguration<AuthorizationConfiguration>) ac;
        assertEquals(2, composite.getConfigurations().size());

        List<AuthorizationConfiguration> aggregates = composite.getConfigurations();
        // EXERCISE: retrieve the principal-based authorization from the aggregated authorization configurations
        AuthorizationConfiguration principalbased = aggregates.get(0);
        assertTrue(principalbased instanceof PrincipalBasedAuthorizationConfiguration);

        // EXERCISE: retrieve the default authorization from the aggregated authorization configurations
        AuthorizationConfiguration defaultAc = aggregates.get(1);
        assertTrue(defaultAc instanceof AuthorizationConfigurationImpl);
    }

    @Test
    public void testSupportedPrincipals() throws Exception {
        FilterProvider pf = getFilterProvider();
        Filter filter = pf.getFilter(getSecurityProvider(), root, getNamePathMapper());

        // EXERCISE: use different combinations of the 6 principals to build 2 sets: one that is supported and one that is not supported by the filter
        Principal regularUserPrincipal = getRegularUserPrincipal();
        Principal groupPrincipal = getGroupPrincipal();
        Principal systemUser1 = getSystemUserPrincipal("su1", null);
        Principal systemUser2 = getSystemUserPrincipal("su2", getSupportedIntermediatePath());
        Principal systemUser3 = getSystemUserPrincipal("su3", PathUtils.concatRelativePaths(UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH, "testpath"));
        Principal systemUser4 = getSystemUserPrincipal("su4", getSupportedIntermediatePath() + "/testpath");

        Set<Principal> unsupported = Set.of(/* EXERCISE*/);
        Set<Principal> supported = Set.of(/* EXERCISE*/);

        assertFalse(filter.canHandle(unsupported));
        assertTrue(filter.canHandle(supported));
    }

    @Test
    public void testAccessControlManager() throws Exception {
        AccessControlManager acMgr = getPrincipalBasedAuthorizationConfiguration().getAccessControlManager(root, getNamePathMapper());
        assertTrue(acMgr instanceof JackrabbitAccessControlManager);
        assertEquals("EXERCISE", acMgr.getClass().getName());

        AccessControlManager compositeAcMgr = getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper());
        assertTrue(compositeAcMgr instanceof JackrabbitAccessControlManager);

        // EXERCISE: inspect the 'compositeAcMgr'
    }

    @Test
    public void testPermissionProviderSupportedPrincipals() throws Exception {
        Set<Principal> principals = Set.of(/* EXERCISE: fill set with supported principal(s) */);

        PermissionProvider pp = getPrincipalBasedAuthorizationConfiguration().getPermissionProvider(root, adminSession.getWorkspaceName(), principals);
        assertFalse(pp instanceof EmptyPermissionProvider);

        // EXERCISE: inspect the return value and explain it. add an assertion.
        PermissionProvider compositePP = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, root.getContentSession().getWorkspaceName(), principals);
        assertEquals("EXERCISE", compositePP.getClass().getName());
    }

    @Test
    public void testPermissionProviderUnsupportedPrincipals() throws Exception {
        Set<Principal> principals = Set.of(/* EXERCISE: fill set with unsupported principal(s) */);

        PermissionProvider pp = getPrincipalBasedAuthorizationConfiguration().getPermissionProvider(root, root.getContentSession().getWorkspaceName(), principals);
        assertTrue(pp instanceof EmptyPermissionProvider);

        // EXERCISE: inspect the return value and explain it. add an assertion.
        PermissionProvider compositePP = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, root.getContentSession().getWorkspaceName(), principals);
        assertEquals("EXERCISE", compositePP.getClass().getName());
    }
}