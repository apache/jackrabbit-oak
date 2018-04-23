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
package org.apache.jackrabbit.oak.exercise.security.authorization.advanced;

import javax.jcr.GuestCredentials;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrincipalSetPolicy;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: Advanced Authorization Topics
 * =============================================================================
 *
 * Title: Aggregating Multiple Authorization Models : How It Works
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the inner working of aggregated authorization by stepping through
 * access control management and permission evaluation.
 * Note that this exercise uses a non-OSGi setup for training purpose relying on
 * helpers available only for test setup.
 *
 * Exercises:
 * Debug through each of the test methods in order to get an understand on how
 * the composite setup works.
 *
 * - {@link #testTestUserReadPermissions()}
 * - {@link #testTestUserWritePermissions()}
 * - {@link #testGuestReadPermissions()}
 * - {@link #testGuestWritePermissions()}
 * - {@link #testAdminReadPermissions()}
 * - {@link #testAdminWritePermissions()}
 * - {@link #testEffectivePolicies()}
 * - {@link #testApplicablePolicies()}
 * - {@link #testGetPolicies()}
 * - {@link #testRemovePolicy()}
 *
 * - Explain the {@link org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.PolicyOwner} interface.
 *
 * - Write more tests to explore aggregation of additional features within the
 *   authorization setup such as e.g.
 *   > Privilege discovery
 *   > Repository level privileges such as e.g. ability to register a new node type.
 *   > XML Import
 *   > Validation
 *   > Repository Initialization
 *
 *
 * Advanced Exercises
 * -----------------------------------------------------------------------------
 *
 * - Change the 'authorizationCompositionType' to 'OR' and discuss the expected effect
 *   on the tests. Re-run the tests to verify your expectations.
 *
 * - Take another look at {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider}.
 *   What happens if a given {@link AuthorizationConfiguration} exposes a simple
 *   {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider}
 *   that doesn't implement the aggregation-extension?
 *   To verify your findings write a test-setup that combines the default authorization model with
 *   {@link org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined.PredefinedAuthorizationConfiguration}
 *
 * </pre>
 */
public class L3_UnderstandAggregationTest extends AbstractSecurityTest {

    private PropertyState prop;

    private AccessControlManager acMgr;

    @Override
    public void before() throws Exception {
        super.before();

        prop = PropertyStates.createProperty("prop", "value");

        Tree var = TreeUtil.addChild(root.getTree("/"), "var", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        var.setProperty(prop);

        Tree content = TreeUtil.addChild(root.getTree("/"), "content", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        content.setProperty(prop);

        Tree c1 = TreeUtil.addChild(content, "c1", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        c1.setProperty(prop);

        Tree c2 = TreeUtil.addChild(content, "c2", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        c2.setProperty(prop);

        acMgr = getAccessControlManager(root);

        // at /content grant read-access for everyone using default model
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, content.getPath());
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(content.getPath(), acl);

        // at /content/c1 deny reading properties (default model) and establish
        // a CUG that limits access to test user
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(c1.getPath());
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof PrincipalSetPolicy) {
                PrincipalSetPolicy psp = (PrincipalSetPolicy) policy;
                psp.addPrincipals(getTestUser().getPrincipal());
                acMgr.setPolicy(c1.getPath(), psp);
            } else if (policy instanceof JackrabbitAccessControlList) {
                JackrabbitAccessControlList jacl = (JackrabbitAccessControlList) policy;
                jacl.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(PrivilegeConstants.REP_ADD_PROPERTIES), true);
                acMgr.setPolicy(c1.getPath(), jacl);
            }
        }
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            root.getTree("/content").remove();
            root.getTree("/var").remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected SecurityProvider initSecurityProvider() {
        SecurityProvider sp = super.initSecurityProvider();
        CugConfiguration cugConfiguration = new CugConfiguration();
        cugConfiguration.setParameters(ConfigurationParameters.of("cugSupportedPaths", new String[] {"/content"}, "cugEnabled", true));

        SecurityProviderHelper.updateConfig(sp, cugConfiguration, AuthorizationConfiguration.class);
        return sp;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of("authorizationCompositionType", CompositeAuthorizationConfiguration.CompositionType.AND.toString());
    }

    @Test
    public void testTestUserReadPermissions() throws Exception {
        try (ContentSession cs = createTestSession()) {
            Root r = cs.getLatestRoot();

            assertFalse(r.getTree("/").exists());
            assertFalse(r.getTree("/var").exists());

            Tree t = r.getTree("/content");
            assertTrue(t.exists());
            assertTrue(t.hasProperty(prop.getName()));

            t = r.getTree("/content/c2");
            assertTrue(t.exists());
            assertTrue(t.hasProperty(prop.getName()));

            t = r.getTree("/content/c1");
            assertTrue(t.exists());
            assertTrue(t.hasProperty(prop.getName()));
        }
    }

    @Test
    public void testTestUserWritePermissions() throws Exception {
        try (ContentSession cs = createTestSession()) {
            Root r = cs.getLatestRoot();

            Tree t = r.getTree("/content/c1");
            t.setProperty("addingProperty", "value");
            root.commit();

            assertFalse(getAccessControlManager(r).hasPrivileges("/content/c1", privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES)));
            assertFalse(getAccessControlManager(r).hasPrivileges("/content/c1", privilegesFromNames(PrivilegeConstants.JCR_REMOVE_CHILD_NODES)));
            assertFalse(getAccessControlManager(r).hasPrivileges("/content/c1", privilegesFromNames(PrivilegeConstants.REP_REMOVE_PROPERTIES)));
            assertFalse(getAccessControlManager(r).hasPrivileges("/content/c1", privilegesFromNames(PrivilegeConstants.REP_ALTER_PROPERTIES)));
        }
    }

    @Test
    public void testGuestReadPermissions() throws Exception {
        try (ContentSession cs = login(new GuestCredentials())) {
            Root r = cs.getLatestRoot();

            assertFalse(r.getTree("/").exists());
            assertFalse(r.getTree("/var").exists());

            Tree t = r.getTree("/content");
            assertTrue(t.exists());
            assertTrue(t.hasProperty(prop.getName()));

            t = r.getTree("/content/c2");
            assertTrue(t.exists());
            assertTrue(t.hasProperty(prop.getName()));

            assertFalse(r.getTree("/content/c1").exists());
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testGuestWritePermissions() throws Exception {
        try (ContentSession cs = login(new GuestCredentials())) {
            Root r = cs.getLatestRoot();

            // EXERCISE: write additional code testing permissions required for
            //           another property and child node.

            Tree t = r.getTree("/content");
            t.setProperty("prop2", "value");
            t.addChild("anotherChild");

            r.commit();
        }
    }

    @Test
    public void testAdminReadPermissions() {
        assertTrue(root.getTree("/").exists());
        assertTrue(root.getTree("/var").exists());

        Tree t = root.getTree("/content");
        assertTrue(t.exists());
        assertTrue(t.hasProperty(prop.getName()));

        t = root.getTree("/content/c2");
        assertTrue(t.exists());
        assertTrue(t.hasProperty(prop.getName()));

        t = root.getTree("/content/c1");
        assertTrue(t.exists());
        assertTrue(t.hasProperty(prop.getName()));
    }

    @Test
    public void testAdminWritePermissions() throws Exception {
        for (String p : new String[] {"/", "/var", "/content", "/content/c1", "/content/c2"}) {
            assertTrue(acMgr.hasPrivileges(p, privilegesFromNames(PrivilegeConstants.JCR_ALL)));
        }
    }

    @Test
    public void testEffectivePolicies() throws Exception {
        // EXERCISE: inspect the effective policies to be discovered at the various paths
        //           explain the result and the differences.

        // EXERCISE: compare list of effective policies to policies return upon AccessControlManager.getPolicies

        AccessControlPolicy[] effective = acMgr.getEffectivePolicies("/content/c1");
        assertEquals(3, effective.length);

        effective = acMgr.getEffectivePolicies("/content/c2");
        assertEquals(1, effective.length);

        effective = acMgr.getEffectivePolicies("/var");
        assertEquals(0, effective.length);

        effective = acMgr.getEffectivePolicies(NamespaceConstants.NAMESPACES_PATH);
        assertEquals(1, effective.length);

        // EXERCISE: try to modify the policies obtained in any of the calls

        // EXERCISE: try to write back the effective policies using AccessControlManager.setPolicy
    }

    @Test
    public void testApplicablePolicies() throws Exception {
        // EXERCISE: observe the type of applicable policies and explain differences
        //           between the 3 paths.
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies("/content/c2");
        int cnt = 0;
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            cnt++;
        }
        assertEquals(2, cnt);

        it = acMgr.getApplicablePolicies("/var");
        cnt = 0;
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            cnt++;
        }
        assertEquals(1, cnt);

        it = acMgr.getApplicablePolicies("/content");
        cnt = 0;
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            cnt++;
        }
        assertEquals(1, cnt);

        // EXERCISE: expand the test by optionally modifying the applicable policies
        //           and writing them back using AccessControlManager.setPolicy

        // EXERCISE: explain the effective permissions resulting from both applying
        //           empty and modified policies.
    }

    @Test
    public void testGetSetPolicies() throws Exception {
        AccessControlPolicy[] policies = acMgr.getPolicies("/content/c1");

        assertEquals(2, policies.length);

        // EXERCISE: observe how the different authorization models claim responsibility for the setPolicy call.
        for (AccessControlPolicy policy : policies) {
            acMgr.setPolicy("/content/c1", policy);
        }

        assertEquals(2, acMgr.getPolicies("/content/c1").length);
    }

    @Test
    public void testRemovePolicy() throws Exception {
        AccessControlPolicy[] policies = acMgr.getPolicies("/content/c1");

        assertEquals(2, policies.length);

        // EXERCISE: observe how the different authorization models claim responsibility for the removal.
        for (AccessControlPolicy policy : policies) {
            acMgr.removePolicy("/content/c1", policy);
        }

        assertEquals(0, acMgr.getPolicies("/content/c1").length);
    }
}