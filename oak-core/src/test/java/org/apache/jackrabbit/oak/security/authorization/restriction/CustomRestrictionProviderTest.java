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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.value.StringValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;

/**
 * Test suite for a custom restriction provider. The restriction is enabled based on the (non) existence of a property.
 * The test creates nodes along '/testRoot/a/b/c/d/e' and sets the 'protect-me' property on '/testRoot/a/b/c'.
 */
public class CustomRestrictionProviderTest extends AbstractSecurityTest {

    private static final String TEST_ROOT_PATH = "/testRoot";
    private static final String TEST_A_PATH = "/testRoot/a";
    private static final String TEST_B_PATH = "/testRoot/a/b";
    private static final String TEST_C_PATH = "/testRoot/a/b/c";
    private static final String TEST_D_PATH = "/testRoot/a/b/c/d";
    private static final String TEST_E_PATH = "/testRoot/a/b/c/d/e";
    private static final String PROP_NAME_PROTECT_ME = "protect-me";

    private Principal testPrincipal;

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        RestrictionProvider rProvider = CompositeRestrictionProvider.newInstance(new PropertyRestrictionProvider(), new RestrictionProviderImpl());
        Map<String, RestrictionProvider> authorizMap = Map.of(AccessControlConstants.PARAM_RESTRICTION_PROVIDER, rProvider);
        return ConfigurationParameters.of(Map.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(authorizMap)));
    }

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        Tree rootNode = root.getTree("/");
        Tree testRootNode = TreeUtil.addChild(rootNode, "testRoot", NT_UNSTRUCTURED);
        Tree a = TreeUtil.addChild(testRootNode, "a", NT_UNSTRUCTURED);
        Tree b = TreeUtil.addChild(a, "b", NT_UNSTRUCTURED);
        Tree c = TreeUtil.addChild(b, "c", NT_UNSTRUCTURED);
        c.setProperty(PROP_NAME_PROTECT_ME, true);
        Tree d = TreeUtil.addChild(c, "d", NT_UNSTRUCTURED);
        TreeUtil.addChild(d, "e", NT_UNSTRUCTURED);
        root.commit();

        testPrincipal = getTestUser().getPrincipal();
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            // revert uncommitted changes
            root.refresh();

            // remove all test content
            root.getTree(TEST_ROOT_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private void addEntry(String path, boolean grant, String restriction, String... privilegeNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        if (restriction.length() > 0) {
            Map<String, Value> rs = new HashMap<>();
            rs.put(PropertyRestrictionProvider.RESTRICTION_NAME, new StringValue(restriction));
            acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), grant, rs);
        } else {
            acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), grant);
        }
        acMgr.setPolicy(path, acl);
        root.commit();
    }

    private void assertIsGranted(PermissionProvider pp, Root root, boolean allow, String path, long permissions) {
        assertEquals("user should " + (allow ? "" : "not ") + "have " + permissions + " on " + path,
                allow, pp.isGranted(root.getTree(path), null, permissions));
    }

    private PermissionProvider getPermissionProvider(ContentSession session) {
        return getSecurityProvider()
                .getConfiguration(AuthorizationConfiguration.class)
                .getPermissionProvider(root, session.getWorkspaceName(), session.getAuthInfo().getPrincipals());
    }

    /**
     * Tests the custom restriction provider that checks on the existence of a property.
     */
    @Test(expected = CommitFailedException.class)
    public void testProtectByRestriction() throws Exception {
        // allow rep:write      /testroot
        // deny  jcr:removeNode /testroot/a  hasProperty=protect-me
        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_A_PATH, false, PROP_NAME_PROTECT_ME, PrivilegeConstants.JCR_REMOVE_NODE);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();
            PermissionProvider pp = getPermissionProvider(testSession);
            assertIsGranted(pp, testRoot, true, TEST_A_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_D_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_E_PATH, Permissions.REMOVE_NODE);

            // should be able to remove /a/b/c/d
            testRoot.getTree(TEST_D_PATH).remove();
            testRoot.commit();

            testRoot.getTree(TEST_C_PATH).remove();
            testRoot.commit();
            //fail("should not be able to delete " + TEST_C_PATH);
        }
    }

    /**
     * Tests the custom restriction provider that checks on the existence of a property.
     */
    @Test
    public void testProtectPropertiesByRestriction() throws Exception {
        // allow rep:write            /testroot
        // deny  jcr:modifyProperties /testroot/a  hasProperty=protect-me
        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_A_PATH, false, PROP_NAME_PROTECT_ME, PrivilegeConstants.JCR_MODIFY_PROPERTIES);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();

            PermissionProvider pp = getPermissionProvider(testSession);
            assertIsGranted(pp, testRoot, true, TEST_A_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, true, TEST_D_PATH, Permissions.MODIFY_PROPERTY);
            assertIsGranted(pp, testRoot, true, TEST_E_PATH, Permissions.MODIFY_PROPERTY);
        }
    }

    /**
     * Tests the custom restriction provider that checks on the absence of a property.
     */
    @Test
    public void testUnProtectByRestriction() throws Exception {
        // allow rep:write      /testroot
        // deny  jcr:removeNode /testroot
        // allow jcr:removeNode /testroot/a  hasProperty=!protect-me
        addEntry(TEST_ROOT_PATH, true, "", PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);
        addEntry(TEST_ROOT_PATH, false, "", PrivilegeConstants.JCR_REMOVE_NODE);
        addEntry(TEST_A_PATH, true, "!" + PROP_NAME_PROTECT_ME, PrivilegeConstants.JCR_REMOVE_NODE);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();
            PermissionProvider pp = getPermissionProvider(testSession);
            assertIsGranted(pp, testRoot, true, TEST_A_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_B_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, false, TEST_C_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_D_PATH, Permissions.REMOVE_NODE);
            assertIsGranted(pp, testRoot, true, TEST_E_PATH, Permissions.REMOVE_NODE);
        }
    }

    /**
     * Customs restriction provider that matches restrictions based on the existence of a property.
     */
    public static class PropertyRestrictionProvider extends AbstractRestrictionProvider {

        static final String RESTRICTION_NAME = "hasProperty";

        PropertyRestrictionProvider() {
            super(supportedRestrictions());
        }

        @NotNull
        private static Map<String, RestrictionDefinition> supportedRestrictions() {
            RestrictionDefinition dates = new RestrictionDefinitionImpl(RESTRICTION_NAME, Type.STRING, false);
            return Collections.singletonMap(dates.getName(), dates);
        }

        //------------------------------------------------< RestrictionProvider >---

        @NotNull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
            if (oakPath != null) {
                PropertyState property = tree.getProperty(RESTRICTION_NAME);
                if (property != null) {
                    return HasPropertyPattern.create(property);
                }
            }
            return RestrictionPattern.EMPTY;
        }

        @NotNull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
            if (oakPath != null) {
                for (Restriction r : restrictions) {
                    String name = r.getDefinition().getName();
                    if (RESTRICTION_NAME.equals(name)) {
                        return HasPropertyPattern.create(r.getProperty());
                    }
                }
            }
            return RestrictionPattern.EMPTY;
        }
    }

    public static class HasPropertyPattern implements RestrictionPattern {

        private final String propertyName;

        private final boolean negate;

        private HasPropertyPattern(@NotNull String propertyName) {
            if (propertyName.startsWith("!")) {
                this.propertyName = propertyName.substring(1);
                negate = true;
            } else {
                this.propertyName = propertyName;
                negate = false;
            }
        }

        static RestrictionPattern create(PropertyState stringProperty) {
            if (stringProperty.count() == 1) {
                return new HasPropertyPattern(stringProperty.getValue(Type.STRING));
            } else {
                return RestrictionPattern.EMPTY;
            }
        }

        @Override
        public boolean matches(@NotNull Tree tree, @Nullable PropertyState property) {
            boolean match = false;

            // configured property name found on underlying jcr:content node has precedence
            if (tree.hasChild(JcrConstants.JCR_CONTENT)) {
                match = tree.getChild(JcrConstants.JCR_CONTENT).hasProperty(propertyName);
            }
            if (!match) {
                match = tree.hasProperty(propertyName);
            }

            return negate != match;
        }

        @Override
        public boolean matches(@NotNull String path) {
            return matches();
        }

        @Override
        public boolean matches() {
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HasPropertyPattern{");
            sb.append("propertyName='").append(propertyName).append('\'');
            sb.append(", negate=").append(negate);
            sb.append('}');
            return sb.toString();
        }
    }
}


