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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class for CUG related test that setup the authorization configuration
 * to expose the CUG specific implementations of {@code AccessControlManager}
 * and {@code PermissionProvider}.
 */
public abstract class AbstractCugTest extends AbstractSecurityTest implements CugConstants, NodeTypeConstants {

    static final String SUPPORTED_PATH = "/content";
    static final String SUPPORTED_PATH2 = "/content2";
    static final String SUPPORTED_PATH3 = "/some/content/tree";
    static final String UNSUPPORTED_PATH = "/testNode";
    static final String INVALID_PATH = "/path/to/non/existing/tree";

    static final String[] SUPPORTED_PATHS = {SUPPORTED_PATH, SUPPORTED_PATH2, SUPPORTED_PATH3};

    static final ConfigurationParameters CUG_CONFIG = ConfigurationParameters.of(
            CugConstants.PARAM_CUG_SUPPORTED_PATHS, SUPPORTED_PATHS,
            CugConstants.PARAM_CUG_ENABLED, true);

    static final String TEST_GROUP_ID = "testGroup" + UUID.randomUUID();
    static final String TEST_USER2_ID = "testUser2" + UUID.randomUUID();

    @Override
    public void before() throws Exception {
        super.before();

        /*
         * Create tree structure:
         *
         * + root
         *   + content
         *     + subtree
         *   + content2
         *   + some
         *     + content
         *       + tree
         *   + testNode
         *     + child
         */
        Tree rootNode = root.getTree("/");

        createTrees(rootNode, "content", "subtree");
        createTrees(rootNode, "content2");
        createTrees(rootNode, "some", "content", "tree");
        createTrees(rootNode, "testNode", "child");
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            // revert transient pending changes (that might be invalid)
            root.refresh();

            // remove the test group and second test user
            Authorizable testGroup = getUserManager(root).getAuthorizable(TEST_GROUP_ID);
            if (testGroup != null) {
                testGroup.remove();
            }
            Authorizable testUser2 = getUserManager(root).getAuthorizable(TEST_USER2_ID);
            if (testUser2 != null) {
                testUser2.remove();
            }
            for (String p : new String[] {SUPPORTED_PATH, SUPPORTED_PATH2, Text.getAbsoluteParent(SUPPORTED_PATH3, 0), UNSUPPORTED_PATH}) {
                Tree t = root.getTree(p);
                if (t.exists()) {
                    t.remove();
                }
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = CugSecurityProvider.newTestSecurityProvider(getSecurityConfigParameters());
        }
        return securityProvider;
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, CUG_CONFIG);
    }

    CugPermissionProvider createCugPermissionProvider(@NotNull Set<String> supportedPaths, @NotNull Principal... principals) {
        return new CugPermissionProvider(root, root.getContentSession().getWorkspaceName(), Set.of(principals), supportedPaths, getConfig(AuthorizationConfiguration.class).getContext(), getRootProvider(), getTreeProvider());
    }

    void createTrees(@NotNull Tree tree, @NotNull String... names) throws AccessDeniedException {
        Tree parent = tree;
        for (String n : names) {
            parent = TreeUtil.addChild(parent, n, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        }
    }

    void setupCugsAndAcls(@NotNull String... paths) throws Exception {
        UserManager uMgr = getUserManager(root);
        Principal testGroupPrincipal = getTestGroupPrincipal();

        User testUser2 = uMgr.createUser(TEST_USER2_ID, TEST_USER2_ID);
        ((Group) uMgr.getAuthorizable(testGroupPrincipal)).addMember(testUser2);
        root.commit();

        User testUser = getTestUser();

        // add more child nodes
        Tree n = root.getTree(SUPPORTED_PATH);
        createTrees(n, "a", "b", "c");
        createTrees(n, "aa", "bb", "cc");

        // create cugs
        // - /content/a     : allow testGroup, deny everyone
        // - /content/aa/bb : allow testGroup, deny everyone
        // - /content/a/b/c : allow everyone,  deny testGroup (isolated)
        // - /content2      : allow everyone,  deny testGroup (isolated)
        Map<String, Principal> m = Map.of(
                "/content/a", testGroupPrincipal,
                "/content/aa/bb", testGroupPrincipal,
                "/content/a/b/c", EveryonePrincipal.getInstance(),
                "/content2", EveryonePrincipal.getInstance());
        String[] cugPaths = (paths.length == 0) ? m.keySet().toArray(new String[0]) : paths;
        for (String cugPath : cugPaths) {
            createCug(cugPath, m.get(cugPath));
        }

        // setup regular acl at /content:
        // - testUser  ; allow ; jcr:read
        // - testGroup ; allow ; jcr:read, jcr:write, jcr:readAccessControl
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/content");
        acl.addAccessControlEntry(testUser.getPrincipal(), privilegesFromNames(
                PrivilegeConstants.JCR_READ));
        acl.addAccessControlEntry(testGroupPrincipal, privilegesFromNames(
                PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)
        );
        acMgr.setPolicy("/content", acl);
        root.commit();
    }

    CugExclude getExclude() {
        return new CugExclude.Default();
    }

    void createCug(@NotNull String absPath, @NotNull Principal principal) throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(absPath);
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof CugPolicy) {
                ((CugPolicy) policy).addPrincipals(principal);
                acMgr.setPolicy(absPath, policy);
                return;
            }
        }
        throw new IllegalStateException("Unable to create CUG at " + absPath);
    }

    static void createCug(@NotNull Root root, @NotNull String path, @NotNull String principalName) throws RepositoryException {
        Tree tree = root.getTree(path);
        Validate.checkState(tree.exists());

        TreeUtil.addMixin(tree, MIX_REP_CUG_MIXIN, root.getTree(NODE_TYPES_PATH), null);
        TreeUtil.addChild(tree, REP_CUG_POLICY, NT_REP_CUG_POLICY).setProperty(REP_PRINCIPAL_NAMES, Set.of(principalName), Type.STRINGS);
    }

    Principal getTestGroupPrincipal() throws Exception {
        UserManager uMgr = getUserManager(root);
        Group g = uMgr.getAuthorizable(TEST_GROUP_ID, Group.class);
        if (g == null) {
            g = uMgr.createGroup(TEST_GROUP_ID);
            root.commit();
        }
        return g.getPrincipal();
    }

    ContentSession createTestSession2() throws Exception {
        return login(new SimpleCredentials(TEST_USER2_ID, TEST_USER2_ID.toCharArray()));
    }

    static void assertCugPermission(@NotNull TreePermission tp, boolean isSupportedPath) {
        if (isSupportedPath) {
            assertTrue(tp instanceof CugTreePermission);
        } else {
            assertTrue(tp instanceof EmptyCugTreePermission);
        }
    }

    static void assertNestedCugs(@NotNull Root root, @NotNull RootProvider rootProvider,
                                 @NotNull String cugHoldingPath, boolean hasCugPolicy, @NotNull String... expectedNestedPaths) {
        Root immutableRoot = rootProvider.createReadOnlyRoot(root);

        Tree tree = immutableRoot.getTree(cugHoldingPath);
        if (hasCugPolicy) {
            assertFalse(tree.hasProperty(HIDDEN_NESTED_CUGS));
            tree = tree.getChild(REP_CUG_POLICY);
        }

        assertTrue(tree.exists());

        if (tree.isRoot()) {
            if (expectedNestedPaths.length == 0) {
                assertFalse(tree.hasProperty(HIDDEN_TOP_CUG_CNT));
                assertFalse(tree.hasProperty(HIDDEN_NESTED_CUGS));
            } else {
                assertTrue(tree.hasProperty(HIDDEN_NESTED_CUGS));
                assertEquals(Set.of(expectedNestedPaths), CollectionUtils.toSet(tree.getProperty(HIDDEN_NESTED_CUGS).getValue(Type.PATHS)));

                assertTrue(tree.hasProperty(HIDDEN_TOP_CUG_CNT));
                assertEquals(Long.valueOf(expectedNestedPaths.length), tree.getProperty(HIDDEN_TOP_CUG_CNT).getValue(Type.LONG));
            }
        } else {
            assertFalse(tree.hasProperty(HIDDEN_TOP_CUG_CNT));
        }

        if (expectedNestedPaths.length == 0) {
            assertFalse(tree.hasProperty(HIDDEN_NESTED_CUGS));
        } else {
            assertTrue(tree.hasProperty(HIDDEN_NESTED_CUGS));
            assertEquals(Set.of(expectedNestedPaths), CollectionUtils.toSet(tree.getProperty(HIDDEN_NESTED_CUGS).getValue(Type.PATHS)));
        }
    }

    static TreePermission getTreePermission(@NotNull Root root,
                                            @NotNull String path,
                                            @NotNull PermissionProvider pp) {
        Tree t = root.getTree("/");
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
        for (String segm : PathUtils.elements(path)) {
            t = t.getChild(segm);
            tp = pp.getTreePermission(t, tp);
        }
        return tp;
    }
    
    static void assertEquivalentCugs(@NotNull AccessControlPolicy[] expected, @NotNull AccessControlPolicy[] result) {
        assertEquals(expected.length, result.length);
        for (int i = 0; i<expected.length; i++) {
            assertTrue(expected[i] instanceof CugPolicyImpl);
            assertTrue(result[i] instanceof CugPolicyImpl);
            
            CugPolicyImpl exp = (CugPolicyImpl) expected[i];
            CugPolicyImpl res = (CugPolicyImpl) result[i];
            assertEquals(exp.getPath(), res.getPath());
            assertEquals(exp.getPrincipals(), res.getPrincipals());
        }
    }
}
