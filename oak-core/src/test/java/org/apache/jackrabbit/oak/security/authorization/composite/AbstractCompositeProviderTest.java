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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCompositeProviderTest extends AbstractSecurityTest implements NodeTypeConstants {

    static final String ROOT_PATH = "/";
    static final String TEST_PATH = "/test";
    static final String TEST_CHILD_PATH = "/test/child";
    static final String TEST_A_PATH = "/test/a";
    static final String TEST_A_B_PATH = "/test/a/b";
    static final String TEST_A_B_C_PATH = "/test/a/b/c";
    static final String TEST_A_B2_PATH = "/test/a/b2";

    static final String TEST_PATH_2 = "/test2";

    static final String PROP_NAME = "propName";

    static final List<String> NODE_PATHS = ImmutableList.of("/", TEST_PATH, TEST_PATH_2, TEST_CHILD_PATH, TEST_A_PATH, TEST_A_B_PATH, TEST_A_B_C_PATH, TEST_A_B2_PATH);

    Map<String, Long> defPermissions;
    Map<String, Set<String>> defPrivileges;


    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));

        NodeUtil test = rootNode.addChild("test", NT_OAK_UNSTRUCTURED);
        test.setString(PROP_NAME, "strValue");
        test.addChild("child", NT_OAK_UNSTRUCTURED).setString(PROP_NAME, "strVal");
        NodeUtil a = test.addChild("a", NT_OAK_UNSTRUCTURED);
        a.addChild("b2", NT_OAK_UNSTRUCTURED);
        a.addChild("b", NT_OAK_UNSTRUCTURED).addChild("c", NT_OAK_UNSTRUCTURED).setString(PROP_NAME, "strVal");

        rootNode.addChild("test2", NT_OAK_UNSTRUCTURED);

        AccessControlManager acMgr = getAccessControlManager(root);
        Principal everyone = EveryonePrincipal.getInstance();

        allow(acMgr, everyone, null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT);
        allow(acMgr, everyone, TEST_PATH, PrivilegeConstants.JCR_READ);
        allow(acMgr, everyone, TEST_CHILD_PATH, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);

        allow(acMgr, everyone, TEST_A_PATH, PrivilegeConstants.JCR_WRITE);
        deny(acMgr, everyone, TEST_A_B_PATH, PrivilegeConstants.REP_REMOVE_PROPERTIES, PrivilegeConstants.JCR_REMOVE_NODE);
        deny(acMgr, everyone, TEST_A_B_C_PATH, PrivilegeConstants.REP_READ_NODES);

        root.commit();

        defPermissions = ImmutableMap.<String, Long>builder().
                put(TEST_PATH, Permissions.READ).
                put(TEST_CHILD_PATH, Permissions.READ | Permissions.READ_ACCESS_CONTROL).
                put(TEST_A_PATH, Permissions.READ | Permissions.SET_PROPERTY | Permissions.MODIFY_CHILD_NODE_COLLECTION).
                put(TEST_A_B2_PATH, Permissions.READ | Permissions.WRITE | Permissions.MODIFY_CHILD_NODE_COLLECTION).
                put(TEST_A_B_PATH, Permissions.READ | Permissions.ADD_NODE | Permissions.ADD_PROPERTY | Permissions.MODIFY_PROPERTY | Permissions.MODIFY_CHILD_NODE_COLLECTION).
                put(TEST_A_B_C_PATH, Permissions.READ_PROPERTY | Permissions.ADD_NODE | Permissions.ADD_PROPERTY | Permissions.MODIFY_PROPERTY | Permissions.MODIFY_CHILD_NODE_COLLECTION).
                build();
        defPrivileges = ImmutableMap.<String, Set<String>>builder().
                put(ROOT_PATH, ImmutableSet.<String>of()).
                put(TEST_PATH_2, ImmutableSet.<String>of()).
                put(TEST_PATH, ImmutableSet.of(PrivilegeConstants.JCR_READ)).
                put(TEST_CHILD_PATH, ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)).
                put(TEST_A_PATH, ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_WRITE)).
                put(TEST_A_B2_PATH, ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_WRITE)).
                put(TEST_A_B_PATH, ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.REP_ADD_PROPERTIES, PrivilegeConstants.REP_ALTER_PROPERTIES)).
                put(TEST_A_B_C_PATH, ImmutableSet.of(PrivilegeConstants.REP_READ_PROPERTIES, PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.REP_ADD_PROPERTIES, PrivilegeConstants.REP_ALTER_PROPERTIES)).
                build();
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree(TEST_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private static void allow(@Nonnull AccessControlManager acMgr,
                      @Nonnull Principal principal,
                      @Nullable String path,
                      @Nonnull String... privilegeNames) throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), true);
        acMgr.setPolicy(acl.getPath(), acl);
    }

    private static void deny(@Nonnull AccessControlManager acMgr,
                     @Nonnull Principal principal,
                     @Nullable String path,
                     @Nonnull String... privilegeNames) throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), false);
        acMgr.setPolicy(acl.getPath(), acl);
    }

    abstract AggregatedPermissionProvider getTestPermissionProvider();

    boolean reverseOrder() {
        return false;
    }

    private List<AggregatedPermissionProvider> getAggregatedProviders(@Nonnull String workspaceName,
                                                                      @Nonnull AuthorizationConfiguration config,
                                                                      @Nonnull Set<Principal> principals) {
        if (reverseOrder()) {
            return ImmutableList.of(
                    (AggregatedPermissionProvider) config.getPermissionProvider(root, workspaceName, principals),
                    getTestPermissionProvider());
        } else {
            return ImmutableList.of(
                    (AggregatedPermissionProvider) config.getPermissionProvider(root, workspaceName, principals),
                    getTestPermissionProvider());
        }
    }


    CompositePermissionProvider createPermissionProvider(Principal... principals) {
        return createPermissionProvider(ImmutableSet.copyOf(principals));
    }

    CompositePermissionProvider createPermissionProvider(Set<Principal> principals) {
        String workspaceName = root.getContentSession().getWorkspaceName();
        AuthorizationConfiguration config = getConfig(AuthorizationConfiguration.class);
        return new CompositePermissionProvider(root, getAggregatedProviders(workspaceName, config, principals), config.getContext());
    }

    @Test
    public void testHasPrivilegesJcrAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);

            assertFalse(p, pp.hasPrivileges(tree, PrivilegeConstants.JCR_ALL));
        }
    }

    @Test
    public void testHasPrivilegesNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);

            assertTrue(p, pp.hasPrivileges(tree));
        }
    }

    @Test
    public void testHasPrivilegesOnRepoJcrAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        assertFalse(pp.hasPrivileges(null, PrivilegeConstants.JCR_ALL));
    }

    @Test
    public void testHasPrivilegesOnRepoNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        assertTrue(pp.hasPrivileges(null));
    }

    @Test
    public void testIsGrantedAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();

        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.ALL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.ALL));
        }
    }

    @Test
    public void testIsGrantedNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();

        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.NO_PERMISSION));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.NO_PERMISSION));
        }
    }

    @Test
    public void testGetTreePermissionInstance() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        List<String> paths = ImmutableList.of("/", TEST_PATH, TEST_CHILD_PATH, TEST_CHILD_PATH + "/nonexisting");
        for (String path : paths) {
            TreePermission tp = createPermissionProvider().getTreePermission(root.getTree(path), parentPermission);
            assertTrue(tp.getClass().getName().endsWith("CompositeTreePermission"));
            parentPermission = tp;
        }
    }

    @Test
    public void testGetRepositoryPermissionInstance() throws Exception {
        RepositoryPermission rp = createPermissionProvider().getRepositoryPermission();
        assertTrue(rp.getClass().getName().endsWith("CompositeRepositoryPermission"));
    }

    @Test
    public void testRepositoryPermissionIsNotGranted() throws Exception {
        RepositoryPermission rp = createPermissionProvider().getRepositoryPermission();
        assertFalse(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.WORKSPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.ALL));
        assertFalse(rp.isGranted(Permissions.NO_PERMISSION));
    }
}