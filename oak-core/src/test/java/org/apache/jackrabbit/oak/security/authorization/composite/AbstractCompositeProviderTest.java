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
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public abstract class AbstractCompositeProviderTest extends AbstractSecurityTest implements NodeTypeConstants, PrivilegeConstants {

    static final String ROOT_PATH = PathUtils.ROOT_PATH;
    static final String TEST_PATH = "/test";
    static final String TEST_CHILD_PATH = "/test/child";
    static final String TEST_A_PATH = "/test/a";
    static final String TEST_A_B_PATH = "/test/a/b";
    static final String TEST_A_B_C_PATH = "/test/a/b/c";
    static final String TEST_A_B2_PATH = "/test/a/b2";

    static final String TEST_PATH_2 = "/test2";

    static final List<String> NODE_PATHS = ImmutableList.of(ROOT_PATH, TEST_PATH, TEST_PATH_2, TEST_CHILD_PATH, TEST_A_PATH, TEST_A_B_PATH, TEST_A_B_C_PATH, TEST_A_B2_PATH);
    static final List<String> TP_PATHS = ImmutableList.of(ROOT_PATH, TEST_PATH, TEST_A_PATH, TEST_A_B_PATH, TEST_A_B_C_PATH, TEST_A_B_C_PATH + "/nonexisting");

    static final PropertyState PROPERTY_STATE = PropertyStates.createProperty("propName", "val");

    static final String[] ALL_ACTIONS = new String[] {
            Session.ACTION_READ,
            Session.ACTION_ADD_NODE,
            JackrabbitSession.ACTION_REMOVE_NODE,
            Session.ACTION_SET_PROPERTY,
            JackrabbitSession.ACTION_ADD_PROPERTY,
            JackrabbitSession.ACTION_MODIFY_PROPERTY,
            JackrabbitSession.ACTION_REMOVE_PROPERTY,
            Session.ACTION_REMOVE,
            JackrabbitSession.ACTION_READ_ACCESS_CONTROL,
            JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL,
            JackrabbitSession.ACTION_LOCKING,
            JackrabbitSession.ACTION_NODE_TYPE_MANAGEMENT,
            JackrabbitSession.ACTION_VERSIONING,
            JackrabbitSession.ACTION_USER_MANAGEMENT
    };

    Map<String, Long> defPermissions;
    Map<String, Set<String>> defPrivileges;
    Map<String, String[]> defActionsGranted;

    Root readOnlyRoot;

    @Override
    public void before() throws Exception {
        super.before();

        Tree rootNode = root.getTree("/");

        Tree test = TreeUtil.addChild(rootNode, "test", NT_OAK_UNSTRUCTURED);
        TreeUtil.addChild(test, "child", NT_OAK_UNSTRUCTURED);
        Tree a = TreeUtil.addChild(test, "a", NT_OAK_UNSTRUCTURED);
        TreeUtil.addChild(a, "b2", NT_OAK_UNSTRUCTURED);
        Tree b = TreeUtil.addChild(a, "b", NT_OAK_UNSTRUCTURED);
        TreeUtil.addChild(b, "c", NT_OAK_UNSTRUCTURED);

        TreeUtil.addChild(rootNode, "test2", NT_OAK_UNSTRUCTURED);

        AccessControlManager acMgr = getAccessControlManager(root);
        Principal everyone = EveryonePrincipal.getInstance();

        allow(acMgr, everyone, null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT);
        allow(acMgr, everyone, TEST_PATH, JCR_READ);
        allow(acMgr, everyone, TEST_CHILD_PATH, JCR_READ_ACCESS_CONTROL);

        allow(acMgr, everyone, TEST_A_PATH, JCR_WRITE, JCR_VERSION_MANAGEMENT);
        deny(acMgr, everyone, TEST_A_B_PATH, REP_REMOVE_PROPERTIES, JCR_REMOVE_NODE);
        deny(acMgr, everyone, TEST_A_B_C_PATH, REP_READ_NODES);

        root.commit();

        defPermissions = ImmutableMap.<String, Long>builder().
                put(TEST_PATH, Permissions.READ).
                put(TEST_CHILD_PATH,
                        Permissions.READ |
                        Permissions.READ_ACCESS_CONTROL).
                put(TEST_A_PATH,
                        Permissions.READ |
                        Permissions.SET_PROPERTY |
                        Permissions.MODIFY_CHILD_NODE_COLLECTION |
                        Permissions.VERSION_MANAGEMENT).
                put(TEST_A_B2_PATH,
                        Permissions.READ |
                        Permissions.WRITE |
                        Permissions.MODIFY_CHILD_NODE_COLLECTION |
                        Permissions.VERSION_MANAGEMENT).
                put(TEST_A_B_PATH,
                        Permissions.READ |
                        Permissions.ADD_NODE |
                        Permissions.ADD_PROPERTY |
                        Permissions.MODIFY_PROPERTY |
                        Permissions.MODIFY_CHILD_NODE_COLLECTION |
                        Permissions.VERSION_MANAGEMENT).
                put(TEST_A_B_C_PATH,
                        Permissions.READ_PROPERTY |
                        Permissions.ADD_NODE |
                        Permissions.ADD_PROPERTY |
                        Permissions.MODIFY_PROPERTY |
                        Permissions.MODIFY_CHILD_NODE_COLLECTION |
                        Permissions.VERSION_MANAGEMENT).
                build();
        defPrivileges = ImmutableMap.<String, Set<String>>builder().
                put(ROOT_PATH, ImmutableSet.<String>of()).
                put(TEST_PATH_2, ImmutableSet.<String>of()).
                put(TEST_PATH, ImmutableSet.of(JCR_READ)).
                put(TEST_CHILD_PATH, ImmutableSet.of(JCR_READ, JCR_READ_ACCESS_CONTROL)).
                put(TEST_A_PATH, ImmutableSet.of(JCR_READ, JCR_WRITE, JCR_VERSION_MANAGEMENT)).
                put(TEST_A_B2_PATH, ImmutableSet.of(JCR_READ, JCR_WRITE, JCR_VERSION_MANAGEMENT)).
                put(TEST_A_B_PATH, ImmutableSet.of(JCR_READ, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, JCR_VERSION_MANAGEMENT)).
                put(TEST_A_B_C_PATH, ImmutableSet.of(REP_READ_PROPERTIES, JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, JCR_VERSION_MANAGEMENT)).
                build();

        defActionsGranted = ImmutableMap.<String, String[]>builder().
                put(TEST_PATH, new String[] {Session.ACTION_READ}).
                put(TEST_CHILD_PATH, new String[] {Session.ACTION_READ, JackrabbitSession.ACTION_READ_ACCESS_CONTROL}).
                put(TEST_A_PATH, new String[] {Session.ACTION_READ, Session.ACTION_SET_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_PATH + "/jcr:primaryType", new String[] {Session.ACTION_SET_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_PATH + "/propName", new String[] {JackrabbitSession.ACTION_ADD_PROPERTY, JackrabbitSession.ACTION_MODIFY_PROPERTY, JackrabbitSession.ACTION_REMOVE_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_PATH + "/nodeName", new String[] {Session.ACTION_ADD_NODE, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_B2_PATH, new String[] {Session.ACTION_READ, Session.ACTION_ADD_NODE, JackrabbitSession.ACTION_REMOVE_NODE, Session.ACTION_REMOVE, Session.ACTION_SET_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_B_PATH, new String[] {Session.ACTION_READ, Session.ACTION_ADD_NODE, JackrabbitSession.ACTION_ADD_PROPERTY, JackrabbitSession.ACTION_MODIFY_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_B_PATH + "/nonExisting", new String[] {Session.ACTION_READ, Session.ACTION_ADD_NODE, JackrabbitSession.ACTION_ADD_PROPERTY, JackrabbitSession.ACTION_MODIFY_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_B_C_PATH + "/jcr:primaryType",  new String[] {Session.ACTION_READ, JackrabbitSession.ACTION_VERSIONING}).
                put(TEST_A_B_C_PATH,  new String[] {Session.ACTION_ADD_NODE, JackrabbitSession.ACTION_ADD_PROPERTY, JackrabbitSession.ACTION_VERSIONING}).
                build();

        readOnlyRoot = RootFactory.createReadOnlyRoot(root);
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

    @Nonnull
    static String getActionString(@Nonnull String... actions) {
        return Text.implode(actions, ",");
    }

    static void assertCompositeTreePermission(@Nonnull TreePermission tp) {
        assertTrue(tp.getClass()+ "", tp instanceof CompositeTreePermission);
    }

    static void assertCompositeTreePermission(boolean expected, @Nonnull TreePermission tp) {
        assertEquals(expected, tp instanceof CompositeTreePermission);
    }

    abstract AggregatedPermissionProvider getTestPermissionProvider();

    boolean reverseOrder() {
        return false;
    }

    List<AggregatedPermissionProvider> getAggregatedProviders(@Nonnull String workspaceName,
                                                              @Nonnull AuthorizationConfiguration config,
                                                              @Nonnull Set<Principal> principals) {
        ImmutableList<AggregatedPermissionProvider> l = ImmutableList.of(
                    (AggregatedPermissionProvider) config.getPermissionProvider(root, workspaceName, principals),
                    getTestPermissionProvider());
        if (reverseOrder()) {
            return l.reverse();
        } else {
            return l;
        }
    }

    CompositePermissionProvider createPermissionProvider(Principal... principals) {
        return createPermissionProvider(ImmutableSet.copyOf(principals));
    }

    CompositePermissionProvider createPermissionProvider(Set<Principal> principals) {
        String workspaceName = root.getContentSession().getWorkspaceName();
        AuthorizationConfiguration config = getConfig(AuthorizationConfiguration.class);
        return new CompositePermissionProvider(root, getAggregatedProviders(workspaceName, config, principals),
                config.getContext(), CompositionType.AND);
    }

    CompositePermissionProvider createPermissionProviderOR(Principal... principals) {
        return createPermissionProviderOR(ImmutableSet.copyOf(principals));
    }

    CompositePermissionProvider createPermissionProviderOR(Set<Principal> principals) {
        String workspaceName = root.getContentSession().getWorkspaceName();
        AuthorizationConfiguration config = getConfig(AuthorizationConfiguration.class);
        return new CompositePermissionProvider(root, getAggregatedProviders(workspaceName, config, principals),
                config.getContext(), CompositionType.OR);
    }

    @Test
    public void testRefresh() throws Exception {
         createPermissionProvider().refresh();
         createPermissionProviderOR().refresh();
    }

    @Test
    public void testHasPrivilegesJcrAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            assertFalse(p, pp.hasPrivileges(tree, JCR_ALL));
        }
    }

    @Test
    public void testHasPrivilegesJcrAllOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            assertFalse(p, pp.hasPrivileges(tree, JCR_ALL));
        }
    }

    @Test
    public void testHasPrivilegesNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            assertTrue(p, pp.hasPrivileges(tree));
            assertTrue(p, ppo.hasPrivileges(tree));
        }
    }

    @Test
    public void testHasPrivilegesOnRepoJcrAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        assertFalse(pp.hasPrivileges(null, JCR_ALL));
        PermissionProvider ppo = createPermissionProviderOR();
        assertFalse(ppo.hasPrivileges(null, JCR_ALL));
    }

    @Test
    public void testHasPrivilegesOnRepoNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        assertTrue(pp.hasPrivileges(null));
        PermissionProvider ppo = createPermissionProviderOR();
        assertTrue(ppo.hasPrivileges(null));
    }

    @Test
    public void testIsGrantedAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();

        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.ALL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.ALL));
            assertFalse(p, ppo.isGranted(tree, null, Permissions.ALL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), ppo.isGranted(tree, ps, Permissions.ALL));
        }
    }

    @Test
    public void testIsGrantedNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();

        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.NO_PERMISSION));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.NO_PERMISSION));
            assertFalse(p, ppo.isGranted(tree, null, Permissions.NO_PERMISSION));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), ppo.isGranted(tree, ps, Permissions.NO_PERMISSION));
        }
    }

    @Test
    public void testIsNotGranted() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();

        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(p, ppo.isGranted(tree, null, Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), ppo.isGranted(tree, ps, Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    @Test
    public void testIsGrantedActionNone() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();
        String actions = "";

        for (String nodePath : NODE_PATHS) {
            assertFalse(nodePath, pp.isGranted(nodePath, actions));
            assertFalse(nodePath, ppo.isGranted(nodePath, actions));

            String propPath = PathUtils.concat(nodePath, JcrConstants.JCR_PRIMARYTYPE);
            assertFalse(propPath, pp.isGranted(propPath, actions));
            assertFalse(propPath, ppo.isGranted(propPath, actions));

            String nonExPath = PathUtils.concat(nodePath, "nonExisting");
            assertFalse(nonExPath, pp.isGranted(nonExPath, actions));
            assertFalse(nonExPath, ppo.isGranted(nonExPath, actions));
        }
    }

    @Test
    public void testIsNotGrantedAction() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();
        String[] actions = new String[]{JackrabbitSession.ACTION_LOCKING, JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL};

        for (String nodePath : NODE_PATHS) {
            String actionStr = getActionString(actions);
            assertFalse(nodePath, pp.isGranted(nodePath, actionStr));
            assertFalse(nodePath, ppo.isGranted(nodePath, actionStr));

            String propPath = PathUtils.concat(nodePath, JcrConstants.JCR_PRIMARYTYPE);
            assertFalse(propPath, pp.isGranted(propPath, actionStr));
            assertFalse(propPath, ppo.isGranted(propPath, actionStr));

            String nonExPath = PathUtils.concat(nodePath, "nonExisting");
            assertFalse(nonExPath, pp.isGranted(nonExPath, actionStr));
            assertFalse(nonExPath, ppo.isGranted(nonExPath, actionStr));
        }
    }

    @Test
    public void testGetTreePermissionAllParent() throws Exception {
        TreePermission tp = createPermissionProvider().getTreePermission(readOnlyRoot.getTree(TEST_PATH), TreePermission.ALL);
        assertSame(TreePermission.ALL, tp);
        TreePermission tpo = createPermissionProviderOR().getTreePermission(readOnlyRoot.getTree(TEST_PATH), TreePermission.ALL);
        assertSame(TreePermission.ALL, tpo);
    }

    @Test
    public void testGetTreePermissionEmptyParent() throws Exception {
        TreePermission tp = createPermissionProvider().getTreePermission(readOnlyRoot.getTree(TEST_PATH), TreePermission.EMPTY);
        assertSame(TreePermission.EMPTY, tp);
        TreePermission tpo = createPermissionProviderOR().getTreePermission(readOnlyRoot.getTree(TEST_PATH), TreePermission.EMPTY);
        assertSame(TreePermission.EMPTY, tpo);
    }

    @Test
    public void testTreePermissionIsGrantedAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        TreePermission parentPermission = TreePermission.EMPTY;

        PropertyState ps = PropertyStates.createProperty("propName", "val");

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);

            assertFalse(tp.isGranted(Permissions.ALL));
            assertFalse(tp.isGranted(Permissions.ALL, ps));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedAllOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        PropertyState ps = PropertyStates.createProperty("propName", "val");

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);

            assertFalse(tp.isGranted(Permissions.ALL));
            assertFalse(tp.isGranted(Permissions.ALL, ps));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsNotGranted() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        TreePermission parentPermission = TreePermission.EMPTY;

        PropertyState ps = PropertyStates.createProperty("propName", "val");

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);

            assertFalse(tp.isGranted(Permissions.NO_PERMISSION));
            assertFalse(tp.isGranted(Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(tp.isGranted(Permissions.NO_PERMISSION, ps));
            assertFalse(tp.isGranted(Permissions.MODIFY_ACCESS_CONTROL, ps));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsNotGrantedOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        PropertyState ps = PropertyStates.createProperty("propName", "val");

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);

            assertFalse(tp.isGranted(Permissions.NO_PERMISSION));
            assertFalse(tp.isGranted(Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(tp.isGranted(Permissions.NO_PERMISSION, ps));
            assertFalse(tp.isGranted(Permissions.MODIFY_ACCESS_CONTROL, ps));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        TreePermission parentPermission = TreePermission.EMPTY;
        PermissionProvider ppO = createPermissionProviderOR();
        TreePermission parentPermissionO = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertFalse(tp.canReadAll());
            parentPermission = tp;

            TreePermission tpO = ppO.getTreePermission(readOnlyRoot.getTree(path), parentPermissionO);
            assertFalse(tpO.canReadAll());
            parentPermissionO = tpO;
        }
    }

    @Test
    public void testTreePermissionCanReadProperties() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertFalse(tp.canReadProperties());

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadPropertiesOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertFalse(tp.canReadProperties());

            parentPermission = tp;
        }
    }

    @Test
    public void testGetTreePermissionInstance() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertCompositeTreePermission(tp);
            parentPermission = tp;
        }
    }

    @Test
    public void testGetTreePermissionInstanceOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertCompositeTreePermission(tp);
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionGetChild() throws Exception {
        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");

        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = createPermissionProvider().getTreePermission(rootTree, TreePermission.EMPTY);

        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);
            assertCompositeTreePermission(tp);
        }
    }

    @Test
    public void testTreePermissionGetChildOR() throws Exception {
        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");

        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = createPermissionProviderOR().getTreePermission(rootTree, TreePermission.EMPTY);

        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);
            assertCompositeTreePermission(tp);
        }
    }

    @Test
    public void testGetRepositoryPermissionInstance() throws Exception {
        RepositoryPermission rp = createPermissionProvider().getRepositoryPermission();
        assertTrue(rp.getClass().getName().endsWith("CompositeRepositoryPermission"));
        RepositoryPermission rpO = createPermissionProviderOR().getRepositoryPermission();
        assertTrue(rpO.getClass().getName().endsWith("CompositeRepositoryPermission"));
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

    @Test
    public void testRepositoryPermissionIsNotGrantedOR() throws Exception {
        RepositoryPermission rp = createPermissionProviderOR().getRepositoryPermission();
        assertFalse(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.WORKSPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.ALL));
        assertFalse(rp.isGranted(Permissions.NO_PERMISSION));
    }
}