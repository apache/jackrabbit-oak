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
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the {@link org.apache.jackrabbit.oak.security.authorization.composite.CompositePermissionProvider}
 * where permissions are granted but not all permissions/privileges have been
 * covered by the call. This might be the case when combining different
 * providers that don't cover the full set of permissions/privileges.
 *
 * For simplicity the test only lists a single provider which only supports a
 * limited set of permissions|privileges.
 *
 * The expected result is:
 * - testing for the supported privileges|permissions must reveal that it is granted
 * - any other combination of permissions|privileges must be denied.
 */
public class CompositeProviderCoverageTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cpp;
    private CompositePermissionProvider cppO;

    @Override
    public void before() throws Exception {
        super.before();

        cpp = createPermissionProvider();
        cppO = createPermissionProviderOR();
    }

    @Override
    AggregatedPermissionProvider getTestPermissionProvider() {
        return new LimitCoverageProvider(root);
    }

    @Override
    List<AggregatedPermissionProvider> getAggregatedProviders(@Nonnull String workspaceName, @Nonnull AuthorizationConfiguration config, @Nonnull Set<Principal> principals) {
        return ImmutableList.of(getTestPermissionProvider());
    }

    @Override
    @Test
    public void testGetTreePermissionInstance() throws Exception {
        PermissionProvider pp = createPermissionProvider(EveryonePrincipal.getInstance());
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertTrue(tp instanceof LimitedTreePermission);
            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testGetTreePermissionInstanceOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR(EveryonePrincipal.getInstance());
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertTrue(tp instanceof LimitedTreePermission);
            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testTreePermissionGetChild() throws Exception {
        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");

        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = createPermissionProvider().getTreePermission(rootTree, TreePermission.EMPTY);

        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);
            assertTrue(tp instanceof LimitedTreePermission);
        }
    }

    @Override
    @Test
    public void testTreePermissionGetChildOR() throws Exception {
        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");

        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = createPermissionProviderOR().getTreePermission(rootTree, TreePermission.EMPTY);

        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);
            assertTrue(tp instanceof LimitedTreePermission);
        }
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : NODE_PATHS) {
            assertEquals(ImmutableSet.of(REP_READ_NODES), cpp.getPrivileges(readOnlyRoot.getTree(p)));
            assertEquals(ImmutableSet.of(REP_READ_NODES), cppO.getPrivileges(readOnlyRoot.getTree(p)));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        assertEquals(ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT), cpp.getPrivileges(null));
        assertEquals(ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT), cppO.getPrivileges(null));
    }

    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(cpp.hasPrivileges(tree, REP_READ_NODES));
            assertFalse(cpp.hasPrivileges(tree, JCR_READ));
            assertFalse(cpp.hasPrivileges(tree, JCR_WRITE));
            assertFalse(cpp.hasPrivileges(tree, JCR_ALL));

            assertTrue(cppO.hasPrivileges(tree, REP_READ_NODES));
            assertFalse(cppO.hasPrivileges(tree, JCR_READ));
            assertFalse(cppO.hasPrivileges(tree, JCR_WRITE));
            assertFalse(cppO.hasPrivileges(tree, JCR_ALL));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertTrue(cpp.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));
        assertFalse(cpp.hasPrivileges(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(cpp.hasPrivileges(null, JCR_ALL));

        assertTrue(cppO.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));
        assertFalse(cppO.hasPrivileges(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(cppO.hasPrivileges(null, JCR_ALL));
    }


    @Test
    public void testIsGranted() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(cpp.isGranted(tree, null, Permissions.READ_NODE));
            assertFalse(cpp.isGranted(tree, null, Permissions.LOCK_MANAGEMENT));
            assertFalse(cpp.isGranted(tree, null, Permissions.ALL));
            assertFalse(cpp.isGranted(tree, null, Permissions.READ_NODE | Permissions.LOCK_MANAGEMENT));

            assertTrue(cppO.isGranted(tree, null, Permissions.READ_NODE));
            assertFalse(cppO.isGranted(tree, null, Permissions.LOCK_MANAGEMENT));
            assertFalse(cppO.isGranted(tree, null, Permissions.ALL));
            assertFalse(cppO.isGranted(tree, null, Permissions.READ_NODE | Permissions.LOCK_MANAGEMENT));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(cpp.isGranted(tree, PROPERTY_STATE, Permissions.READ_NODE));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.READ_PROPERTY));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.LOCK_MANAGEMENT));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.ALL));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.READ_NODE | Permissions.LOCK_MANAGEMENT));

            assertTrue(cppO.isGranted(tree, PROPERTY_STATE, Permissions.READ_NODE));
            assertFalse(cppO.isGranted(tree, PROPERTY_STATE, Permissions.READ_PROPERTY));
            assertFalse(cppO.isGranted(tree, PROPERTY_STATE, Permissions.LOCK_MANAGEMENT));
            assertFalse(cppO.isGranted(tree, PROPERTY_STATE, Permissions.ALL));
            assertFalse(cppO.isGranted(tree, PROPERTY_STATE, Permissions.READ_NODE | Permissions.LOCK_MANAGEMENT));
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String nodePath : NODE_PATHS) {
            String propPath = PathUtils.concat(nodePath, JcrConstants.JCR_PRIMARYTYPE);

            assertTrue(cpp.isGranted(nodePath, Session.ACTION_READ));
            assertFalse(cpp.isGranted(propPath, Session.ACTION_READ));
            assertFalse(cpp.isGranted(nodePath, Session.ACTION_REMOVE));
            assertFalse(cpp.isGranted(propPath, JackrabbitSession.ACTION_MODIFY_PROPERTY));
            assertFalse(cpp.isGranted(nodePath, getActionString(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, JackrabbitSession.ACTION_READ_ACCESS_CONTROL)));

            assertTrue(cppO.isGranted(nodePath, Session.ACTION_READ));
            assertFalse(cppO.isGranted(propPath, Session.ACTION_READ));
            assertFalse(cppO.isGranted(nodePath, Session.ACTION_REMOVE));
            assertFalse(cppO.isGranted(propPath, JackrabbitSession.ACTION_MODIFY_PROPERTY));
            assertFalse(cppO.isGranted(nodePath, getActionString(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, JackrabbitSession.ACTION_READ_ACCESS_CONTROL)));

            String nonExisting = PathUtils.concat(nodePath, "nonExisting");
            assertFalse(cpp.isGranted(nonExisting, Session.ACTION_READ));
            assertFalse(cpp.isGranted(nonExisting, JackrabbitSession.ACTION_ADD_PROPERTY));
            assertFalse(cpp.isGranted(nonExisting, Session.ACTION_ADD_NODE));

            assertFalse(cppO.isGranted(nonExisting, Session.ACTION_READ));
            assertFalse(cppO.isGranted(nonExisting, JackrabbitSession.ACTION_ADD_PROPERTY));
            assertFalse(cppO.isGranted(nonExisting, Session.ACTION_ADD_NODE));
        }
    }

    @Test
    public void testRepositoryPermissionsIsGranted() throws Exception {
        RepositoryPermission rp = cpp.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.ALL));

        RepositoryPermission rpO = cppO.getRepositoryPermission();
        assertTrue(rpO.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(rpO.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rpO.isGranted(Permissions.ALL));
    }

    @Test
    public void testTreePermissionIsGranted() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            TreePermission tp = cpp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);

            assertTrue(tp.isGranted(Permissions.READ_NODE));
            assertFalse(tp.isGranted(Permissions.REMOVE_NODE));
            assertFalse(tp.isGranted(Permissions.READ));
            assertFalse(tp.isGranted(Permissions.ALL));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            TreePermission tp = cppO.getTreePermission(readOnlyRoot.getTree(path), parentPermission);

            assertTrue(tp.isGranted(Permissions.READ_NODE));
            assertFalse(tp.isGranted(Permissions.REMOVE_NODE));
            assertFalse(tp.isGranted(Permissions.READ));
            assertFalse(tp.isGranted(Permissions.ALL));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedProperty() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cpp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);

            assertFalse(tp.isGranted(Permissions.READ_PROPERTY, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.REMOVE_PROPERTY, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.READ, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.ALL, PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedPropertyOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppO.getTreePermission(readOnlyRoot.getTree(path), parentPermission);

            assertFalse(tp.isGranted(Permissions.READ_PROPERTY, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.REMOVE_PROPERTY, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.READ, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.ALL, PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanRead() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cpp.getTreePermission(t, parentPermission);

            assertTrue(tp.canRead());

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cppO.getTreePermission(t, parentPermission);

            assertTrue(tp.canRead());

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadProperty() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cpp.getTreePermission(t, parentPermission);
            assertFalse(tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadPropertyOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cppO.getTreePermission(t, parentPermission);
            assertFalse(tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    private final class LimitCoverageProvider extends AbstractAggrProvider {

        LimitCoverageProvider(Root root) {
            super(root);
        }

        @Nonnull
        @Override
        public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            PrivilegeBits supported = PrivilegeBits.getInstance(
                    PrivilegeBits.BUILT_IN.get(JCR_NAMESPACE_MANAGEMENT),
                    PrivilegeBits.BUILT_IN.get(REP_READ_NODES));
            if (privilegeBits != null) {
                return PrivilegeBits.getInstance(privilegeBits).retain(supported);
            } else {
                return supported;
            }
        }

        @Override
        public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
            if (tree == null) {
                return permissions & Permissions.NAMESPACE_MANAGEMENT;
            } else {
                return permissions & Permissions.READ_NODE;
            }
        }

        @Override
        public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
            return permissions & Permissions.READ_NODE;
        }

        @Override
        public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
            return permissions & Permissions.READ_NODE;
        }

        @Override
        public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
            return permissions == Permissions.READ_NODE;
        }

        @Nonnull
        @Override
        public Set<String> getPrivileges(@Nullable Tree tree) {
            return (tree == null) ? ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT) : ImmutableSet.of(REP_READ_NODES);
        }

        @Override
        public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
            return true;
        }

        @Nonnull
        @Override
        public RepositoryPermission getRepositoryPermission() {
            return new RepositoryPermission() {
                @Override
                public boolean isGranted(long repositoryPermissions) {
                    return Permissions.NAMESPACE_MANAGEMENT == repositoryPermissions;
                }
            };
        }

        @Nonnull
        @Override
        public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
            return new LimitedTreePermission();
        }

        @Override
        public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
            return permissions == Permissions.READ_NODE;
        }

        @Override
        public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
            return true;
        }
    }

    private static final class LimitedTreePermission implements TreePermission {
        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return this;
        }

        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return false;
        }

        @Override
        public boolean canReadAll() {
            return false;
        }

        @Override
        public boolean canReadProperties() {
            return false;
        }

        @Override
        public boolean isGranted(long permissions) {
            return Permissions.READ_NODE == permissions;
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return false;
        }
    }
}