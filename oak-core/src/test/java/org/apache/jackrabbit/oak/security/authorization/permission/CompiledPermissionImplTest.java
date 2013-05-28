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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.security.Principal;
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.authorization.restriction.RestrictionProviderImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.ALLOW_ALL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.ALLOW_ALL_REGULAR;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.ALLOW_NODES;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.ALLOW_PROPERTIES;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.ALLOW_THIS;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.DENY_ALL_REGULAR;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus.DENY_THIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompiledPermissionImplTest extends AbstractSecurityTest implements PermissionConstants, PrivilegeConstants, AccessControlConstants {

    private Principal userPrincipal;
    private Principal group1;
    private Principal group2;
    private Principal group3;

    private PrivilegeBitsProvider pbp;
    private RestrictionProvider rp;

    private String node1Path = "/nodeName1";
    private String node2Path = node1Path + "/nodeName2";

    private Set<String> allPaths;
    private Set<String> rootAndUsers;
    private Set<String> nodePaths;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        userPrincipal = new PrincipalImpl("test");
        group1 = EveryonePrincipal.getInstance();
        group2 = new GroupImpl("group2");
        group3 = new GroupImpl("group3");

        pbp = new PrivilegeBitsProvider(root);
        rp = new RestrictionProviderImpl();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil system = rootNode.getChild("jcr:system");
        NodeUtil perms = system.addChild(REP_PERMISSION_STORE, NT_REP_PERMISSION_STORE);
        perms.addChild(userPrincipal.getName(), NT_REP_PERMISSION_STORE);
        perms.addChild(group1.getName(), NT_REP_PERMISSION_STORE);
        perms.addChild(group2.getName(), NT_REP_PERMISSION_STORE);
        perms.addChild(group3.getName(), NT_REP_PERMISSION_STORE);
        NodeUtil testNode = rootNode.addChild("nodeName1", NT_UNSTRUCTURED);
        testNode.setString("propName1", "strValue");
        NodeUtil testNode2 = testNode.addChild("nodeName2", NT_UNSTRUCTURED);
        testNode2.setString("propName2", "strValue");
        root.commit();

        allPaths = ImmutableSet.of("/", UserConstants.DEFAULT_USER_PATH, node1Path, node2Path);
        rootAndUsers = ImmutableSet.of("/", UserConstants.DEFAULT_USER_PATH);
        nodePaths = ImmutableSet.of(node1Path, node2Path);
    }

    @Override
    public void after() throws Exception {
        root.getTree(PERMISSIONS_STORE_PATH).remove();
        root.commit();

        super.after();
    }

    @Override
    protected SecurityProvider getSecurityProvider() {
        return new SecurityProviderImpl() {

            @Nonnull
            @Override
            public Iterable<? extends SecurityConfiguration> getConfigurations() {
                List<SecurityConfiguration> configs = new ArrayList<SecurityConfiguration>();
                for (SecurityConfiguration sc : super.getConfigurations()) {
                    if (sc instanceof AccessControlConfiguration) {
                        configs.add(new OpenAccessControlConfiguration());
                    } else {
                        configs.add(sc);
                    }
                }
                return configs;
            }

            @Nonnull
            @Override
            public <T> T getConfiguration(Class<T> configClass) {
                if (AccessControlConfiguration.class == configClass) {
                    return (T) new OpenAccessControlConfiguration();
                } else {
                    return super.getConfiguration(configClass);
                }
            }
        };
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus() throws Exception {
        allow(userPrincipal, "/", 0, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(userPrincipal));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, allPaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus1() throws Exception {
        allow(group1, node2Path, 0, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));

        assertReadStatus(DENY_THIS, DENY_THIS, cp, ImmutableSet.of("/", node1Path, UserConstants.DEFAULT_USER_PATH));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus2() throws Exception {
        allow(userPrincipal, "/", 0, JCR_READ);
        deny(group1, "/", 0, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(userPrincipal,group1));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, allPaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus3() throws Exception {
        allow(group1, "/", 0, JCR_READ);
        deny(group2, "/", 1, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));
        assertReadStatus(DENY_ALL_REGULAR, DENY_THIS, cp, allPaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus4() throws Exception {
        allow(group1, "/", 0, JCR_READ);
        allow(group2, node2Path, 1, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, allPaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus5() throws Exception {
        allow(userPrincipal, "/", 0, JCR_READ);
        deny(group2, node1Path, 1, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(userPrincipal, group2));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, allPaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus6() throws Exception {
        allow(group2, "/", 0, JCR_READ);
        deny(userPrincipal, node1Path, 0, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(userPrincipal, group2));

        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, rootAndUsers);
        assertReadStatus(DENY_ALL_REGULAR, DENY_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus7() throws Exception {
        allow(group2, "/", 0, REP_READ_PROPERTIES);
        allow(userPrincipal, node1Path, 0, REP_READ_NODES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(userPrincipal, group2));

        assertReadStatus(ALLOW_PROPERTIES, ALLOW_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus8() throws Exception {
        allow(userPrincipal, "/", 0, REP_READ_PROPERTIES);
        allow(group2, node1Path, 0, REP_READ_NODES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(userPrincipal, group2));

        assertReadStatus(ALLOW_PROPERTIES, ALLOW_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus9() throws Exception {
        allow(group2, "/", 0, REP_READ_PROPERTIES);
        allow(group1, node1Path, 0, REP_READ_NODES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));

        assertReadStatus(ALLOW_PROPERTIES, ALLOW_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus10() throws Exception {
        deny(group2, "/", 0, JCR_READ);
        allow(group1, node1Path, 0, REP_READ_NODES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));

        assertReadStatus(DENY_THIS, DENY_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_NODES, DENY_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus11() throws Exception {
        deny(group2, "/", 0, JCR_READ);
        deny(group2, node1Path, 0, JCR_READ);
        allow(group1, node2Path, 0, REP_READ_NODES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));

        Set<String> treePaths = ImmutableSet.of("/", UserConstants.DEFAULT_USER_PATH, node1Path);
        assertReadStatus(DENY_THIS, DENY_THIS, cp, treePaths);
        assertReadStatus(ALLOW_NODES, DENY_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus12() throws Exception {
        allow(group1, "/", 0, JCR_READ);
        deny(group1, node1Path, 0, REP_READ_PROPERTIES);
        allow(group1, node2Path, 0, REP_READ_NODES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));

        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_NODES, DENY_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus13() throws Exception {
        allow(group1, "/", 0, JCR_READ);
        deny(group1, node1Path, 0, REP_READ_PROPERTIES);
        allow(group1, node2Path, 0, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));

        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_NODES, DENY_THIS, cp, node1Path);
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus14() throws Exception {
        allow(group1, "/", 0, REP_READ_NODES);
        deny(group1, node1Path, 0, REP_READ_PROPERTIES);
        allow(group1, node2Path, 0, REP_READ_PROPERTIES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));

        assertReadStatus(ALLOW_NODES, DENY_THIS, cp, rootAndUsers);
        assertReadStatus(ALLOW_NODES, DENY_THIS, cp, node1Path);
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatus15() throws Exception {
        allow(group1, "/", 0, REP_READ_NODES);
        deny(group1, node1Path, 0, JCR_READ);
        allow(group1, node2Path, 0, REP_READ_PROPERTIES);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));

        assertReadStatus(ALLOW_THIS, DENY_THIS, cp, rootAndUsers);
        assertReadStatus(DENY_THIS, DENY_THIS, cp, node1Path);
        assertReadStatus(ALLOW_PROPERTIES, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithAccessControl() throws Exception {
        allow(group1, "/", 0, JCR_READ, JCR_READ_ACCESS_CONTROL);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));
        assertReadStatus(ALLOW_ALL, ALLOW_THIS, cp, allPaths);
        assertReadStatus(ALLOW_ALL, ALLOW_THIS, cp, "/rep:policy"); // TODO: check again
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithAccessControl1() throws Exception {
        allow(group1, node1Path, 0, JCR_READ, JCR_READ_ACCESS_CONTROL);
        deny(group1, node2Path, 0, JCR_READ_ACCESS_CONTROL);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));
        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, node1Path); // TODO: check again... shouldn't this be ALLOW_ALL_REGULAR?
        assertReadStatus(ALLOW_NODES, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithAccessControl2() throws Exception {
        allow(group1, node1Path, 0, JCR_READ);
        allow(group2, node2Path, 0, JCR_READ_ACCESS_CONTROL);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, node1Path);
        assertReadStatus(ALLOW_ALL, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithAccessControl3() throws Exception {
        allow(group1, node1Path, 0, REP_READ_PROPERTIES);
        allow(group2, node2Path, 0, JCR_READ_ACCESS_CONTROL);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));
        // TODO ???
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithRestrictions() throws Exception {
        allow(group1, node1Path, 0, new String[]{JCR_READ}, createGlobRestriction("/*"));
        allow(group2, node1Path, 1, JCR_READ);
        deny(group3, node1Path, 2, JCR_READ);

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1));
        assertReadStatus(DENY_THIS, ALLOW_THIS, cp, ImmutableSet.<String>of(node1Path));
        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, node2Path); // TODO: need to change RestrictionPattern in order to get ALLOW_ALL_REGULAR

        cp = createPermissions(ImmutableSet.of(group1, group2));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_THIS, cp, nodePaths);

        cp = createPermissions(ImmutableSet.of(group1, group2, group3));
        assertReadStatus(DENY_ALL_REGULAR, ALLOW_THIS, cp, nodePaths);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithRestrictions2() throws Exception {
        allow(group2, node1Path, 0, JCR_READ);
        allow(group1, node1Path, 1, new String[]{JCR_READ}, createGlobRestriction("/*"));

        CompiledPermissionImpl cp = createPermissions(ImmutableSet.of(group1, group2));
        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, nodePaths);  // TODO: should be ALLOW_ALL_REGULAR but requires detection of redundant ace
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithRestrictions3() throws Exception {
        allow(group2, node1Path, 0, JCR_READ);
        deny(group3, node1Path, 1, JCR_READ);
        allow(group1, node1Path, 2, new String[]{JCR_READ}, createGlobRestriction("/*"));

        CompiledPermissions cp = createPermissions(ImmutableSet.of(group1, group2, group3));
        assertReadStatus(DENY_THIS, ALLOW_THIS, cp, ImmutableSet.<String>of(node1Path));
        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, node2Path);
    }

    @Ignore("OAK-774")
    @Test
    public void testGetReadStatusWithRestrictions4() throws Exception {
        allow(group2, node1Path, 0, JCR_READ);
        deny(group3, node1Path, 1, REP_READ_PROPERTIES);
        allow(group1, node1Path, 2, new String[]{REP_READ_PROPERTIES}, createGlobRestriction("/*"));

        CompiledPermissions cp = createPermissions(ImmutableSet.of(group1, group2, group3));
        assertReadStatus(ALLOW_THIS, DENY_THIS, cp, ImmutableSet.<String>of(node1Path));
        assertReadStatus(ALLOW_THIS, ALLOW_THIS, cp, node2Path);
    }

    // TODO: more tests with restrictions
    // TODO: complex tests with entries for paths outside of the tested hierarchy
    // TODO: tests for isGranted
    // TODO: tests for hasPrivilege/getPrivileges
    // TODO: tests for path based evaluation

    @Test
    public void testGetReadStatusForReadPaths() throws Exception {
        CompiledPermissionImpl cp = createPermissions(Collections.singleton(userPrincipal));
        assertReadStatus(ALLOW_ALL_REGULAR, ALLOW_ALL_REGULAR, cp, ImmutableSet.copyOf(DEFAULT_READ_PATHS));
    }

    @Test
    public void testIsGrantedForReadPaths() throws Exception {
        CompiledPermissionImpl cp = createPermissions(Collections.singleton(userPrincipal));
        for (String path : DEFAULT_READ_PATHS) {
            assertTrue(cp.isGranted(path, Permissions.READ));
            assertTrue(cp.isGranted(path, Permissions.READ_NODE));
            assertTrue(cp.isGranted(path + '/' + JcrConstants.JCR_PRIMARYTYPE, Permissions.READ_PROPERTY));
            assertFalse(cp.isGranted(path, Permissions.READ_ACCESS_CONTROL));
        }

        for (String path : DEFAULT_READ_PATHS) {
            Tree tree = root.getTree(path);
            assertTrue(cp.isGranted(tree, null, Permissions.READ));
            assertTrue(cp.isGranted(tree, null, Permissions.READ_NODE));
            assertTrue(cp.isGranted(tree, tree.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
            assertFalse(cp.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL));
        }

        assertFalse(cp.isGranted(Permissions.READ));
        assertFalse(cp.isGranted(Permissions.READ_NODE));
        assertFalse(cp.isGranted(Permissions.READ_PROPERTY));
        assertFalse(cp.isGranted(Permissions.READ_ACCESS_CONTROL));
    }

    @Test
    public void testGetPrivilegesForReadPaths() throws Exception {
        CompiledPermissionImpl cp = createPermissions(Collections.singleton(userPrincipal));
        for (String path : DEFAULT_READ_PATHS) {
            Tree tree = root.getTree(path);
            assertEquals(Collections.singleton(PrivilegeConstants.JCR_READ), cp.getPrivileges(tree));
        }

        assertEquals(Collections.<String>emptySet(), cp.getPrivileges(null));
    }

    @Test
    public void testHasPrivilegesForReadPaths() throws Exception {
        CompiledPermissionImpl cp = createPermissions(Collections.singleton(userPrincipal));
        for (String path : DEFAULT_READ_PATHS) {
            Tree tree = root.getTree(path);
            assertTrue(cp.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
            assertTrue(cp.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
            assertTrue(cp.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
            assertFalse(cp.hasPrivileges(tree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        }

        assertFalse(cp.hasPrivileges(null, PrivilegeConstants.JCR_READ));
    }

    private CompiledPermissionImpl createPermissions(Set<Principal> principals) {
        ImmutableTree permissionsTree = new ImmutableRoot(root, TreeTypeProvider.EMPTY).getTree(PERMISSIONS_STORE_PATH);
        return new CompiledPermissionImpl(principals, permissionsTree, pbp, rp, ImmutableSet.copyOf(DEFAULT_READ_PATHS));
    }

    private void allow(Principal principal, String path, int index, String... privilegeNames) throws CommitFailedException {
        setupPermission(principal, path, true, index, privilegeNames, Collections.<Restriction>emptySet());
    }

    private void allow(Principal principal, String path, int index, String[] privilegeNames, Set<Restriction> restrictions) throws CommitFailedException {
        setupPermission(principal, path, true, index, privilegeNames, restrictions);
    }

    private void deny(Principal principal, String path, int index, String... privilegeNames) throws CommitFailedException {
        setupPermission(principal, path, false, index, privilegeNames, Collections.<Restriction>emptySet());
    }

    private void deny(Principal principal, String path, int index, String[] privilegeNames, Set<Restriction> restrictions) throws CommitFailedException {
        setupPermission(principal, path, false, index, privilegeNames, restrictions);
    }

    private void setupPermission(Principal principal, String path, boolean isAllow,
                                 int index, String[] privilegeName, Set<Restriction> restrictions) throws CommitFailedException {
        PrivilegeBits pb = pbp.getBits(privilegeName);
        String name = ((isAllow) ? PREFIX_ALLOW : PREFIX_DENY) + "-" + Objects.hashCode(path, principal, index, pb, isAllow, restrictions);
        Tree principalRoot = root.getTree(PERMISSIONS_STORE_PATH + '/' + principal.getName());
        Tree entry = principalRoot.addChild(name);
        entry.setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSIONS);
        entry.setProperty(REP_ACCESS_CONTROLLED_PATH, path);
        entry.setProperty(REP_INDEX, index);
        entry.setProperty(pb.asPropertyState(REP_PRIVILEGE_BITS));
        for (Restriction restriction : restrictions) {
            entry.setProperty(restriction.getProperty());
        }
        root.commit();
    }

    private void assertReadStatus(ReadStatus expectedTrees,
                                  ReadStatus expectedProperties,
                                  CompiledPermissions cp,
                                  String treePath) {
        assertReadStatus(expectedTrees, expectedTrees, cp, Collections.singleton(treePath));
    }

    private void assertReadStatus(ReadStatus expectedTrees,
                                  ReadStatus expectedProperties,
                                  CompiledPermissions cp,
                                  Set<String> treePaths) {
        for (String path : treePaths) {
            Tree node = root.getTree(path);
            assertSame("Tree " + path, expectedTrees, cp.getReadStatus(node, null));
            assertSame("Property jcr:primaryType " + path, expectedProperties, cp.getReadStatus(node, node.getProperty(JCR_PRIMARYTYPE)));
        }
    }

    private Set<Restriction> createGlobRestriction(String globValue) throws Exception {
        return Collections.singleton(rp.createRestriction(node1Path, REP_GLOB, getValueFactory().createValue(globValue)));
    }

    private class GroupImpl implements Group {

        private final String name;

        private GroupImpl(String name) {
            this.name = name;
        }

        @Override
        public boolean addMember(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeMember(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMember(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Enumeration<? extends Principal> members() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
