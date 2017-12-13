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

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CompositeProviderCustomMixTest extends AbstractSecurityTest {

    @Test
    public void hasPrivilegesTest() throws Exception {
        Set<String> supp1 = ImmutableSet.of(JCR_READ, JCR_NAMESPACE_MANAGEMENT);
        Set<String> supp2 = ImmutableSet.of(JCR_READ, JCR_WRITE);
        Set<String> all = Sets.union(supp1, supp2);

        // tests all possible 256 shuffles
        for (CompositionType type : CompositionType.values()) {
            for (Set<String> granted1 : Sets.powerSet(supp1)) {
                for (Set<String> granted2 : Sets.powerSet(supp2)) {
                    for (Set<String> ps : Sets.powerSet(all)) {
                        CompositePermissionProvider cpp = buildCpp(supp1, granted1, supp2, granted2, type, null);

                        boolean expected = expected(ps, supp1, granted1, supp2, granted2, type, true);
                        boolean result = cpp.hasPrivileges(null, ps.toArray(new String[] {}));

                        String err = "Checking " + ps + " in {supported: " + supp1 + ", granted: " + granted1 + "} "
                                + type + " {supported: " + supp2 + ", granted: " + granted2 + "}";
                        assertEquals(err, expected, result);
                    }
                }
            }
        }
    }

    @Test
    public void isGrantedTest() throws Exception {
        Set<String> supp1 = ImmutableSet.of(JCR_READ, JCR_NODE_TYPE_MANAGEMENT);
        Set<String> supp2 = ImmutableSet.of(JCR_READ, JCR_WRITE);
        Set<String> all = Sets.union(supp1, supp2);

        Map<String, Long> grantMap = Maps.newHashMap();
        grantMap.put(JCR_READ, Permissions.READ);
        grantMap.put(JCR_NODE_TYPE_MANAGEMENT, Permissions.NODE_TYPE_MANAGEMENT);
        grantMap.put(JCR_WRITE, Permissions.WRITE);

        Map<String, String> actionMap = Maps.newHashMap();
        actionMap.put(JCR_READ, JackrabbitSession.ACTION_READ);
        actionMap.put(JCR_NODE_TYPE_MANAGEMENT, JackrabbitSession.ACTION_NODE_TYPE_MANAGEMENT);
        actionMap.put(JCR_WRITE, JackrabbitSession.ACTION_ADD_NODE);

        // tests all possible 256 shuffles
        for (CompositionType type : CompositionType.values()) {
            for (Set<String> granted1 : Sets.powerSet(supp1)) {
                for (Set<String> granted2 : Sets.powerSet(supp2)) {
                    for (Set<String> ps : Sets.powerSet(all)) {
                        CompositePermissionProvider cpp = buildCpp(supp1, granted1, supp2, granted2, type, grantMap);
                        boolean expected = expected(ps, supp1, granted1, supp2, granted2, type, false);

                        boolean result1 = cpp.isGranted(null, null, mapToPermissions(ps, grantMap));
                        String err1 = "[isGranted1] Checking " + ps + " in {supported: " + supp1 + ", granted: "
                                + granted1 + "} " + type + " {supported: " + supp2 + ", granted: " + granted2 + "}";
                        assertEquals(err1, expected, result1);

                        // check existing path
                        boolean result2 = cpp.isGranted("/", mapToActions(ps, actionMap));
                        String err2 = "[isGranted2] Checking " + ps + " in {supported: " + supp1 + ", granted: "
                                + granted1 + "} " + type + " {supported: " + supp2 + ", granted: " + granted2 + "}";
                        assertEquals(err2, expected, result2);

                        // check non existing path
                        boolean result3 = cpp.isGranted("/doesnotexist", mapToActions(ps, actionMap));
                        String err3 = "[isGranted3] Checking " + ps + " in {supported: " + supp1 + ", granted: "
                                + granted1 + "} " + type + " {supported: " + supp2 + ", granted: " + granted2 + "}";
                        assertEquals(err3, expected, result3);
                    }
                }
            }
        }
    }

    @Test
    public void getRepositoryPermissionTest() throws Exception {
        Set<String> supp1 = ImmutableSet.of(JCR_READ, JCR_NODE_TYPE_MANAGEMENT);
        Set<String> supp2 = ImmutableSet.of(JCR_READ, JCR_WRITE);
        Set<String> all = Sets.union(supp1, supp2);

        Map<String, Long> grantMap = Maps.newHashMap();
        grantMap.put(JCR_READ, Permissions.READ);
        grantMap.put(JCR_NODE_TYPE_MANAGEMENT, Permissions.NODE_TYPE_MANAGEMENT);
        grantMap.put(JCR_WRITE, Permissions.WRITE);

        // tests all possible 256 shuffles
        for (CompositionType type : CompositionType.values()) {
            for (Set<String> granted1 : Sets.powerSet(supp1)) {
                for (Set<String> granted2 : Sets.powerSet(supp2)) {
                    for (Set<String> ps : Sets.powerSet(all)) {
                        CompositePermissionProvider cpp = buildCpp(supp1, granted1, supp2, granted2, type, grantMap);

                        boolean expected = expected(ps, supp1, granted1, supp2, granted2, type, false);
                        boolean result = cpp.getRepositoryPermission().isGranted(mapToPermissions(ps, grantMap));

                        String err = "Checking " + ps + " in {supported: " + supp1 + ", granted: " + granted1 + "} "
                                + type + " {supported: " + supp2 + ", granted: " + granted2 + "}";
                        assertEquals(err, expected, result);
                    }
                }
            }
        }
    }

    @Test
    public void getTreePermissionTest() throws Exception {
        Set<String> supp1 = ImmutableSet.of(JCR_READ, JCR_NODE_TYPE_MANAGEMENT);
        Set<String> supp2 = ImmutableSet.of(JCR_READ, JCR_WRITE);
        Set<String> all = Sets.union(supp1, supp2);

        Map<String, Long> grantMap = Maps.newHashMap();
        grantMap.put(JCR_READ, Permissions.READ);
        grantMap.put(JCR_NODE_TYPE_MANAGEMENT, Permissions.NODE_TYPE_MANAGEMENT);
        grantMap.put(JCR_WRITE, Permissions.WRITE);

        // tests all possible 256 shuffles
        for (CompositionType type : CompositionType.values()) {
            for (Set<String> granted1 : Sets.powerSet(supp1)) {
                for (Set<String> granted2 : Sets.powerSet(supp2)) {
                    for (Set<String> ps : Sets.powerSet(all)) {
                        CompositePermissionProvider cpp = buildCpp(supp1, granted1, supp2, granted2, type, grantMap);

                        boolean expected = expected(ps, supp1, granted1, supp2, granted2, type, false);
                        boolean result = cpp.getTreePermission(root.getTree("/"), TreePermission.EMPTY)
                                .isGranted(mapToPermissions(ps, grantMap));

                        String err = "Checking " + ps + " in {supported: " + supp1 + ", granted: " + granted1 + "} "
                                + type + " {supported: " + supp2 + ", granted: " + granted2 + "}";
                        assertEquals(err, expected, result);
                    }
                }
            }
        }
    }

    private static long mapToPermissions(Set<String> items, Map<String, Long> grantMap) {
        long perm = Permissions.NO_PERMISSION;
        for (String i : items) {
            perm |= grantMap.get(i);
        }
        return perm;
    }

    private static String mapToActions(Set<String> items, Map<String, String> actionMap) {
        if (items.isEmpty()) {
            return "";
        }
        String actions = "";
        for (String i : items) {
            actions += actionMap.get(i) + ",";
        }
        return actions.substring(0, actions.length() - 1);
    }

    private boolean expected(Set<String> check, Set<String> supported1, Set<String> granted1, Set<String> supported2,
            Set<String> granted2, CompositionType type, boolean emptyIsTrue) {
        // Special case handled differently in the composite permissions vs.
        // actions
        if (check.isEmpty()) {
            return emptyIsTrue;
        }

        if (type == CompositionType.OR) {
            return Sets.difference(Sets.difference(check, granted1), granted2).isEmpty();
        } else {
            Set<String> f1 = Sets.intersection(supported1, check);
            boolean hasf1 = granted1.containsAll(f1);
            Set<String> f2 = Sets.intersection(supported2, check);
            boolean hasf2 = granted2.containsAll(f2);
            return hasf1 && hasf2;
        }
    }

    private CompositePermissionProvider buildCpp(Set<String> supported1, Set<String> granted1, Set<String> supported2,
            Set<String> granted2, CompositionType type, Map<String, Long> grantMap) {
        AggregatedPermissionProvider a1 = new CustomProvider(root, supported1, granted1, grantMap);
        AggregatedPermissionProvider a2 = new CustomProvider(root, supported2, granted2, grantMap);

        AuthorizationConfiguration config = getConfig(AuthorizationConfiguration.class);
        List<AggregatedPermissionProvider> composite = ImmutableList.of(a1, a2);
        return new CompositePermissionProvider(root, composite, config.getContext(), type, getRootProvider());
    }

    private static class CustomProvider implements AggregatedPermissionProvider {

        private final PrivilegeBitsProvider pbp;

        private final Set<String> supported;
        private final Set<String> granted;
        private final Map<String, Long> grantMap;

        private CustomProvider(@Nonnull Root root, Set<String> supported, Set<String> granted,
                Map<String, Long> grantMap) {
            this.pbp = new PrivilegeBitsProvider(root);

            this.supported = supported;
            this.granted = granted;
            this.grantMap = grantMap;
        }

        private static PrivilegeBits toBits(Set<String> supported, PrivilegeBitsProvider pbp) {
            PrivilegeBits suppBits = PrivilegeBits.getInstance();
            for (String s : supported) {
                suppBits.add(pbp.getBits(s));
            }
            return suppBits;
        }

        @Nonnull
        @Override
        public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            return toBits(supported, pbp).retain(privilegeBits);
        }

        @Override
        public boolean hasPrivileges(Tree tree, String... privilegeNames) {
            Set<String> in = Sets.newHashSet(privilegeNames);
            return granted.containsAll(in);
        }

        private long supportedPermissions(long permissions) {
            long allperms = mapToPermissions(supported, grantMap);
            long delta = Permissions.diff(permissions, allperms);
            return Permissions.diff(permissions, delta);
        }

        @Override
        public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
            return supportedPermissions(permissions);
        }

        @Override
        public long supportedPermissions(TreeLocation location, long permissions) {
            return supportedPermissions(permissions);
        }

        @Override
        public long supportedPermissions(TreePermission treePermission, PropertyState property, long permissions) {
            return supportedPermissions(permissions);
        }

        @Override
        public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
            long myperms = mapToPermissions(granted, grantMap);
            return Permissions.includes(myperms, permissions);
        }

        @Override
        public boolean isGranted(TreeLocation location, long permissions) {
            long myperms = mapToPermissions(granted, grantMap);
            return Permissions.includes(myperms, permissions);
        }

        @Override
        public RepositoryPermission getRepositoryPermission() {
            return new RepositoryPermission() {

                @Override
                public boolean isGranted(long repositoryPermissions) {
                    long myperms = mapToPermissions(granted, grantMap);
                    return Permissions.includes(myperms, repositoryPermissions);
                }
            };
        }

        @Override
        public TreePermission getTreePermission(Tree tree, TreeType type, TreePermission parentPermission) {
            return new CustomTreePermission(granted, grantMap);
        }

        @Override
        public void refresh() {
            Assert.fail("method should not be called");
        }

        @Override
        public Set<String> getPrivileges(Tree tree) {
            Assert.fail("method should not be called");
            return null;
        }

        @Override
        public TreePermission getTreePermission(Tree tree, TreePermission parentPermission) {
            Assert.fail("method should not be called");
            return null;
        }

        @Override
        public boolean isGranted(String oakPath, String jcrActions) {
            Assert.fail("method should not be called");
            return false;
        }

        @Override
        public String toString() {
            return "CustomProvider [supported=" + supported + ", granted=" + granted + "]";
        }
    }

    private static class CustomTreePermission implements TreePermission {

        private final Set<String> granted;
        private final Map<String, Long> grantMap;

        public CustomTreePermission(Set<String> granted, Map<String, Long> grantMap) {
            this.granted = granted;
            this.grantMap = grantMap;
        }

        @Override
        public TreePermission getChildPermission(String childName, NodeState childState) {
            Assert.fail("method should not be called");
            return null;
        }

        @Override
        public boolean canRead() {
            Assert.fail("method should not be called");
            return false;
        }

        @Override
        public boolean canRead(PropertyState property) {
            Assert.fail("method should not be called");
            return false;
        }

        @Override
        public boolean canReadAll() {
            Assert.fail("method should not be called");
            return false;
        }

        @Override
        public boolean canReadProperties() {
            Assert.fail("method should not be called");
            return false;
        }

        @Override
        public boolean isGranted(long permissions) {
            long myperms = mapToPermissions(granted, grantMap);
            return Permissions.includes(myperms, permissions);
        }

        @Override
        public boolean isGranted(long permissions, PropertyState property) {
            Assert.fail("method should not be called");
            return false;
        }

    }
}
