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

import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Randomized PermissionStore test. It generates a random structure (1110
 * nodes), samples 10% of the paths for setting 'user' allow read, for setting
 * 'user' deny read, 10% for setting 'group' allow read and 10% for setting
 * 'group' deny read.
 * <p>
 * For testing a custom implementation against the known evaluation rules,
 * override the {@link #getSecurityConfigParameters()} method.
 * <p>
 * For testing a custom implementation against the default implementation,
 * override the {@link #candidatePermissionProvider(Root, String, Set)}.
 *
 */
public abstract class AbstractPermissionRandomTestIT extends AbstractSecurityTest {

    protected final long seed = new Random().nextLong();
    private final Random random = new Random(seed);
    protected final String testPath = "testPath" + random.nextInt();

    private List<String> paths = new ArrayList<>();

    final Set<String> allowU = new HashSet<>();
    private final Set<String> denyU = new HashSet<>();
    private final Set<String> allowG = new HashSet<>();
    private final Set<String> denyG = new HashSet<>();

    private ContentSession testSession;
    private final String groupId = "gr" + UUID.randomUUID();

    @Override
    public void before() throws Exception {
        super.before();

        Tree rootNode = root.getTree("/");
        Tree testNode = TreeUtil.getOrAddChild(rootNode, testPath, NT_UNSTRUCTURED);

        // Setup 1110x
        create(testNode, 10, 0, 3, paths);
        root.commit();

        Collections.sort(paths);
        int sampleSize = paths.size() / 10;

        sample(paths, sampleSize, random, allowU);
        sample(paths, sampleSize, random, denyU);
        sample(paths, sampleSize, random, allowG);
        sample(paths, sampleSize, random, denyG);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.EMPTY;
    }

    protected PermissionProvider candidatePermissionProvider(@NotNull Root root, @NotNull String workspaceName,
            @NotNull Set<Principal> principals) {
        return new SetsPP(allowU, denyU, allowG, denyG);
    }

    private static void create(Tree t, int count, int lvl, int maxlvl, List<String> paths) throws Exception {
        if (lvl == maxlvl) {
            return;
        }
        for (int i = 0; i < count; i++) {
            Tree c = TreeUtil.addChild(t, "n" + i, NT_UNSTRUCTURED);
            paths.add(c.getPath());
            create(c, count, lvl + 1, maxlvl, paths);
        }
    }

    protected static void sample(List<String> paths, int size, Random random, Set<String> sample) {
        assertTrue(size > 0 && size <= paths.size());
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(paths.size());
            String path = paths.get(index);
            sample.add(path);
        }
    }

    @Test
    public void testRandomRead() throws Exception {
        Principal u = getTestUser().getPrincipal();
        Group group = getUserManager(root).createGroup(groupId);
        group.addMember(getTestUser());
        Principal g = group.getPrincipal();

        // set user allow read
        for (String path : allowU) {
            setPrivileges(u, path, true, JCR_READ);
        }
        // set user deny read
        for (String path : denyU) {
            setPrivileges(u, path, false, JCR_READ);
        }
        // set group allow read
        for (String path : allowG) {
            setPrivileges(g, path, true, JCR_READ);
        }
        // set group deny read
        for (String path : denyG) {
            setPrivileges(g, path, false, JCR_READ);
        }

        testSession = createTestSession();
        Root testRoot = testSession.getLatestRoot();

        AuthorizationConfiguration acConfig = getConfig(AuthorizationConfiguration.class);
        PermissionProvider pp = acConfig.getPermissionProvider(testRoot, testSession.getWorkspaceName(),
                testSession.getAuthInfo().getPrincipals());

        PermissionProvider candidate = candidatePermissionProvider(testRoot, testSession.getWorkspaceName(),
                testSession.getAuthInfo().getPrincipals());
        boolean isSetImpl = candidate instanceof SetsPP;

        for (String path : paths) {
            Tree t = testRoot.getTree(path);

            boolean hasPrivileges0 = pp.hasPrivileges(t, JCR_READ);
            boolean isGrantedA0 = pp.isGranted(t.getPath(), Session.ACTION_READ);
            boolean isGrantedP0 = pp.isGranted(t, null, Permissions.READ);
            String[] privs0 = pp.getPrivileges(t).toArray(new String[] {});
            Arrays.sort(privs0);

            boolean hasPrivileges1 = candidate.hasPrivileges(t, JCR_READ);
            boolean isGrantedA1 = candidate.isGranted(t.getPath(), Session.ACTION_READ);
            boolean isGrantedP1 = candidate.isGranted(t, null, Permissions.READ);
            String[] privs1 = candidate.getPrivileges(t).toArray(new String[] {});
            Arrays.sort(privs1);

            if (isSetImpl) {
                assertEquals("Unexpected #hasPrivileges on [" + path + "] expecting " + hasPrivileges1 + " got "
                        + hasPrivileges0 + ", seed " + seed, hasPrivileges1, hasPrivileges0);
                assertEquals("Unexpected #isGranted on [" + path + "] expecting " + isGrantedA1 + " got " + isGrantedA0
                        + ", seed " + seed, isGrantedA1, isGrantedA0);
                assertEquals("Unexpected #isGranted on [" + path + "] expecting " + isGrantedP1 + " got " + isGrantedP0
                        + ", seed " + seed, isGrantedP1, isGrantedP0);
                assertArrayEquals(privs1, privs0);

            } else {
                assertEquals("Unexpected #hasPrivileges on [" + path + "] expecting " + hasPrivileges0 + " got "
                        + hasPrivileges1 + ", seed " + seed, hasPrivileges1, hasPrivileges0);
                assertEquals("Unexpected #isGranted on [" + path + "] expecting " + isGrantedA0 + " got " + isGrantedA1
                        + ", seed " + seed, isGrantedA1, isGrantedA0);
                assertEquals("Unexpected #isGranted on [" + path + "] expecting " + isGrantedP0 + " got " + isGrantedP1
                        + ", seed " + seed, isGrantedP1, isGrantedP0);
                assertArrayEquals(privs0, privs1);
            }

        }
    }

    @Override
    public void after() throws Exception {
        try {
            if (testSession != null) {
                testSession.close();
            }
            assertTrue(root.getTree("/" + testPath).remove());
            root.commit();
        } finally {
            super.after();
        }
    }

    private void setPrivileges(Principal principal, String path, boolean allow, String... privileges) throws Exception {
        AccessControlManager acm = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, path);
        acl.addEntry(principal, privilegesFromNames(privileges), allow);
        acm.setPolicy(path, acl);
        root.commit();
    }

    private static class SetsPP implements PermissionProvider {

        SetsPP(Set<String> allowU, Set<String> denyU, Set<String> allowG, Set<String> denyG) {
            this.allowU = allowU;
            this.denyU = denyU;
            this.allowG = allowG;
            this.denyG = denyG;
        }

        final Set<String> allowU;
        final Set<String> denyU;
        final Set<String> allowG;
        final Set<String> denyG;

        @Override
        public void refresh() {
        }

        @NotNull
        @Override
        public Set<String> getPrivileges(Tree tree) {
            if (canRead(tree.getPath())) {
                return Set.of(JCR_READ);
            } else {
                return Set.of();
            }
        }

        @Override
        public boolean hasPrivileges(Tree tree, @NotNull String... privilegeNames) {
            assertTrue("Implemened only for JCR_READ",
                    privilegeNames.length == 1 && privilegeNames[0].equals(JCR_READ));
            return canRead(tree.getPath());
        }

        @NotNull
        @Override
        public RepositoryPermission getRepositoryPermission() {
            throw new RuntimeException("unimplemented");
        }

        @NotNull
        @Override
        public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
            throw new RuntimeException("unimplemented");
        }

        @Override
        public boolean isGranted(@NotNull Tree tree, PropertyState property, long permissions) {
            assertTrue("Implemened only for Permissions.READ on trees",
                    property == null && permissions == Permissions.READ);
            return canRead(tree.getPath());
        }

        @Override
        public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
            assertEquals("Implemened only for Session.ACTION_READ", Session.ACTION_READ, jcrActions);
            return canRead(oakPath);
        }

        private boolean canRead(String p) {

            String deny = extractStatus(p, denyU);
            String allow = extractStatus(p, allowU);
            String gdeny = extractStatus(p, denyG);
            String gallow = extractStatus(p, allowG);

            if (deny != null) {
                if (allow != null) {
                    return deny.length() < allow.length();
                } else {
                    return false;
                }

            } else if (allow != null) {
                return true;

            } else if (gdeny != null) {
                if (gallow != null) {
                    return gdeny.length() < gallow.length();
                } else {
                    return false;
                }

            } else {
                return gallow != null;
            }
        }

        private static String extractStatus(String p, Set<String> paths) {
            String res = null;
            int len = 0;
            for (String path : paths) {
                if (p.contains(path) && len < path.length()) {
                    res = path;
                    len = path.length();
                }
            }
            return res;
        }
    }
}
