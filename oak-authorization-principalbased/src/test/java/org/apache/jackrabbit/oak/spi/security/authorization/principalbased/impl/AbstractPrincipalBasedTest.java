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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.ObjectArrays;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NT_OAK_UNSTRUCTURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractPrincipalBasedTest extends AbstractSecurityTest {

    static final String INTERMEDIATE_PATH = UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH + "/test";
    static final String SUPPORTED_PATH = PathUtils.concat(UserConstants.DEFAULT_USER_PATH, INTERMEDIATE_PATH);

    static final String TEST_OAK_PATH = "/oak:content/child/grandchild/oak:subtree";

    static final Map<String, String> LOCAL_NAME_MAPPINGS = Map.of(
            "a","internal",
            "b","http://www.jcp.org/jcr/1.0",
            "c","http://jackrabbit.apache.org/oak/ns/1.0"
    );

    private User testSystemUser;
    private PrincipalBasedAuthorizationConfiguration principalBasedAuthorizationConfiguration;

    String testJcrPath;
    String testContentJcrPath;

    @Before
    public void before() throws Exception {
        super.before();
        namePathMapper = new NamePathMapperImpl(new LocalNameMapper(root, LOCAL_NAME_MAPPINGS));
        testJcrPath =  getNamePathMapper().getJcrPath(TEST_OAK_PATH);
        testContentJcrPath = PathUtils.getAncestorPath(testJcrPath, 3);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            if (testSystemUser != null) {
                getUserManager(root).getAuthorizable(testSystemUser.getID()).remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    @NotNull
    protected SecurityProvider initSecurityProvider() {
        SecurityProvider sp = super.initSecurityProvider();
        principalBasedAuthorizationConfiguration = new PrincipalBasedAuthorizationConfiguration();
        principalBasedAuthorizationConfiguration.bindFilterProvider(getFilterProvider());
        principalBasedAuthorizationConfiguration.bindMountInfoProvider(Mounts.defaultMountInfoProvider());
        SecurityProviderHelper.updateConfig(sp, principalBasedAuthorizationConfiguration, AuthorizationConfiguration.class);
        return sp;
    }

    @Override
    @NotNull
    protected Privilege[] privilegesFromNames(@NotNull String... privilegeNames) throws RepositoryException {
        Iterable<String> pn = Iterables.transform(CollectionUtils.toLinkedSet(privilegeNames), privName -> getNamePathMapper().getJcrName(privName));
        return super.privilegesFromNames(pn);
    }

    @NotNull
    User getTestSystemUser() throws Exception {
        if (testSystemUser == null) {
            String uid = "testSystemUser" + UUID.randomUUID();
            testSystemUser = getUserManager(root).createSystemUser(uid, INTERMEDIATE_PATH);
            root.commit();
        }
        return testSystemUser;

    }

    void setupContentTrees(@NotNull String oakPath) throws Exception {
        setupContentTrees(NT_OAK_UNSTRUCTURED, oakPath);
    }

    void setupContentTrees(@NotNull String ntName, @NotNull String... oakPaths) throws Exception {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        for (String absPath : oakPaths) {
            Tree t = rootTree;
            for (String element : PathUtils.elements(absPath)) {
                t = TreeUtil.getOrAddChild(t, element, ntName);
            }
        }
    }

    @NotNull
    PrincipalPolicyImpl getPrincipalPolicyImpl(@NotNull Principal testPrincipal, @NotNull JackrabbitAccessControlManager acMgr) throws Exception {
        for (JackrabbitAccessControlPolicy policy : ObjectArrays.concat(acMgr.getApplicablePolicies(testPrincipal), acMgr.getPolicies(testPrincipal), JackrabbitAccessControlPolicy.class)) {
            if (policy instanceof PrincipalPolicyImpl) {
                return (PrincipalPolicyImpl) policy;
            }
        }
        throw new IllegalStateException("unable to obtain PrincipalPolicyImpl");
    }

    @NotNull
    PrincipalPolicyImpl setupPrincipalBasedAccessControl(@NotNull Principal testPrincipal, @Nullable String effectivePath, @NotNull String... privNames) throws Exception {
        // set principal-based policy for 'testPrincipal'
        JackrabbitAccessControlManager jacm = getAccessControlManager(root);
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, jacm);
        policy.addEntry(effectivePath, privilegesFromNames(privNames));
        jacm.setPolicy(policy.getPath(), policy);
        return policy;
    }

    boolean addPrincipalBasedEntry(@NotNull PrincipalPolicyImpl policy, @Nullable String effectivePath, @NotNull String... privNames) throws Exception {
        boolean mod = policy.addEntry(effectivePath, privilegesFromNames(privNames));
        getAccessControlManager(root).setPolicy(policy.getPath(), policy);
        return mod;
    }

    boolean addDefaultEntry(@Nullable String path, @NotNull Principal principal, @NotNull String... privNames) throws Exception {
        return addDefaultEntry(path, principal, null, null, privNames);
    }

    boolean addDefaultEntry(@Nullable String path, @NotNull Principal principal, @Nullable Map<String, Value> restr, @Nullable Map<String, Value[]> mvRestr, @NotNull String... privNames) throws Exception {
        JackrabbitAccessControlManager jacm = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(jacm, path);
        requireNonNull(acl);

        boolean mod = acl.addEntry(principal, privilegesFromNames(privNames), true, restr, mvRestr);
        jacm.setPolicy(acl.getPath(), acl);
        return mod;
    }

    @NotNull
    PrincipalBasedPermissionProvider createPermissionProvider(@NotNull Root root, @NotNull Principal... principals) {
        PermissionProvider pp = principalBasedAuthorizationConfiguration.getPermissionProvider(root, root.getContentSession().getWorkspaceName(), Set.of(principals));
        if (pp instanceof PrincipalBasedPermissionProvider) {
            return (PrincipalBasedPermissionProvider) pp;
        } else {
            throw new IllegalStateException("not a PrincipalBasedPermissionProvider");
        }
    }

    PrincipalBasedAccessControlManager createAccessControlManager(@NotNull Root root) {
        AccessControlManager acMgr = principalBasedAuthorizationConfiguration.getAccessControlManager(root, getNamePathMapper());
        if (acMgr instanceof PrincipalBasedAccessControlManager) {
            return (PrincipalBasedAccessControlManager) acMgr;
        } else {
            throw new IllegalStateException("not a PrincipalBasedAccessControlManager");
        }
    }

    @NotNull
    FilterProvider getFilterProvider() {
        return createFilterProviderImpl(SUPPORTED_PATH);
    }

    @NotNull
    static FilterProviderImpl createFilterProviderImpl(@NotNull final String path) {
        return new FilterProviderImpl(path);
    }

    @NotNull
    MgrProvider getMgrProvider(Root r) {
        return new MgrProviderImpl(principalBasedAuthorizationConfiguration, r, getNamePathMapper());
    }

    @NotNull
    PrincipalBasedAuthorizationConfiguration getPrincipalBasedAuthorizationConfiguration() {
        return principalBasedAuthorizationConfiguration;
    }
    
    static void assertEffectivePolicies(@NotNull AccessControlPolicy[] effective, int expectedPolicies, 
                                        int principalPolicySize, boolean readPolicyExpected) {
        assertEquals(expectedPolicies, effective.length);
        if (principalPolicySize > -1) {
            assertTrue(effective[0] instanceof ImmutablePrincipalPolicy);
            assertEquals(principalPolicySize, ((ImmutablePrincipalPolicy) effective[0]).size());
        }
        if (readPolicyExpected) {
            assertTrue(effective[effective.length-1] instanceof ReadPolicy);
        }
    }
}