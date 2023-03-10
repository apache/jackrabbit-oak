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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractAccessControlTest extends AbstractSecurityTest {

    static final String TEST_PATH = "/testPath";

    private PrivilegeManager privilegeManager;
    PrincipalManager principalManager;

    Principal testPrincipal;
    Privilege[] testPrivileges;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        TreeUtil.addChild(root.getTree(PathUtils.ROOT_PATH), "testPath", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        testPrincipal = getTestUser().getPrincipal();
        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_LOCK_MANAGEMENT);

        privilegeManager = getPrivilegeManager(root);
        principalManager = getPrincipalManager(root);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            Tree t = root.getTree(TEST_PATH);
            if (t.exists()) {
                t.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @NotNull
    RestrictionProvider getRestrictionProvider() {
        return getConfig(AuthorizationConfiguration.class).getRestrictionProvider();
    }

    @NotNull
    PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(root);
    }

    @NotNull
    ACE createEntry(@NotNull Principal principal, boolean isAllow, @Nullable Set<Restriction> restrictions, @NotNull String... privilegeNames) throws RepositoryException {
        return createEntry(principal, privilegesFromNames(privilegeNames), isAllow, (restrictions==null) ? Collections.emptySet() : restrictions);
    }
    
    @NotNull
    ACE createEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow, @NotNull Set<Restriction> restrictions) throws RepositoryException {
        ACL acl = createACL(TEST_PATH, Collections.emptyList(), getNamePathMapper(), getRestrictionProvider());
        return acl.createACE(principal, getBitsProvider().getBits(privileges, getNamePathMapper()), isAllow, restrictions);
    }

    @NotNull
    ACE createEntry(@NotNull Principal principal, @NotNull PrivilegeBits bits, boolean isAllow, @NotNull Set<Restriction> restrictions) throws RepositoryException {
        AccessControlPolicyIterator it = getAccessControlManager(root).getApplicablePolicies(TEST_PATH);
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof ACL) {
                return ((ACL) policy).createACE(principal, bits, isAllow, restrictions);
            }
        }

        throw new UnsupportedOperationException();
    }

    @NotNull
    ACL createACL(@Nullable String jcrPath,
                  @NotNull List<ACE> entries,
                  @NotNull NamePathMapper namePathMapper,
                  @NotNull RestrictionProvider restrictionProvider) {
        return createACL(jcrPath, entries, namePathMapper, restrictionProvider, privilegeManager);
    }

    @NotNull
    ACL createACL(@Nullable String jcrPath,
                  @NotNull List<ACE> entries,
                  @NotNull NamePathMapper namePathMapper,
                  @NotNull RestrictionProvider restrictionProvider,
                  @NotNull PrivilegeManager privilegeManager) {
        String path = (jcrPath == null) ? null : namePathMapper.getOakPath(jcrPath);
        return new ACL(path, entries, namePathMapper) {
            @NotNull
            @Override
            public RestrictionProvider getRestrictionProvider() {
                return restrictionProvider;
            }

            @Override
            @NotNull
            ACE createACE(@NotNull Principal principal, @NotNull PrivilegeBits privilegeBits, boolean isAllow, @NotNull Set<Restriction> restrictions) throws RepositoryException {
                return createEntry(principal, privilegeBits, isAllow, restrictions);
            }

            @Override
            boolean checkValidPrincipal(@Nullable Principal principal) throws AccessControlException {
                return Util.checkValidPrincipal(principal, principalManager, Util.getImportBehavior(getConfig(AuthorizationConfiguration.class)));
            }

            @Override
            @NotNull
            PrivilegeManager getPrivilegeManager() {
                return privilegeManager;
            }

            @Override
            @NotNull
            PrivilegeBits getPrivilegeBits(@NotNull Privilege[] privileges) {
                return new PrivilegeBitsProvider(root).getBits(privileges, getNamePathMapper());
            }
        };
    }
    
    static JackrabbitAccessControlEntry mockAccessControlEntry(@NotNull Principal principal, @NotNull Privilege[] privs) {
        JackrabbitAccessControlEntry ace = mock(JackrabbitAccessControlEntry.class);
        when(ace.getPrincipal()).thenReturn(principal);
        when(ace.getPrivileges()).thenReturn(privs);
        return ace;
    }
    
    static void assertPolicies(@Nullable AccessControlPolicy[] policies, long expectedSize) {
        assertNotNull(policies);
        assertEquals(expectedSize, policies.length);
    }
}
