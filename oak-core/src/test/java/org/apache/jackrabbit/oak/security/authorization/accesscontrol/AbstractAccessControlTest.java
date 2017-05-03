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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;

public abstract class AbstractAccessControlTest extends AbstractSecurityTest implements PrivilegeConstants {

    static final String TEST_PATH = "/testPath";

    PrivilegeManager privilegeManager;
    PrincipalManager principalManager;

    ACL acl;
    Principal testPrincipal;
    Privilege[] testPrivileges;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), getNamePathMapper());
        rootNode.addChild("testPath", JcrConstants.NT_UNSTRUCTURED);
        root.commit();

        testPrincipal = getTestUser().getPrincipal();
        testPrivileges = privilegesFromNames(JCR_ADD_CHILD_NODES, JCR_LOCK_MANAGEMENT);

        privilegeManager = getPrivilegeManager(root);
        principalManager = getPrincipalManager(root);

        acl = createEmptyACL();
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

    RestrictionProvider getRestrictionProvider() {
        return getConfig(AuthorizationConfiguration.class).getRestrictionProvider();
    }

    PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(root);
    }

    List<ACE> createTestEntries() throws RepositoryException {
        List<ACE> entries = new ArrayList(3);
        for (int i = 0; i < 3; i++) {
            entries.add(createEntry(
                    new PrincipalImpl("testPrincipal" + i), true, null, PrivilegeConstants.JCR_READ));
        }
        return entries;
    }

    ACE createEntry(Principal principal, boolean isAllow, Set<Restriction> restrictions, String... privilegeNames) throws RepositoryException {
        return createEntry(principal, privilegesFromNames(privilegeNames), isAllow, restrictions);
    }

    ACE createEntry(Principal principal, Privilege[] privileges, boolean isAllow)
            throws RepositoryException {
        return createEntry(principal, privileges, isAllow, null);
    }

    ACE createEntry(Principal principal, PrivilegeBits bits, boolean isAllow, Set<Restriction> restrictions) throws RepositoryException {
        AccessControlPolicyIterator it = getAccessControlManager(root).getApplicablePolicies(TEST_PATH);
        while (it.hasNext()) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof ACL) {
                return ((ACL) policy).createACE(principal, bits, isAllow, restrictions);
            }
        }

        throw new UnsupportedOperationException();
    }

    private ACE createEntry(@Nonnull Principal principal, @Nonnull Privilege[] privileges, boolean isAllow, @Nullable Set<Restriction> restrictions)
            throws RepositoryException {
        ACL acl = createEmptyACL();
        return acl.createACE(principal, getBitsProvider().getBits(privileges, getNamePathMapper()), isAllow, restrictions);
    }

    ACL createEmptyACL() {
        return createACL(TEST_PATH, Collections.<ACE>emptyList(), getNamePathMapper(), getRestrictionProvider());
    }

    ACL createACL(@Nonnull List<ACE> entries) {
        return createACL(TEST_PATH, entries, namePathMapper, getRestrictionProvider());
    }

    ACL createACL(@Nullable String jcrPath,
                  @Nonnull List<ACE> entries,
                  @Nonnull NamePathMapper namePathMapper) {
        return createACL(jcrPath, entries, namePathMapper, getRestrictionProvider());
    }

    ACL createACL(@Nullable String jcrPath,
                  @Nonnull List<ACE> entries,
                  @Nonnull NamePathMapper namePathMapper,
                  final @Nonnull RestrictionProvider restrictionProvider) {
        String path = (jcrPath == null) ? null : namePathMapper.getOakPath(jcrPath);
        return new ACL(path, entries, namePathMapper) {
            @Nonnull
            @Override
            public RestrictionProvider getRestrictionProvider() {
                return restrictionProvider;
            }

            @Override
            ACE createACE(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions) throws RepositoryException {
                return createEntry(principal, privilegeBits, isAllow, restrictions);
            }

            @Override
            boolean checkValidPrincipal(Principal principal) throws AccessControlException {
                Util.checkValidPrincipal(principal, principalManager);
                return true;
            }

            @Override
            PrivilegeManager getPrivilegeManager() {
                return privilegeManager;
            }

            @Override
            PrivilegeBits getPrivilegeBits(Privilege[] privileges) {
                return new PrivilegeBitsProvider(root).getBits(privileges, getNamePathMapper());
            }
        };
    }
}