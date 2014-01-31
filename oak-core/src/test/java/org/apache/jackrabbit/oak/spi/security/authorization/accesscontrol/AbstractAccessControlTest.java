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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.NamespaceRegistry;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.name.ReadWriteNamespaceRegistry;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;

public abstract class AbstractAccessControlTest extends AbstractSecurityTest {

    private RestrictionProvider restrictionProvider;
    private PrivilegeBitsProvider bitsProvider;

    protected void registerNamespace(String prefix, String uri) throws Exception {
        NamespaceRegistry nsRegistry = new ReadWriteNamespaceRegistry(root) {
            @Override
            protected Root getWriteRoot() {
                return root;
            }
        };
        nsRegistry.registerNamespace(prefix, uri);
    }

    protected RestrictionProvider getRestrictionProvider() {
        if (restrictionProvider == null) {
            restrictionProvider = getConfig(AuthorizationConfiguration.class).getRestrictionProvider();
        }
        return restrictionProvider;
    }

    protected PrivilegeBitsProvider getBitsProvider() {
        if (bitsProvider == null) {
            bitsProvider = new PrivilegeBitsProvider(root);
        }
        return bitsProvider;
    }

    protected Principal getTestPrincipal() throws Exception {
        return getTestUser().getPrincipal();
    }

    protected ACE createEntry(Principal principal, boolean isAllow, Set<Restriction> restrictions, String... privilegeNames) throws RepositoryException {
        return new TestACE(principal, getBitsProvider().getBits(privilegeNames), isAllow, restrictions);
    }

    protected ACE createEntry(Principal principal, Privilege[] privileges, boolean isAllow)
            throws RepositoryException {
        PrivilegeBits bits = getBitsProvider().getBits(privileges, getNamePathMapper());
        return new TestACE(principal, bits, isAllow, null);
    }

    protected ACE createEntry(Principal principal, PrivilegeBits bits, boolean isAllow, Set<Restriction> restrictions) throws AccessControlException {
        return new TestACE(principal, bits, isAllow, restrictions);
    }

    private final class TestACE extends ACE {

    private TestACE(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions) throws AccessControlException {
        super(principal, privilegeBits, isAllow, restrictions, getNamePathMapper());
    }

    @Override
    public Privilege[] getPrivileges() {
        Set<Privilege> privileges = new HashSet<Privilege>();
            for (String name : bitsProvider.getPrivilegeNames(getPrivilegeBits())) {
                try {
                    privileges.add(getPrivilegeManager(root).getPrivilege(name));
                } catch (RepositoryException e) {
                    throw new RuntimeException(e);
                }
            }
            return privileges.toArray(new Privilege[privileges.size()]);
    }
}

}
