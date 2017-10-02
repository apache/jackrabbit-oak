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
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.mockito.Mockito;

public abstract class AbstractAccessControlTest {

    final Root root = Mockito.mock(Root.class);

    Principal testPrincipal = new PrincipalImpl("testPrincipal");

    PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(root);
    }

    NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    ACE createEntry(boolean isAllow, String... privilegeName)
            throws RepositoryException {
        if (privilegeName.length == 1) {
            return createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(privilegeName[0]), isAllow);
        } else {
            PrivilegeBits bits = PrivilegeBits.getInstance();
            for (String n : privilegeName) {
                bits.add(PrivilegeBits.BUILT_IN.get(n));
            }
            return createEntry(testPrincipal, bits.unmodifiable(), isAllow);
        }
    }

    ACE createEntry(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Restriction... restrictions)
            throws RepositoryException {
        return new TestACE(principal, privilegeBits, isAllow, ImmutableSet.copyOf(restrictions));
    }

    private final class TestACE extends ACE {

        private TestACE(Principal principal, PrivilegeBits privilegeBits, boolean isAllow, Set<Restriction> restrictions) throws AccessControlException {
            super(principal, privilegeBits, isAllow, restrictions, getNamePathMapper());
        }

        @Override
        public Privilege[] getPrivileges() {
            throw new UnsupportedOperationException();
        }
    }
}
