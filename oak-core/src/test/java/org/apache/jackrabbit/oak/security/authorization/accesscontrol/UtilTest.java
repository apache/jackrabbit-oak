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

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class UtilTest extends AbstractSecurityTest {

    private static final String DENY = "deny";
    private static final String ALLOW = "allow";

    private PrivilegeBitsProvider bitsProvider;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckValidPrincipalInvalidBehavior() throws Exception {
        Util.checkValidPrincipal(() -> "name", getPrincipalManager(root), ImportBehavior.IGNORE-1);
    }

    @Test(expected = AccessControlException.class)
    public void testCheckValidPrincipalForNull() throws Exception {
        Util.checkValidPrincipal(null, getPrincipalManager(root));
    }

    @Test(expected = AccessControlException.class)
    public void testCheckValidPrincipalForEmpty() throws Exception {
        Util.checkValidPrincipal(new PrincipalImpl(""), getPrincipalManager(root));
    }

    @Test
    public void testGenerateName() throws AccessControlException {
        ACE ace = new TestAce(true);
        String name = Util.generateAceName(ace, 0);

        assertTrue(name.startsWith(ALLOW));
        assertEquals(ALLOW, name);
        assertEquals(name, Util.generateAceName(ace, 0));

        name = Util.generateAceName(ace, 1);
        assertTrue(name.startsWith(ALLOW));
        assertEquals(ALLOW + 1, name);
        assertEquals(name, Util.generateAceName(ace, 1));
    }

    @Test
    public void testGenerateName2() throws AccessControlException {
        ACE ace = new TestAce(false);
        String name = Util.generateAceName(ace, 0);

        assertTrue(name.startsWith(DENY));
        assertEquals(DENY, name);
        assertEquals(name, Util.generateAceName(ace, 0));

        name = Util.generateAceName(ace, 2);
        assertTrue(name.startsWith(DENY));
        assertEquals(DENY + 2, name);
        assertEquals(name, Util.generateAceName(ace, 2));
    }

    @Test
    public void testGenerateNameDifferentAllow() throws Exception {
        ACE allow = new TestAce(false);
        ACE deny = new TestAce(true);

        assertNotEquals(Util.generateAceName(allow, 0), Util.generateAceName(deny, 0));
        assertNotEquals(Util.generateAceName(allow, 1), Util.generateAceName(deny, 1));
        assertNotEquals(Util.generateAceName(allow, 20), Util.generateAceName(deny, 20));
        assertNotEquals(Util.generateAceName(allow, 0), Util.generateAceName(deny, 1));
        assertNotEquals(Util.generateAceName(allow, 1), Util.generateAceName(deny, 20));

    }

    private final class TestAce extends ACE {

        public TestAce(boolean isAllow) throws AccessControlException {
            super(EveryonePrincipal.getInstance(), bitsProvider.getBits(PrivilegeConstants.JCR_READ), isAllow, null, NamePathMapper.DEFAULT);
        }

        @Override
        public Privilege[] getPrivileges() {
            try {
                return privilegesFromNames(bitsProvider.getPrivilegeNames(getPrivilegeBits()));
            } catch (RepositoryException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
    }
}
