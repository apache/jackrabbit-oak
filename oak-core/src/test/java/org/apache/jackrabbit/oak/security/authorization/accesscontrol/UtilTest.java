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

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlException;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
        Util.checkValidPrincipal(() -> null, getPrincipalManager(root), ImportBehavior.BESTEFFORT);
    }

    @Test(expected = AccessControlException.class)
    public void testCheckValidPrincipalForEmpty() throws Exception {
        Util.checkValidPrincipal(new PrincipalImpl(""), getPrincipalManager(root), ImportBehavior.BESTEFFORT);
    }

    @Test
    public void testIsAceNonExistingTree() {
        Tree t = when(mock(Tree.class).exists()).thenReturn(false).getMock();
        assertFalse(Util.isACE(t, ReadOnlyNodeTypeManager.getInstance(root, getNamePathMapper())));
    }

    @Test
    public void testIsAceOtherTree() {
        assertFalse(Util.isACE(root.getTree(PathUtils.ROOT_PATH), ReadOnlyNodeTypeManager.getInstance(root, getNamePathMapper())));
    }

    @Test
    public void testIsAce() {
        Tree t = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        when(t.getProperty(JCR_PRIMARYTYPE)).thenReturn(PropertyStates.createProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_DENY_ACE, Type.NAME));
        assertTrue(Util.isACE(t, ReadOnlyNodeTypeManager.getInstance(root, getNamePathMapper())));
    }

    @Test
    public void testGetImportBehaviorDefault() {
        AuthorizationConfiguration config = when(mock(AuthorizationConfiguration.class).getParameters()).thenReturn(ConfigurationParameters.EMPTY).getMock();
        assertSame(ImportBehavior.ABORT, Util.getImportBehavior(config));
    }

    @Test
    public void testGetImportBehaviorInvalid() {
        AuthorizationConfiguration config = when(mock(AuthorizationConfiguration.class).getParameters()).thenReturn(ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, "invalid")).getMock();
        assertSame(ImportBehavior.ABORT, Util.getImportBehavior(config));
    }

    @Test
    public void testGetImportBehavior() {
        AuthorizationConfiguration config = when(mock(AuthorizationConfiguration.class).getParameters()).thenReturn(ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_BESTEFFORT)).getMock();
        assertSame(ImportBehavior.BESTEFFORT, Util.getImportBehavior(config));
    }

    @Test
    public void testGenerateName() {
        ACE ace = mockACE(true);
        String name = Util.generateAceName(ace, 0);

        assertTrue(name.startsWith(ALLOW));
        assertEquals(ALLOW, name);
        assertEquals(name, Util.generateAceName(ace, 0));

        name = Util.generateAceName(ace, 1);
        assertTrue(name.startsWith(ALLOW));
        assertEquals(ALLOW + 1, name);
        assertEquals(name, Util.generateAceName(ace, 1));

        verify(ace, times(4)).isAllow();
        verifyNoMoreInteractions(ace);
    }

    @Test
    public void testGenerateName2() {
        ACE ace = mockACE(false);
        String name = Util.generateAceName(ace, 0);

        assertTrue(name.startsWith(DENY));
        assertEquals(DENY, name);
        assertEquals(name, Util.generateAceName(ace, 0));

        name = Util.generateAceName(ace, 2);
        assertTrue(name.startsWith(DENY));
        assertEquals(DENY + 2, name);
        assertEquals(name, Util.generateAceName(ace, 2));

        verify(ace, times(4)).isAllow();
        verifyNoMoreInteractions(ace);
    }

    @Test
    public void testGenerateNameDifferentAllow() {
        ACE allow = mockACE(false);
        ACE deny = mockACE(true);

        assertNotEquals(Util.generateAceName(allow, 0), Util.generateAceName(deny, 0));
        assertNotEquals(Util.generateAceName(allow, 1), Util.generateAceName(deny, 1));
        assertNotEquals(Util.generateAceName(allow, 20), Util.generateAceName(deny, 20));
        assertNotEquals(Util.generateAceName(allow, 0), Util.generateAceName(deny, 1));
        assertNotEquals(Util.generateAceName(allow, 1), Util.generateAceName(deny, 20));
        
        verify(allow, times(5)).isAllow();
        verify(deny, times(5)).isAllow();
        verifyNoMoreInteractions(allow, deny);
    }

    private ACE mockACE(boolean isAllow) {
        return mock(ACE.class, withSettings().useConstructor(EveryonePrincipal.getInstance(), bitsProvider.getBits(PrivilegeConstants.JCR_READ), isAllow, null, NamePathMapper.DEFAULT).defaultAnswer(CALLS_REAL_METHODS));
    }
}
