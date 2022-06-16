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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class UtilsTest implements Constants {

    private static final Principal PRINCIPAL = new PrincipalImpl("name");
    private static final String INVALID_PRIVILEGE_NAME = "invalid";

    private static Tree mockTree(boolean exists, @NotNull String name, @Nullable String type) {
        return MockUtility.mockTree(name, type, exists);
    }

    private static Filter mockFilter(boolean canHandle) {
        Filter filter = Mockito.mock(Filter.class);
        when(filter.canHandle(any(Set.class))).thenReturn(canHandle);
        return filter;
    }

    private static PrivilegeManager mockPrivilegeManager() throws RepositoryException {
        PrivilegeManager privilegeManager = Mockito.mock(PrivilegeManager.class);
        when(privilegeManager.getPrivilege(any(String.class))).then((Answer<Privilege>) invocationOnMock -> {
            Privilege p = Mockito.mock(Privilege.class);
            when(p.getName()).thenReturn(invocationOnMock.getArgument(0));
            return p;
        });
        when(privilegeManager.getPrivilege(INVALID_PRIVILEGE_NAME)).thenThrow(new AccessControlException());
        return privilegeManager;
    }

    @Test
    public void testIsPrincipalPolicyTreeNotExists() {
       assertFalse(Utils.isPrincipalPolicyTree(mockTree(false, NT_REP_PRINCIPAL_POLICY, REP_PRINCIPAL_POLICY)));
    }

    @Test
    public void testIsPrincipalPolicyTreeWrongName() {
        assertFalse(Utils.isPrincipalPolicyTree(mockTree(true, REP_RESTRICTIONS, NT_REP_PRINCIPAL_POLICY)));
    }

    @Test
    public void testIsPrincipalPolicyTreeWrongType() {
        assertFalse(Utils.isPrincipalPolicyTree(mockTree(true, REP_PRINCIPAL_POLICY, NT_REP_RESTRICTIONS)));
    }

    @Test
    public void testIsPrincipalPolicyTreeMissingType() {
        assertFalse(Utils.isPrincipalPolicyTree(mockTree(true, REP_PRINCIPAL_POLICY, null)));
    }

    @Test
    public void testIsPrincipalPolicyTreeWrongNameType() {
        assertFalse(Utils.isPrincipalPolicyTree(mockTree(true, REP_RESTRICTIONS, NT_REP_RESTRICTIONS)));
    }

    @Test
    public void testIsPrincipalPolicyTree() {
        assertTrue(Utils.isPrincipalPolicyTree(mockTree(true, REP_PRINCIPAL_POLICY, NT_REP_PRINCIPAL_POLICY))); }

    @Test(expected = AccessControlException.class)
    public void testCanHandleNullName() throws Exception {
        Utils.canHandle(Mockito.mock(Principal.class), mockFilter(true), ImportBehavior.ABORT);
    }

    @Test(expected = AccessControlException.class)
    public void testCanHandleEmptyName() throws Exception {
        Utils.canHandle(new PrincipalImpl(""), mockFilter(true), ImportBehavior.ABORT);
    }

    @Test(expected = AccessControlException.class)
    public void testCanHandleUnsupportedPrincipalAbort() throws Exception {
        Utils.canHandle(PRINCIPAL, mockFilter(false), ImportBehavior.ABORT);
    }

    @Test
    public void testCanHandleUnsupportedPrincipalIgnore() throws Exception {
        assertFalse(Utils.canHandle(PRINCIPAL, mockFilter(false), ImportBehavior.IGNORE));
    }

    @Test
    public void testCanHandleUnsupportedPrincipalBestEffort() throws Exception {
        assertFalse(Utils.canHandle(PRINCIPAL, mockFilter(false), ImportBehavior.BESTEFFORT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCanHandleIllegalImportBehavior() throws Exception {
        assertFalse(Utils.canHandle(PRINCIPAL, mockFilter(true), -1));
    }

    @Test
    public void testCanHandle() throws Exception {
        for (int behavior : new int[] {ImportBehavior.ABORT, ImportBehavior.BESTEFFORT, ImportBehavior.IGNORE}) {
            assertTrue(Utils.canHandle(PRINCIPAL, mockFilter(true), behavior));
        }
    }

    @Test
    public void testPrivilegesFromNamesEmptyNames() {
        assertArrayEquals(new Privilege[0], Utils.privilegesFromOakNames(Collections.emptySet(), Mockito.mock(PrivilegeManager.class), NamePathMapper.DEFAULT));
    }

    @Test
    public void testPrivilegesFromNamesInvalidName() throws Exception {
        assertArrayEquals(new Privilege[0], Utils.privilegesFromOakNames(Collections.singleton(INVALID_PRIVILEGE_NAME), mockPrivilegeManager(), NamePathMapper.DEFAULT));
    }

    @Test
    public void testPrivilegesFromNamesRemapped() throws Exception {
        NamePathMapper mapper = Mockito.mock(NamePathMapper.class);
        when(mapper.getJcrName(any())).thenReturn("c");

        Privilege[] privs = Utils.privilegesFromOakNames(Sets.newHashSet("a", "b"), mockPrivilegeManager(), mapper);
        assertEquals(2, privs.length);
        for (Privilege p : privs) {
            assertEquals("c", p.getName());
        }
    }
}