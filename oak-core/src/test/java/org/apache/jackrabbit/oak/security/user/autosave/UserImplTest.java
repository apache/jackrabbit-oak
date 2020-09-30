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
package org.apache.jackrabbit.oak.security.user.autosave;

import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserImplTest extends AbstractAutoSaveTest {

    private User dlg;
    private UserImpl user;
    private Impersonation impersonationMock = mock(Impersonation.class);

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        dlg = spy(getTestUser());
        assertFalse(dlg instanceof UserImpl);
        when(dlg.getImpersonation()).thenReturn(impersonationMock);

        user = new UserImpl(dlg, autosaveMgr);
    }

    @Test
    public void testIsAdmin() throws Exception {
        assertEquals(dlg.isAdmin(), user.isAdmin());
        verify(dlg, times(2)).isAdmin();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testIsSystemUser() throws Exception {
        assertEquals(dlg.isSystemUser(), user.isSystemUser());
        verify(dlg, times(2)).isSystemUser();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetCredentials() throws RepositoryException {
        assertEquals(dlg.getCredentials().getClass().getName(), user.getCredentials().getClass().getName());
        verify(dlg, times(2)).getCredentials();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetImpersonation() throws RepositoryException {
        user.getImpersonation();
        verify(dlg, times(2)).getImpersonation();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testChancePassword() throws Exception {
        user.changePassword("newPw");
        verify(dlg, times(1)).changePassword("newPw");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testChancePasswordOldPw() throws Exception {
        String oldpw = user.getID();
        user.changePassword("newPw", oldpw);
        verify(dlg, times(1)).changePassword("newPw", oldpw);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testDisable() throws Exception {
        user.disable("disable");
        verify(dlg, times(1)).disable("disable");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testIsDisabled() throws Exception {
        assertEquals(dlg.isDisabled(), user.isDisabled());
        verify(dlg, times(2)).isDisabled();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetDisabledReason() throws Exception {
        assertEquals(dlg.getDisabledReason(), user.getDisabledReason());
        verify(dlg, times(2)).getDisabledReason();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetImpersonators() throws Exception {
        user.getImpersonation().getImpersonators();
        verify(impersonationMock, times(1)).getImpersonators();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGrantImpersonation() throws Exception {
        Principal principal = new PrincipalImpl("any");
        user.getImpersonation().grantImpersonation(principal);
        verify(impersonationMock, times(1)).grantImpersonation(principal);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testRevokeImpersonation() throws Exception {
        Principal principal = new PrincipalImpl("any");
        user.getImpersonation().revokeImpersonation(principal);
        verify(impersonationMock, times(1)).revokeImpersonation(principal);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testImpersonation() throws Exception {
        user.getImpersonation().allows(SystemSubject.INSTANCE);
        verify(impersonationMock, times(1)).allows(SystemSubject.INSTANCE);
        verify(autosaveMgr, never()).autosave();
    }
}