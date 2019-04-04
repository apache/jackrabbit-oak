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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.After;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.security.Principal;
import java.util.Iterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AutoSaveEnabledManagerTest extends AbstractAutoSaveTest {

    @After
    @Override
    public void after() throws Exception {
        try {
            Authorizable a = mgrDlg.getAuthorizable("u");
            if (a != null) {
                a.remove();
            }
            a = mgrDlg.getAuthorizable("g");
            if (a != null) {
                a.remove();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Override
    protected QueryEngineSettings getQueryEngineSettings() {
        if (querySettings == null) {
            querySettings = new QueryEngineSettings();
            querySettings.setFailTraversal(false);
        }
        return querySettings;
    }

    @Test
    public void testIsAutoSave() {
        assertTrue(autosaveMgr.isAutoSave());
        verify(mgrDlg, never()).isAutoSave();
    }

    @Test
    public void testSetAutoSave() throws Exception {
        assertTrue(autosaveMgr.isAutoSave());

        autosaveMgr.autoSave(false);
        assertFalse(autosaveMgr.isAutoSave());
        autosaveMgr.autoSave(true);

        assertTrue(autosaveMgr.isAutoSave());

        verify(mgrDlg, never()).isAutoSave();
        verify(mgrDlg, never()).autoSave(true);
        verify(mgrDlg, never()).autoSave(false);
    }

    @Test
    public void testGetAuthorizable() throws RepositoryException {
        Authorizable a = autosaveMgr.getAuthorizable(UserConstants.DEFAULT_ANONYMOUS_ID);
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
        assertTrue(a instanceof UserImpl);

        a = autosaveMgr.getAuthorizableByPath(a.getPath());
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
        assertTrue(a instanceof UserImpl);

        a = autosaveMgr.getAuthorizable(a.getPrincipal());
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
        assertTrue(a instanceof UserImpl);

        assertNull(autosaveMgr.getAuthorizable("unknown"));
    }

    @Test
    public void testFindAuthorizable() throws RepositoryException {
        Iterator<Authorizable> res = autosaveMgr.findAuthorizables(UserConstants.REP_AUTHORIZABLE_ID, UserConstants.DEFAULT_ANONYMOUS_ID);
        assertTrue(res.hasNext());

        Authorizable a = res.next();
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
    }

    @Test
    public void testFindAuthorizableWithSearchType() throws RepositoryException {
        Iterator<Authorizable> res = autosaveMgr.findAuthorizables(UserConstants.REP_AUTHORIZABLE_ID, UserConstants.DEFAULT_ANONYMOUS_ID, UserManager.SEARCH_TYPE_GROUP);
        assertFalse(res.hasNext());

        verify(mgrDlg, times(1)).findAuthorizables(UserConstants.REP_AUTHORIZABLE_ID, UserConstants.DEFAULT_ANONYMOUS_ID, UserManager.SEARCH_TYPE_GROUP);
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testFindAuthorizableWithQuery() throws RepositoryException {
        Query q = mock(Query.class);
        autosaveMgr.findAuthorizables(q);

        verify(mgrDlg, times(1)).findAuthorizables(q);
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testCreateRemoveUser() throws RepositoryException {
        User u = autosaveMgr.createUser("u", "u");
        assertFalse(root.hasPendingChanges());
        u.remove();
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testCreateUserWithPath() throws Exception {
        autosaveMgr.createUser("u", "u", new PrincipalImpl("u"), "rel/path");
        assertFalse(root.hasPendingChanges());

        verify(mgrDlg, times(1)).createUser("u", "u", new PrincipalImpl("u"), "rel/path");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testCreateSystemUser() throws Exception {
        autosaveMgr.createSystemUser("u", null);
        assertFalse(root.hasPendingChanges());

        verify(mgrDlg, times(1)).createSystemUser("u", null);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testCreateRemoveGroup() throws RepositoryException {
        Group g = autosaveMgr.createGroup("g");
        assertFalse(root.hasPendingChanges());
        g.remove();
        assertFalse(root.hasPendingChanges());

        verify(mgrDlg, times(1)).createGroup("g");
        verify(autosaveMgr, times(2)).autosave();
    }

    @Test
    public void testCreateGroupFromPrincipal() throws RepositoryException {
        Principal principal = new PrincipalImpl("g");
        autosaveMgr.createGroup(principal);
        assertFalse(root.hasPendingChanges());

        verify(mgrDlg, times(1)).createGroup(principal);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testCreateGroupFromPrincipalAndID() throws RepositoryException {
        Principal principal = new PrincipalImpl("g");
        autosaveMgr.createGroup(principal, "g");
        assertFalse(root.hasPendingChanges());

        verify(mgrDlg, times(1)).createGroup(principal, "g");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testCreateGroupFromIdPrincipalAndPath() throws RepositoryException {
        Principal principal = new PrincipalImpl("g");
        autosaveMgr.createGroup("g", principal, "rel/path");
        assertFalse(root.hasPendingChanges());

        verify(mgrDlg, times(1)).createGroup("g", principal, "rel/path");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testCommitFailedRevertChanges() throws RepositoryException {
        autosaveMgr.createUser("u", "u");
        try {
            autosaveMgr.createUser("u", "u");
            fail();
        } catch (RepositoryException e) {
            // success
            assertFalse(root.hasPendingChanges());
        }
        verify(autosaveMgr, times(2)).autosave();
    }

    @Test
    public void testAuthorizable() throws Exception {
        User u = autosaveMgr.createUser("u", "u");
        u.setProperty("prop", getValueFactory().createValue("value"));
        assertFalse(root.hasPendingChanges());

        u.setProperty("prop", new Value[] {getValueFactory().createValue(true)});
        assertFalse(root.hasPendingChanges());

        u.removeProperty("prop");
        assertFalse(root.hasPendingChanges());

        verify(autosaveMgr, times(4)).autosave();
    }

    @Test
    public void testUser() throws Exception {
        User u = autosaveMgr.createUser("u", "u");

        u.disable("disabled");
        assertTrue(u.isDisabled());
        assertFalse(root.hasPendingChanges());

        u.disable(null);
        assertFalse(u.isDisabled());
        assertFalse(root.hasPendingChanges());

        u.changePassword("t");
        assertFalse(root.hasPendingChanges());

        u.changePassword("tt", "t");
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testImpersonation() throws Exception {
        User u = autosaveMgr.createUser("u", "u");

        Impersonation imp = u.getImpersonation();
        Principal p = autosaveMgr.getAuthorizable("anonymous").getPrincipal();
        assertTrue(imp.grantImpersonation(p));
        assertFalse(root.hasPendingChanges());

        assertTrue(imp.revokeImpersonation(p));
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testGroup() throws Exception {
        User u = autosaveMgr.createUser("u", "u");
        Group g = autosaveMgr.createGroup("g");

        assertTrue(g.addMember(u));
        assertFalse(root.hasPendingChanges());
        assertTrue(g.isDeclaredMember(u));

        Iterator<Authorizable> it = g.getDeclaredMembers();
        if (it.hasNext()) {
            Authorizable a = it.next();
            assertTrue(a instanceof AuthorizableImpl);
            a.setProperty("prop", getValueFactory().createValue("blub"));
            assertFalse(root.hasPendingChanges());
        }

        it = g.getMembers();
        if (it.hasNext()) {
            Authorizable a = it.next();
            assertTrue(a instanceof AuthorizableImpl);
            a.setProperty("prop", getValueFactory().createValue("blub"));
            assertFalse(root.hasPendingChanges());
        }

        assertTrue(g.removeMember(u));
        assertFalse(root.hasPendingChanges());
        assertFalse(g.isDeclaredMember(u));
    }

    @Test
    public void testDeclaredMemberOf() throws Exception {
        User u = autosaveMgr.createUser("u", "u");
        Group g = autosaveMgr.createGroup("g");

        assertTrue(g.addMember(u));

        Iterator<Group> groups = u.declaredMemberOf();
        assertTrue(groups.hasNext());

        Group gAgain = groups.next();
        assertTrue(gAgain instanceof GroupImpl);
        assertTrue(gAgain.removeMember(u));
        assertFalse(root.hasPendingChanges());
        assertFalse(u.declaredMemberOf().hasNext());
    }

    @Test
    public void testMemberOf() throws Exception {
        User u = autosaveMgr.createUser("u", "u");
        Group g = autosaveMgr.createGroup("g");

        assertTrue(g.addMember(u));

        Iterator<Group> groups = u.memberOf();
        assertTrue(groups.hasNext());

        Group gAgain = groups.next();
        assertTrue(gAgain instanceof GroupImpl);
        assertTrue(gAgain.removeMember(u));
        assertFalse(root.hasPendingChanges());
        assertFalse(u.declaredMemberOf().hasNext());
    }

    @Test
    public void testAddMembers() throws Exception {
        User u = autosaveMgr.createUser("u", "u");
        Group g = autosaveMgr.createGroup("g");

        assertTrue(g.addMembers(u.getID()).isEmpty());
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testRemoveMembers() throws Exception {
        User u = autosaveMgr.createUser("u", "u");
        Group g = autosaveMgr.createGroup("g");
        g.addMember(u);

        assertTrue(g.removeMembers(u.getID()).isEmpty());
        assertFalse(root.hasPendingChanges());
    }
}