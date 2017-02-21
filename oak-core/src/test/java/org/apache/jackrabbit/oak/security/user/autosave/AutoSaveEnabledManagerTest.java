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

import java.security.Principal;
import java.util.Iterator;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AutoSaveEnabledManagerTest extends AbstractAutoSaveTest {

    @Override
    public void after() throws Exception {
        Authorizable a = mgr.getAuthorizable("u");
        if (a != null) {
            a.remove();
        }
        a = mgr.getAuthorizable("g");
        if (a != null) {
            a.remove();
        }
        if (root.hasPendingChanges()) {
            root.commit();
        }
        super.after();
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters userConfig = ConfigurationParameters.of(
                UserConstants.PARAM_SUPPORT_AUTOSAVE, Boolean.TRUE);
        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
    }

    @Test
    public void testAutoSaveEnabled() throws RepositoryException {
        assertTrue(mgr instanceof AutoSaveEnabledManager);
        assertTrue(mgr.isAutoSave());

        mgr.autoSave(false);
        assertFalse(mgr.isAutoSave());
        mgr.autoSave(true);
    }

    @Test
    public void testGetAuthorizable() throws RepositoryException {
        Authorizable a = mgr.getAuthorizable(UserConstants.DEFAULT_ANONYMOUS_ID);
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
        assertTrue(a instanceof UserImpl);

        a = mgr.getAuthorizableByPath(a.getPath());
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
        assertTrue(a instanceof UserImpl);

        a = mgr.getAuthorizable(a.getPrincipal());
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
        assertTrue(a instanceof UserImpl);

        assertNull(mgr.getAuthorizable("unknown"));
    }

    @Test
    public void testFindAuthorizable() throws RepositoryException {
        Iterator<Authorizable> res = mgr.findAuthorizables(UserConstants.REP_AUTHORIZABLE_ID, UserConstants.DEFAULT_ANONYMOUS_ID);
        assertTrue(res.hasNext());

        Authorizable a = res.next();
        assertNotNull(a);
        assertTrue(a instanceof AuthorizableImpl);
    }

    @Test
    public void testIsAutoSave() {
        assertTrue(mgr.isAutoSave());
    }

    @Test
    public void testAutoSave() throws RepositoryException {
        mgr.autoSave(false);
        mgr.autoSave(true);
    }

    @Test
    public void testCreateRemoveUser() throws RepositoryException {
        User u = mgr.createUser("u", "u");
        assertFalse(root.hasPendingChanges());
        u.remove();
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testCreateRemoveGroup() throws RepositoryException {
        Group g = mgr.createGroup("g");
        assertFalse(root.hasPendingChanges());
        g.remove();
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testCommitFailedRevertChanges() throws RepositoryException {
        User u = mgr.createUser("u", "u");
        try {
            User u2 = mgr.createUser("u", "u");
            fail();
        } catch (RepositoryException e) {
            // success
            assertFalse(root.hasPendingChanges());
        }
    }

    @Test
    public void testAuthorizable() throws Exception {
        User u = mgr.createUser("u", "u");
        u.setProperty("prop", getValueFactory().createValue("value"));
        assertFalse(root.hasPendingChanges());

        u.setProperty("prop", new Value[] {getValueFactory().createValue(true)});
        assertFalse(root.hasPendingChanges());

        u.removeProperty("prop");
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testUser() throws Exception {
        User u = mgr.createUser("u", "u");

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
        User u = mgr.createUser("u", "u");

        Impersonation imp = u.getImpersonation();
        Principal p = mgr.getAuthorizable("anonymous").getPrincipal();
        assertTrue(imp.grantImpersonation(p));
        assertFalse(root.hasPendingChanges());

        assertTrue(imp.revokeImpersonation(p));
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testGroup() throws Exception {
        User u = mgr.createUser("u", "u");
        Group g = mgr.createGroup("g");

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
        User u = mgr.createUser("u", "u");
        Group g = mgr.createGroup("g");

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
        User u = mgr.createUser("u", "u");
        Group g = mgr.createGroup("g");

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
        User u = mgr.createUser("u", "u");
        Group g = mgr.createGroup("g");

        assertTrue(g.addMembers(u.getID()).isEmpty());
        assertFalse(root.hasPendingChanges());
    }

    @Test
    public void testRemoveMembers() throws Exception {
        User u = mgr.createUser("u", "u");
        Group g = mgr.createGroup("g");
        g.addMember(u);

        assertTrue(g.removeMembers(u.getID()).isEmpty());
        assertFalse(root.hasPendingChanges());
    }
}