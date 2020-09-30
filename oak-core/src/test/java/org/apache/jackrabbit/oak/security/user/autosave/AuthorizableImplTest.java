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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AuthorizableImplTest extends AbstractAutoSaveTest {

    private Authorizable dlg;
    private AuthorizableImpl a;

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        Authorizable u = mgrDlg.createUser("u", "u");
        root.commit();

        dlg = spy(u);
        assertFalse(dlg instanceof AuthorizableImpl);
        a = new AuthorizableImpl(dlg, autosaveMgr);
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            Authorizable a = mgrDlg.getAuthorizable("u");
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetDlg() throws RepositoryException {
        assertSame(dlg, a.getDlg());
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetMgr() throws RepositoryException {
        assertSame(autosaveMgr, a.getMgr());
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetId() throws Exception {
        assertEquals(dlg.getID(), a.getID());
        verify(dlg, times(2)).getID();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testIsGroup() throws RepositoryException {
        assertEquals(dlg.isGroup(), a.isGroup());
        verify(dlg, times(2)).isGroup();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetPrincipal() throws Exception {
        assertEquals(dlg.getPrincipal(), a.getPrincipal());
        verify(dlg, times(2)).getPrincipal();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testDeclaredMemberOf() throws Exception {
        assertTrue(Iterators.elementsEqual(dlg.declaredMemberOf(), a.declaredMemberOf()));
        verify(dlg, times(2)).declaredMemberOf();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testMemberOf() throws Exception {
        assertTrue(Iterators.elementsEqual(dlg.memberOf(), a.memberOf()));
        verify(dlg, times(2)).memberOf();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testRemove() throws Exception {
        a.remove();
        verify(dlg, times(1)).remove();
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testGetPropertyNames() throws Exception {
        assertTrue(Iterators.elementsEqual(dlg.getPropertyNames(), a.getPropertyNames()));
        verify(dlg, times(2)).getPropertyNames();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testGetPropertyNamesRelPath() throws Exception {
        assertTrue(Iterators.elementsEqual(dlg.getPropertyNames("."), a.getPropertyNames(".")));
        verify(dlg, times(2)).getPropertyNames(".");
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testHasProperty() throws Exception {
        assertEquals(dlg.hasProperty("propName"), a.hasProperty("propName"));
        verify(dlg, times(2)).hasProperty("propName");
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testSetProperty() throws Exception {
        Value v = getValueFactory(root).createValue(23);
        a.setProperty("propName", v);
        verify(dlg, times(1)).setProperty("propName", v);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testSetPropertyMv() throws Exception {
        Value[] v = new Value[] {};
        a.setProperty("propName", v);
        verify(dlg, times(1)).setProperty("propName", v);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testGetProperty() throws Exception {
        assertNull(a.getProperty("propName"));
        verify(dlg, times(1)).getProperty("propName");
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testRemoveProperty() throws Exception {
        assertFalse(a.removeProperty("propName"));
        verify(dlg, times(1)).removeProperty("propName");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testGetPath() throws Exception {
        assertEquals(dlg.getPath(), a.getPath());
        verify(dlg, times(2)).getPath();
        verify(autosaveMgr, never()).autosave();
    }

    @Test
    public void testToString() throws Exception {
        Authorizable dlg = getTestUser();
        assertEquals(dlg.toString(), new AuthorizableImpl(dlg, autosaveMgr).toString());
    }

    @Test
    public void testHashCode() throws Exception {
        Authorizable dlg = getTestUser();
        assertEquals(dlg.hashCode(), new AuthorizableImpl(dlg, autosaveMgr).hashCode());
    }

    @Test
    public void testEquals() throws Exception {
        Authorizable dlg = getTestUser();
        AuthorizableImpl wrapped = new AuthorizableImpl(dlg, autosaveMgr);
        assertEquals(wrapped,  wrapped);
        assertEquals(wrapped, new AuthorizableImpl(dlg, autosaveMgr));
        assertEquals(wrapped, new UserImpl(getTestUser(), new AutoSaveEnabledManager(getUserManager(root), root)));
    }

    @Test
    public void testNotEquals() throws Exception {
        Authorizable dlg = getTestUser();
        AuthorizableImpl wrapped = new AuthorizableImpl(dlg, autosaveMgr);
        assertNotEquals(wrapped, dlg);
        assertNotEquals(wrapped, null);

        Authorizable mock = mock(Authorizable.class);
        assertNotEquals(wrapped, mock);

        mock = mock(AuthorizableImpl.class);
        assertNotEquals(wrapped, mock);
    }
}