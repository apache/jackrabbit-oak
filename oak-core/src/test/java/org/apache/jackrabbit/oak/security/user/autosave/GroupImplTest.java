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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GroupImplTest extends AbstractAutoSaveTest {

    private Group dlg;
    private User memberDlg;
    private GroupImpl group;

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        Group g = mgrDlg.createGroup("g");
        memberDlg = mgrDlg.createUser("u", null);
        g.addMember(memberDlg);
        root.commit();

        dlg = spy(g);
        assertFalse(dlg instanceof GroupImpl);

        group = new GroupImpl(dlg, autosaveMgr);
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            dlg.remove();
            memberDlg.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetDeclaredMembers() throws Exception {
        assertTrue(group.getDeclaredMembers().hasNext());
        verify(dlg, times(1)).getDeclaredMembers();
    }

    @Test
    public void testGetMembers() throws Exception {
        assertTrue(group.getMembers().hasNext());
        verify(dlg, times(1)).getMembers();
    }

    @Test
    public void testIsDeclaredMember() throws Exception {
        User u = new UserImpl(memberDlg, autosaveMgr);
        assertFalse(group.isDeclaredMember(memberDlg));
        assertTrue(group.isDeclaredMember(u));

        verify(dlg, times(1)).isDeclaredMember(memberDlg);
        verify(dlg, never()).isDeclaredMember(u);
    }

    @Test
    public void testIsMember() throws Exception {
        User u = new UserImpl(memberDlg, autosaveMgr);
        assertFalse(group.isMember(memberDlg));
        assertTrue(group.isMember(u));

        verify(dlg, times(1)).isMember(memberDlg);
        verify(dlg, never()).isMember(u);
    }

    @Test
    public void testAddInvalidMember() throws Exception {
        assertFalse(group.addMember(getTestUser()));
        verify(dlg, never()).addMember(getTestUser());
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testAddMember() throws Exception {
        User u = new UserImpl(getTestUser(), autosaveMgr);
        assertTrue(group.addMember(u));
        verify(dlg, times(1)).addMember(getTestUser());
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testAddMembersById() throws Exception {
        assertEquals(ImmutableSet.of("m1", "m2", "m3"), group.addMembers("m1", "m2", "m3"));
        verify(dlg, times(1)).addMembers("m1", "m2", "m3");
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testRemoveInvalidMember() throws Exception {
        assertFalse(group.removeMember(memberDlg));
        verify(dlg, never()).removeMember(memberDlg);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testRemoveMember() throws Exception {
        User u = new UserImpl(memberDlg, autosaveMgr);
        assertTrue(group.removeMember(u));
        verify(dlg, times(1)).removeMember(memberDlg);
        verify(autosaveMgr, times(1)).autosave();
    }

    @Test
    public void testRemoveMembersById() throws Exception {
        assertEquals(ImmutableSet.of(getTestUser().getID()), group.removeMembers("u", getTestUser().getID()));
        verify(dlg, times(1)).removeMembers("u", getTestUser().getID());
        verify(autosaveMgr, times(1)).autosave();
    }
}