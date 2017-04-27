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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Test for {@code ImmutableACL}
 */
public class ImmutableACLTest extends AbstractAccessControlListTest {

    private Privilege[] testPrivileges;

    @Before
    public void before() throws Exception {
        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);
    }

    private static Privilege[] privilegesFromNames(String... privNames) {
        Privilege[] p = new Privilege[privNames.length];
        for (int i = 0; i < privNames.length; i++) {
            Privilege privilege = Mockito.mock(Privilege.class);
            when(privilege.getName()).thenReturn(privNames[i]);
            p[i] = privilege;
        }
        return p;
    }

    protected ImmutableACL createACL(@Nullable String jcrPath,
                                     @Nonnull List<JackrabbitAccessControlEntry> entries,
                                     @Nonnull NamePathMapper namePathMapper,
                                     @Nonnull RestrictionProvider restrictionProvider) {
        String oakPath = (jcrPath == null) ? null : namePathMapper.getOakPath(jcrPath);
        return new ImmutableACL(oakPath, entries, restrictionProvider, namePathMapper);
    }

    private void assertImmutable(JackrabbitAccessControlList acl) throws Exception {
        String msg = "ACL should be immutable.";

        try {
            acl.addAccessControlEntry(testPrincipal, testPrivileges);
            fail(msg);
        } catch (AccessControlException e) {
            // success
        }

        try {
            acl.addEntry(testPrincipal, testPrivileges, true);
            fail(msg);
        } catch (AccessControlException e) {
            // success
        }

        try {
            acl.addEntry(testPrincipal, testPrivileges, false, Collections.<String, Value>emptyMap());
            fail(msg);
        } catch (AccessControlException e) {
            // success
        }

        try {
            acl.addEntry(testPrincipal, testPrivileges, false, Collections.<String, Value>emptyMap(), Collections.<String, Value[]>emptyMap());
            fail(msg);
        } catch (AccessControlException e) {
            // success
        }

        AccessControlEntry[] entries = acl.getAccessControlEntries();
        if (entries.length > 1) {
            try {
                acl.orderBefore(entries[0], null);
                fail(msg);
            }  catch (AccessControlException e) {
                // success
            }

            try {
                acl.orderBefore(entries[1], entries[0]);
                fail(msg);
            }  catch (AccessControlException e) {
                // success
            }
        }

        for (AccessControlEntry ace : entries) {
            try {
                acl.removeAccessControlEntry(ace);
                fail(msg);
            }  catch (AccessControlException e) {
                // success
            }
        }
    }

    @Test
    public void testImmutable() throws Exception {
        List<JackrabbitAccessControlEntry> entries = new ArrayList();
        entries.add(createEntry(true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES));
        entries.add(createEntry(false, PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT));

        JackrabbitAccessControlList acl = createACL(entries);
        assertFalse(acl.isEmpty());
        assertEquals(2, acl.size());
        assertEquals(getTestPath(), acl.getPath());
        assertImmutable(acl);
    }

    @Test
    public void testEmptyIsImmutable() throws Exception {
        JackrabbitAccessControlList acl = createEmptyACL();

        assertTrue(acl.isEmpty());
        assertEquals(0, acl.size());
        assertEquals(getTestPath(), acl.getPath());
        assertImmutable(acl);
    }

    @Test
    public void testEqualsForEmpty() throws Exception {

        JackrabbitAccessControlList acl = createEmptyACL();

        assertEquals(acl, createEmptyACL());
        ACE entry = createEntry(true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);
        assertFalse(acl.equals(createACL(entry)));
        assertFalse(acl.equals(new TestACL(getTestPath(), getRestrictionProvider(), getNamePathMapper(), Collections.<JackrabbitAccessControlEntry>emptyList())));
    }

    @Test
    public void testEquals() throws Exception {
        RestrictionProvider rp = getRestrictionProvider();
        ACE ace1 = createEntry(testPrincipal, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_VERSION_MANAGEMENT), false);
        ACE ace2 = createEntry(true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);
        ACE ace2b = createEntry(true, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        JackrabbitAccessControlList acl = createACL(ace1, ace2);
        assertTrue(acl instanceof ImmutableACL);
        assertEquals(acl, acl);

        JackrabbitAccessControlList repoAcl = createACL((String) null, ace1, ace2);
        assertTrue(repoAcl instanceof ImmutableACL);
        assertEquals(repoAcl, repoAcl);

        assertEquals(acl, createACL(ace1, ace2));
        assertEquals(acl, createACL(ace1, ace2b));

        assertEquals(repoAcl, createACL((String) null, ace1, ace2b));

        assertFalse(acl.equals(createACL(ace2, ace1)));
        assertFalse(acl.equals(repoAcl));
        assertFalse(acl.equals(createEmptyACL()));
        assertFalse(acl.equals(createACL("/anotherPath", ace1, ace2)));
        assertFalse(acl.equals(new TestACL("/anotherPath", rp, getNamePathMapper(), ace1, ace2)));
        assertFalse(acl.equals(new TestACL("/anotherPath", rp, getNamePathMapper(), ace1, ace2)));
        assertFalse(acl.equals(new TestACL("/anotherPath", rp, getNamePathMapper())));
        assertFalse(acl.equals(new TestACL(getTestPath(), rp, getNamePathMapper(), ace1, ace2)));
    }

    @Test
    public void testHashCode() throws Exception {
        RestrictionProvider rp = getRestrictionProvider();
        ACE ace1 = createEntry(false, PrivilegeConstants.JCR_VERSION_MANAGEMENT);
        ACE ace2 = createEntry(true, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_ADD_CHILD_NODES);
        ACE ace2b = createEntry(true, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES, PrivilegeConstants.JCR_ADD_CHILD_NODES);

        JackrabbitAccessControlList acl = createACL(ace1, ace2);
        JackrabbitAccessControlList repoAcl = createACL((String) null, ace1, ace2);

        int hc = acl.hashCode();
        assertTrue(hc == createACL(ace1, ace2).hashCode());
        assertTrue(hc == createACL(ace1, ace2b).hashCode());

        assertTrue(repoAcl.hashCode() == createACL((String) null, ace1, ace2b).hashCode());

        assertFalse(hc == createACL(ace2, ace1).hashCode());
        assertFalse(hc == repoAcl.hashCode());
        assertFalse(hc == createEmptyACL().hashCode());
        assertFalse(hc == createACL("/anotherPath", ace1, ace2).hashCode());
        assertFalse(hc == new TestACL("/anotherPath", rp, getNamePathMapper(), ace1, ace2).hashCode());
        assertFalse(hc == new TestACL("/anotherPath", rp, getNamePathMapper(), ace1, ace2).hashCode());
        assertFalse(hc == new TestACL("/anotherPath", rp, getNamePathMapper()).hashCode());
        assertFalse(hc == new TestACL(getTestPath(), rp, getNamePathMapper(), ace1, ace2).hashCode());
    }
}