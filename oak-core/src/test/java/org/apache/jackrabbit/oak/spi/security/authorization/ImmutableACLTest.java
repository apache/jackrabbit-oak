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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ImmutableACLTest... TODO
 */
public class ImmutableACLTest extends AbstractSecurityTest {

    private PrivilegeManager privMgr;
    private RestrictionProvider restrictionProvider;

    private String testPath = "/testPath";
    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    @Before
    public void before() throws Exception {
        super.before();

        Root root = adminSession.getLatestRoot();
        NamePathMapper npMapper = NamePathMapper.DEFAULT;

        privMgr = getSecurityProvider().getPrivilegeConfiguration().getPrivilegeManager(root, npMapper);
        restrictionProvider = getSecurityProvider().getAccessControlConfiguration().getRestrictionProvider(npMapper);

        testPrincipal = new PrincipalImpl("testPrincipal");
        testPrivileges = new Privilege[] {
                privMgr.getPrivilege(PrivilegeConstants.JCR_READ),
                privMgr.getPrivilege(PrivilegeConstants.JCR_ADD_CHILD_NODES)
        };
    }

    private ImmutableACL createACL(List<JackrabbitAccessControlEntry> entries) {
        return createACL(entries, testPath);
    }

    private ImmutableACL createACL(List<JackrabbitAccessControlEntry> entries, String path) {
        return new ImmutableACL(path, entries, restrictionProvider);
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
        List<JackrabbitAccessControlEntry> entries = new ArrayList<JackrabbitAccessControlEntry>();
        entries.add(new ACE(testPrincipal, testPrivileges, true, null));
        entries.add(new ACE(testPrincipal, new Privilege[] {privMgr.getPrivilege(PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT)}, false, null));

        ImmutableACL acl = createACL(entries);
        assertFalse(acl.isEmpty());
        assertEquals(2, acl.size());
        assertEquals(testPath, acl.getPath());
        assertImmutable(acl);
    }

    @Test
    public void testEmptyIsImmutable() throws Exception {
        ImmutableACL acl = createACL(Collections.<JackrabbitAccessControlEntry>emptyList());

        assertTrue(acl.isEmpty());
        assertEquals(0, acl.size());
        assertEquals(testPath, acl.getPath());
        assertImmutable(acl);
    }

    @Test
    public void testEquals() throws Exception {
        List<JackrabbitAccessControlEntry> entries = Collections.<JackrabbitAccessControlEntry>singletonList(new ACE(testPrincipal, testPrivileges, true, null));

        JackrabbitAccessControlList empty = createACL(Collections.<JackrabbitAccessControlEntry>emptyList());
        JackrabbitAccessControlList acl = createACL(entries);

        assertEquals(empty, createACL(Collections.<JackrabbitAccessControlEntry>emptyList()));
        assertEquals(acl, createACL(entries));

        assertFalse(empty.equals(acl));
        assertFalse(acl.equals(empty));
        assertFalse(acl.equals(createACL(entries, "/anotherPath")));
        assertFalse(acl.equals(new TestACL("/anotherPath", entries)));
        assertFalse(acl.equals(new TestACL("/anotherPath", Collections.<JackrabbitAccessControlEntry>emptyList())));
        assertFalse(acl.equals(new TestACL(testPath, entries)));
        assertFalse(empty.equals(new TestACL(testPath, Collections.<JackrabbitAccessControlEntry>emptyList())));
    }

    /**
     * Another JackrabbitAccessControlList
     */
    private final class TestACL extends AbstractAccessControlList {

        private List<JackrabbitAccessControlEntry> entries = new ArrayList<JackrabbitAccessControlEntry>();

        public TestACL(String jcrPath, List<JackrabbitAccessControlEntry> entries) {
            super(jcrPath);
            this.entries.addAll(entries);
        }

        @Override
        public boolean isEmpty() {
            return entries.isEmpty();
        }

        @Override
        public int size() {
            return entries.size();
        }

        @Override
        public boolean addEntry(Principal principal, Privilege[] privileges,
                                boolean isAllow, Map<String, Value> restrictions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AccessControlEntry[] getAccessControlEntries() {
            return entries.toArray(new AccessControlEntry[entries.size()]);
        }

        @Override
        public void removeAccessControlEntry(AccessControlEntry ace) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public List<JackrabbitAccessControlEntry> getEntries() {
            return entries;
        }

        @Nonnull
        @Override
        public RestrictionProvider getRestrictionProvider() {
            return restrictionProvider;
        }
    }
}