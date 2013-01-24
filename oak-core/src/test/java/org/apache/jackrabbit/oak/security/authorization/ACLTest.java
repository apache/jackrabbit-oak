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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlListTest;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ACLTest... TODO
 *
 * TODO: test restrictions
 * TODO: add test with multiple entries
 */
public class ACLTest extends AbstractAccessControlListTest{

    private PrivilegeManager privilegeManager;
    private PrincipalManager principalManager;

    private AbstractAccessControlList emptyAcl;
    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        privilegeManager = getPrivilegeManager();
        principalManager = getSecurityProvider().getPrincipalConfiguration().getPrincipalManager(root, getNamePathMapper());

        emptyAcl = createEmptyACL();
        testPrincipal = getTestPrincipal();
        testPrivileges = privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_LOCK_MANAGEMENT);
    }

    @Override
    protected AbstractAccessControlList createACL(String jcrPath, List<JackrabbitAccessControlEntry> entries, NamePathMapper namePathMapper) {
        String path = (jcrPath == null) ? null : namePathMapper.getOakPathKeepIndex(jcrPath);
        final RestrictionProvider rp = getRestrictionProvider();
        return new ACL(path, entries, namePathMapper) {
            @Override
            public RestrictionProvider getRestrictionProvider() {
                return rp;
            }
            @Override
            PrincipalManager getPrincipalManager() {
                return principalManager;
            }
            @Override
            PrivilegeManager getPrivilegeManager() {
                return privilegeManager;
            }
        };
    }

    @Test
    public void testAddInvalidEntry() throws Exception {
        Principal unknownPrincipal = new PrincipalImpl("unknown");
        try {
            emptyAcl.addAccessControlEntry(unknownPrincipal, privilegesFromNames(Privilege.JCR_READ));
            fail("Adding an ACE with an unknown principal should fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testAddEntryWithoutPrivilege() throws Exception {
        try {
            emptyAcl.addAccessControlEntry(testPrincipal, new Privilege[0]);
            fail("Adding an ACE with empty privilege array should fail.");
        } catch (AccessControlException e) {
            // success
        }
        try {
            emptyAcl.addAccessControlEntry(testPrincipal, null);
            fail("Adding an ACE with null privileges should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testAddEntryWithInvalidPrivilege() throws Exception {
        try {
            emptyAcl.addAccessControlEntry(testPrincipal, new Privilege[] {new InvalidPrivilege()});
            fail("Adding an ACE with invalid privileges should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testAddEntry() throws Exception {
        assertTrue(emptyAcl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap()));
        assertFalse(emptyAcl.isEmpty());
    }

    @Test
    public void testAddEntryTwice() throws Exception {
        emptyAcl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap());
        assertFalse(emptyAcl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap()));
    }

    @Test
    public void testAddEntryWithInvalidRestrictions() throws Exception {
        Map<String,Value> restrictions = Collections.singletonMap("unknownRestriction", new ValueFactoryImpl(root.getBlobFactory(), namePathMapper).createValue("value"));
        try {
            emptyAcl.addEntry(testPrincipal, testPrivileges, false, restrictions);
            fail("Invalid restrictions -> AccessControlException expected");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testRemoveEntry() throws Exception {
        assertTrue(emptyAcl.addAccessControlEntry(testPrincipal, testPrivileges));
        emptyAcl.removeAccessControlEntry(emptyAcl.getAccessControlEntries()[0]);
        assertTrue(emptyAcl.isEmpty());
    }

    @Test
    public void testRemoveEntries() throws Exception {
        JackrabbitAccessControlList acl = createACL(getTestPath(), createTestEntries(), namePathMapper);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            acl.removeAccessControlEntry(ace);
        }
        assertTrue(acl.isEmpty());
    }

    @Test
    public void testRemoveInvalidEntry() throws Exception {
        try {
            emptyAcl.removeAccessControlEntry(new JackrabbitAccessControlEntry() {
                public boolean isAllow() {
                    return false;
                }
                public String[] getRestrictionNames() {
                    return new String[0];
                }
                public Value getRestriction(String restrictionName) {
                    return null;
                }
                public Principal getPrincipal() {
                    return testPrincipal;
                }

                public Privilege[] getPrivileges() {
                    return testPrivileges;
                }
            });
            fail("Passing an unknown ACE should fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testRemoveNonExisting() throws Exception {
        try {
            emptyAcl.removeAccessControlEntry(new ACE(testPrincipal, testPrivileges, true, null));
            fail("Removing a non-existing ACE should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testReorderToTheEnd() throws Exception {
        Privilege[] read = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(PrivilegeConstants.JCR_WRITE);

        AbstractAccessControlList acl = createEmptyACL();
        acl.addAccessControlEntry(testPrincipal, read);
        acl.addEntry(testPrincipal, write, false);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), write);

        List<JackrabbitAccessControlEntry> entries = acl.getEntries();
        assertEquals(3, entries.size());

        AccessControlEntry first = entries.get(0);
        acl.orderBefore(first, null);

        List<JackrabbitAccessControlEntry> entriesAfter = acl.getEntries();
        assertEquals(first, entriesAfter.get(2));
    }

    @Test
    public void testReorder() throws Exception {
        Privilege[] read = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(PrivilegeConstants.JCR_WRITE);

        AbstractAccessControlList acl = createEmptyACL();
        acl.addAccessControlEntry(testPrincipal, read);
        acl.addEntry(testPrincipal, write, false);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), write);

        AccessControlEntry[] entries = acl.getAccessControlEntries();

        assertEquals(3, entries.length);
        AccessControlEntry first = entries[0];
        AccessControlEntry second = entries[1];
        AccessControlEntry third = entries[2];

        // reorder 'second' to the first position
        acl.orderBefore(second, first);
        assertEquals(second, acl.getEntries().get(0));
        assertEquals(first, acl.getEntries().get(1));
        assertEquals(third, acl.getEntries().get(2));

        // reorder 'third' before 'first'
        acl.orderBefore(third, first);
        assertEquals(second, acl.getEntries().get(0));
        assertEquals(third, acl.getEntries().get(1));
        assertEquals(first, acl.getEntries().get(2));
    }

    @Test
    public void testReorderInvalidElements() throws Exception {
        Privilege[] read = privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(PrivilegeConstants.JCR_WRITE);

        emptyAcl.addAccessControlEntry(testPrincipal, read);
        emptyAcl.addAccessControlEntry(EveryonePrincipal.getInstance(), write);

        AccessControlEntry invalid = new ACE(testPrincipal, write, false, Collections.<Restriction>emptySet());
        try {
            emptyAcl.orderBefore(invalid, emptyAcl.getEntries().get(0));
            fail("src entry not contained in list -> reorder should fail.");
        } catch (AccessControlException e) {
            // success
        }
        try {
            emptyAcl.orderBefore(emptyAcl.getEntries().get(0), invalid);
            fail("dest entry not contained in list -> reorder should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    //--------------------------------------------------------------------------

    private class InvalidPrivilege implements Privilege {

        @Override
        public String getName() {
            return "invalidPrivilege";
        }

        @Override
        public boolean isAbstract() {
            return false;
        }

        @Override
        public boolean isAggregate() {
            return false;
        }

        @Override
        public Privilege[] getDeclaredAggregatePrivileges() {
            return new Privilege[0];
        }

        @Override
        public Privilege[] getAggregatePrivileges() {
            return new Privilege[0];
        }
    }
}