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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EntryTest extends AbstractAccessControlTest {

    private AccessControlManager acMgr;

    private Value globValue;
    private Value[] nameValues;
    private Value nameValue;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        acMgr = getAccessControlManager(root);
        testPrincipal = new Principal() {
            public String getName() {
                return "TestPrincipal";
            }
        };
        ValueFactory valueFactory = getValueFactory(root);
        globValue = valueFactory.createValue("*");
        nameValue = valueFactory.createValue("nt:file", PropertyType.NAME);
        nameValues = new Value[] {
                valueFactory.createValue("nt:folder", PropertyType.NAME),
                valueFactory.createValue("nt:file", PropertyType.NAME)
        };
    }

    private ACE createEntry(String... privilegeNames)
            throws RepositoryException {
        return createEntry(testPrincipal, true, null, privilegeNames);
    }

    private ACE createEntry(String[] privilegeNames, boolean isAllow)
            throws RepositoryException {
        return createEntry(testPrincipal, isAllow, null, privilegeNames);
    }

    private ACE createEntry(Set<Restriction> restrictions) throws Exception {
        return createEntry(testPrincipal, true, restrictions, PrivilegeConstants.JCR_READ);
    }

    private Restriction createRestriction(String name, Value value) throws Exception {
        return getRestrictionProvider().createRestriction("/a/b/c", name, value);
    }

    private Restriction createRestriction(String name, Value[] values) throws Exception {
        return getRestrictionProvider().createRestriction("/a/b/c", name, values);
    }

    @Test
    public void testIsAllow() throws RepositoryException {
        ACE ace = createEntry(new String[]{PrivilegeConstants.JCR_READ}, true);
        assertTrue(ace.isAllow());

        ace = createEntry(new String[]{PrivilegeConstants.JCR_READ}, false);
        assertFalse(ace.isAllow());
    }

    @Test
    public void testGetPrincipal() throws RepositoryException {
        ACE tmpl = createEntry(new String[]{PrivilegeConstants.JCR_READ}, true);
        assertNotNull(tmpl.getPrincipal());
        assertEquals(testPrincipal.getName(), tmpl.getPrincipal().getName());
        assertSame(testPrincipal, tmpl.getPrincipal());
    }

    @Test
    public void testNullPrincipal() throws Exception {
        try {
            Privilege[] privs = new Privilege[]{
                    acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL)
            };
            createEntry(null, privs, true);
            fail("Principal must not be null");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testGetPrivileges() throws RepositoryException {
        ACE entry = createEntry(new String[]{PrivilegeConstants.JCR_READ}, true);

        Privilege[] privs = entry.getPrivileges();
        assertNotNull(privs);
        assertEquals(1, privs.length);
        assertEquals(privs[0], acMgr.privilegeFromName(PrivilegeConstants.JCR_READ));

        entry = createEntry(new String[]{PrivilegeConstants.REP_WRITE}, true);
        privs = entry.getPrivileges();
        assertNotNull(privs);
        assertEquals(1, privs.length);
        assertEquals(privs[0], acMgr.privilegeFromName(PrivilegeConstants.REP_WRITE));

        entry = createEntry(new String[]{PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES}, true);
        privs = entry.getPrivileges();
        assertNotNull(privs);
        assertEquals(2, privs.length);

        Privilege[] expected = AccessControlUtils.privilegesFromNames(acMgr,
                PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES);
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(privs));
    }

    @Test
    public void testGetPrivilegeBits() throws RepositoryException {
        ACE entry = createEntry(new String[]{PrivilegeConstants.JCR_READ}, true);

        PrivilegeBits bits = entry.getPrivilegeBits();
        assertNotNull(bits);
        assertEquals(bits, getBitsProvider().getBits(PrivilegeConstants.JCR_READ));

        entry = createEntry(new String[]{PrivilegeConstants.REP_WRITE}, true);
        bits = entry.getPrivilegeBits();
        assertNotNull(bits);
        assertEquals(bits, getBitsProvider().getBits(PrivilegeConstants.REP_WRITE));

        entry = createEntry(new String[]{PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES}, true);
        bits = entry.getPrivilegeBits();
        assertNotNull(bits);

        PrivilegeBits expected = getBitsProvider().getBits(
                PrivilegeConstants.JCR_ADD_CHILD_NODES,
                PrivilegeConstants.JCR_REMOVE_CHILD_NODES);
        assertEquals(expected, bits);
    }

    @Test(expected = AccessControlException.class)
    public void testNullPrivileges() throws Exception {
        new EmptyACE(null);
    }

    @Test(expected = AccessControlException.class)
    public void testEmptyPrivileges() throws Exception {
        new EmptyACE(PrivilegeBits.EMPTY);
    }

    @Test
    public void testRedundantPrivileges() throws Exception {
        ACE ace = createEntry(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ);
        assertEquals(getBitsProvider().getBits(PrivilegeConstants.JCR_READ), ace.getPrivilegeBits());
    }

    /**
     * @since oak 1.0 ACE doesn't validate privileges.
     */
    @Test
    public void testUnknownPrivilege() throws Exception {
        Privilege invalidPriv = new Privilege() {
            public String getName() {
                return "";
            }

            public boolean isAbstract() {
                return false;
            }

            public boolean isAggregate() {
                return false;
            }

            public Privilege[] getDeclaredAggregatePrivileges() {
                return new Privilege[0];
            }

            public Privilege[] getAggregatePrivileges() {
                return new Privilege[0];
            }
        };
        Privilege[] privs = new Privilege[]{invalidPriv, acMgr.privilegeFromName(PrivilegeConstants.JCR_READ)};
        ACE entry = createEntry(testPrincipal, privs, true);
        assertEquals(getBitsProvider().getBits(PrivilegeConstants.JCR_READ), entry.getPrivilegeBits());
    }

    @Test
    public void testAggregatePrivileges() throws Exception {
        ACE ace = createEntry(PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES);

        assertEquals(getBitsProvider().getBits(PrivilegeConstants.JCR_READ), ace.getPrivilegeBits());
        assertArrayEquals(privilegesFromNames(PrivilegeConstants.JCR_READ), ace.getPrivileges());
    }

    @Test
    public void testGetRestrictionNames() throws Exception {
        // empty restrictions
        String[] restrictionNames = createEntry(Collections.<Restriction>emptySet()).getRestrictionNames();
        assertNotNull(restrictionNames);
        assertEquals(0, restrictionNames.length);

        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);

        // single restriction
        restrictionNames = createEntry(ImmutableSet.of(globRestr)).getRestrictionNames();
        assertEquals(1, restrictionNames.length);

        // 2 restrictions
        restrictionNames = createEntry(ImmutableSet.of(globRestr, nameRestr)).getRestrictionNames();
        assertEquals(2, restrictionNames.length);
    }

    @Test
    public void testGetRestrictionForEmpty() throws Exception {
        // empty restrictions
        Value val = createEntry(Collections.<Restriction>emptySet()).getRestriction(AccessControlConstants.REP_GLOB);
        assertNull(val);
    }

    @Test
    public void testGetNonExistingRestriction() throws Exception {
        // single valued restriction
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        ACE ace = createEntry(ImmutableSet.of(globRestr));
        assertNull(ace.getRestriction(AccessControlConstants.REP_NT_NAMES));
    }

    @Test
    public void testGetRestrictionForSingleValued() throws Exception {
        // single valued restriction
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        ACE ace = createEntry(ImmutableSet.of(globRestr));
        Value val = ace.getRestriction(AccessControlConstants.REP_GLOB);
        assertNotNull(val);
        assertEquals(globValue, val);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test(expected = ValueFormatException.class)
    public void testGetRestrictionForMultiValued() throws Exception {
        // multivalued restriction
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        ACE ace = createEntry(ImmutableSet.of(nameRestr));

        ace.getRestriction(AccessControlConstants.REP_NT_NAMES);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionForMultiValued2() throws Exception {
        // single value restriction stored in multi-value property
        Restriction singleNameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, new Value[] {nameValue});

        ACE ace = createEntry(ImmutableSet.of(singleNameRestr));
        Value val = ace.getRestriction(AccessControlConstants.REP_NT_NAMES);
        assertEquals(nameValue, val);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetEmptyRestrictions() throws Exception {
        // empty restrictions
        Value[] vs = createEntry(Collections.<Restriction>emptySet()).getRestrictions(AccessControlConstants.REP_GLOB);
        assertNull(vs);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetNonExistingRestrictions() throws Exception {
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        ACE ace = createEntry(ImmutableSet.of(nameRestr));
        assertNull(ace.getRestrictions(AccessControlConstants.REP_GLOB));
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionsForSingleValue() throws Exception {
        // single valued restriction
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);
        ACE ace = createEntry(ImmutableSet.of(globRestr));
        Value[] vs = ace.getRestrictions(AccessControlConstants.REP_GLOB);
        assertNotNull(vs);
        assertArrayEquals(new Value[]{globValue}, vs);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionsForMultiValued() throws Exception {
        // multivalued restriction
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        ACE ace = createEntry(ImmutableSet.of(nameRestr));
        Value[] vs = ace.getRestrictions(AccessControlConstants.REP_NT_NAMES);
        assertEquals(2, vs.length);
        assertArrayEquals(nameValues, vs);
    }

    /**
     * @since OAK 1.0: support for multi-value restrictions
     */
    @Test
    public void testGetRestrictionsForMultiValued2() throws Exception {
        // single value restriction stored in multi-value property
        Restriction singleNameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, new Value[]{nameValue});
        ACE ace = createEntry(ImmutableSet.of(singleNameRestr));
        Value[] vs = ace.getRestrictions(AccessControlConstants.REP_NT_NAMES);
        assertEquals(1, vs.length);
        assertEquals(nameValue, vs[0]);
    }

    @Test
    public void testGetRestrictions() throws Exception {
        Restriction nameRestr = createRestriction(AccessControlConstants.REP_NT_NAMES, nameValues);
        Restriction globRestr = createRestriction(AccessControlConstants.REP_GLOB, globValue);

        Set<Restriction> expected = ImmutableSet.of(nameRestr, globRestr);
        ACE ace = createEntry(expected);

        assertEquals(expected, ace.getRestrictions());
    }

    @Test
    public void testGetRestrictionsNone() throws Exception {
        ACE ace = createEntry(ImmutableSet.<Restriction>of());

        assertTrue(ace.getRestrictions().isEmpty());
    }

    @Test
    public void testEquals() throws RepositoryException {

        Map<AccessControlEntry, AccessControlEntry> equalAces = new HashMap<AccessControlEntry, AccessControlEntry>();

        ACE ace = createEntry(PrivilegeConstants.JCR_ALL);
        // create same entry again
        equalAces.put(ace, createEntry(PrivilegeConstants.JCR_ALL));

        // create entry with declared aggregate privileges
        Privilege[] declaredAllPrivs = acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL).getDeclaredAggregatePrivileges();
        equalAces.put(ace, createEntry(testPrincipal, declaredAllPrivs, true));

        // create entry with aggregate privileges
        Privilege[] aggregateAllPrivs = acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL).getAggregatePrivileges();
        equalAces.put(ace, createEntry(testPrincipal, aggregateAllPrivs, true));

        // create entry with different privilege order
        List<Privilege> reordered = new ArrayList<Privilege>(Arrays.asList(aggregateAllPrivs));
        reordered.add(reordered.remove(0));
        equalAces.put(createEntry(testPrincipal, reordered.toArray(new Privilege[reordered.size()]), true),
                createEntry(testPrincipal, aggregateAllPrivs, true));

        // even if entries are build with aggregated or declared aggregate privileges
        equalAces.put(createEntry(testPrincipal, declaredAllPrivs, true),
                createEntry(testPrincipal, aggregateAllPrivs, true));

        for (AccessControlEntry entry : equalAces.keySet()) {
            assertEquals(entry, equalAces.get(entry));
        }
    }

    @Test
    public void testEquals2() throws RepositoryException {
        ACE ace = createEntry(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_READ);
        // priv array contains duplicates
        ACE ace2 = createEntry(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_READ);

        assertEquals(ace, ace2);
    }

    @Test
    public void testNotEquals() throws RepositoryException {
        ACE ace = createEntry(new String[]{PrivilegeConstants.JCR_ALL}, true);
        List<JackrabbitAccessControlEntry> otherAces = new ArrayList<JackrabbitAccessControlEntry>();

        try {
            // ACE template with different principal
            Principal princ = new Principal() {
                public String getName() {
                    return "a name";
                }
            };
            Privilege[] privs = new Privilege[]{
                    acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL)
            };
            otherAces.add(createEntry(princ, privs, true));
        } catch (RepositoryException e) {
        }

        // ACE template with different privileges
        try {
            otherAces.add(createEntry(new String[]{PrivilegeConstants.JCR_READ}, true));
        } catch (RepositoryException e) {
        }
        // ACE template with different 'allow' flag
        try {
            otherAces.add(createEntry(new String[]{PrivilegeConstants.JCR_ALL}, false));
        } catch (RepositoryException e) {
        }
        // ACE template with different privileges and 'allows
        try {
            otherAces.add(createEntry(new String[]{PrivilegeConstants.REP_WRITE}, false));
        } catch (RepositoryException e) {
        }

        // other ace impl
        final Privilege[] privs = new Privilege[]{
                acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL)
        };

        JackrabbitAccessControlEntry pe = new JackrabbitAccessControlEntry() {
            public boolean isAllow() {
                return true;
            }

            public String[] getRestrictionNames() {
                return new String[0];
            }

            public Value getRestriction(String restrictionName) {
                return null;
            }

            public Value[] getRestrictions(String restrictionName) {
                return null;
            }

            public Principal getPrincipal() {
                return testPrincipal;
            }

            public Privilege[] getPrivileges() {
                return privs;
            }
        };
        otherAces.add(pe);

        for (JackrabbitAccessControlEntry otherAce : otherAces) {
            assertFalse(ace.equals(otherAce));
        }
    }

    @Test
    public void testHashCode() throws RepositoryException {
        JackrabbitAccessControlEntry ace = createEntry(PrivilegeConstants.JCR_ALL);
        Privilege[] declaredAllPrivs = acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL).getDeclaredAggregatePrivileges();
        Privilege[] aggregateAllPrivs = acMgr.privilegeFromName(PrivilegeConstants.JCR_ALL).getAggregatePrivileges();
        List<Privilege> l = Lists.newArrayList(aggregateAllPrivs);
        l.add(l.remove(0));
        Privilege[] reordered = l.toArray(new Privilege[l.size()]);

        Map<AccessControlEntry, AccessControlEntry> equivalent = new HashMap<AccessControlEntry, AccessControlEntry>();
        // create same entry again
        equivalent.put(ace, createEntry(PrivilegeConstants.JCR_ALL));
        // create entry with duplicate privs
        equivalent.put(ace, createEntry(PrivilegeConstants.JCR_ALL, PrivilegeConstants.JCR_ALL));
        // create entry with declared aggregate privileges
        equivalent.put(ace, createEntry(testPrincipal, declaredAllPrivs, true));
        // create entry with aggregate privileges
        equivalent.put(ace, createEntry(testPrincipal, aggregateAllPrivs, true));
        // create entry with different privilege order
        equivalent.put(ace, createEntry(testPrincipal, reordered, true));
        equivalent.put(createEntry(testPrincipal, declaredAllPrivs, true),
                createEntry(testPrincipal, reordered, true));
        // even if entries are build with aggregated or declared aggregate privileges
        equivalent.put(createEntry(testPrincipal, declaredAllPrivs, true),
                createEntry(testPrincipal, aggregateAllPrivs, true));

        for (AccessControlEntry entry : equivalent.keySet()) {
            AccessControlEntry eqv = equivalent.get(entry);
            assertEquals(entry.hashCode(), eqv.hashCode());
        }
    }

    @Test
    public void testHashCode2() throws Exception {
        JackrabbitAccessControlEntry ace = createEntry(new String[]{PrivilegeConstants.JCR_ALL}, true);
        final Privilege[] privs = AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_ALL);

        // and the opposite:
        List<JackrabbitAccessControlEntry> otherAces = new ArrayList<JackrabbitAccessControlEntry>();
        // ACE template with different principal
        Principal princ = new Principal() {
            public String getName() {
                return "a name";
            }
        };
        otherAces.add(createEntry(princ, privs, true));

        // ACE template with different privileges
        otherAces.add(createEntry(new String[]{PrivilegeConstants.JCR_READ}, true));

        // ACE template with different 'allow' flag
        otherAces.add(createEntry(new String[]{PrivilegeConstants.JCR_ALL}, false));

        // ACE template with different privileges and 'allows
        otherAces.add(createEntry(new String[]{PrivilegeConstants.REP_WRITE}, false));

        // other ace impl
        JackrabbitAccessControlEntry pe = new JackrabbitAccessControlEntry() {
            public boolean isAllow() {
                return true;
            }

            public String[] getRestrictionNames() {
                return new String[0];
            }

            public Value getRestriction(String restrictionName) {
                return null;
            }

            public Value[] getRestrictions(String restrictionName) {
                return null;
            }

            public Principal getPrincipal() {
                return testPrincipal;
            }

            public Privilege[] getPrivileges() {
                return privs;
            }
        };
        otherAces.add(pe);

        for (JackrabbitAccessControlEntry otherAce : otherAces) {
            assertFalse(ace.hashCode() == otherAce.hashCode());
        }

    }

    private class EmptyACE extends ACE {

        public EmptyACE(PrivilegeBits privilegeBits) throws AccessControlException {
            super(testPrincipal, privilegeBits, true, null, namePathMapper);
        }

        @Override
        public Privilege[] getPrivileges() {
            return new Privilege[0];
        }
    }
}