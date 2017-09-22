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
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.AbstractRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test {@code ACL} implementation.
 */
public class ACLTest extends AbstractAccessControlTest implements PrivilegeConstants, AccessControlConstants {

    private static void assertACE(JackrabbitAccessControlEntry ace, boolean isAllow, Privilege... privileges) {
        assertEquals(isAllow, ace.isAllow());
        assertEquals(Sets.newHashSet(privileges), Sets.newHashSet(ace.getPrivileges()));
    }

    @Test
    public void testGetNamePathMapper() throws Exception {
        assertSame(getNamePathMapper(), createEmptyACL().getNamePathMapper());
        assertSame(NamePathMapper.DEFAULT, createACL(TEST_PATH, ImmutableList.<ACE>of(), NamePathMapper.DEFAULT).getNamePathMapper());
    }

    @Test
    public void testGetPath() {
        NameMapper nameMapper = new GlobalNameMapper(
                Collections.singletonMap("jr", "http://jackrabbit.apache.org"));
        NamePathMapper npMapper = new NamePathMapperImpl(nameMapper);

        // map of jcr-path to standard jcr-path
        Map<String, String> paths = new HashMap<String, String>();
        paths.put(null, null);
        paths.put(TEST_PATH, TEST_PATH);
        paths.put("/", "/");
        paths.put("/jr:testPath", "/jr:testPath");
        paths.put("/{http://jackrabbit.apache.org}testPath", "/jr:testPath");

        for (String path : paths.keySet()) {
            AbstractAccessControlList acl = createACL(path, Collections.<ACE>emptyList(), npMapper);
            assertEquals(paths.get(path), acl.getPath());
        }
    }

    @Test
    public void testGetOakPath() {
        NamePathMapper npMapper = new NamePathMapperImpl(new LocalNameMapper(
                singletonMap("oak", "http://jackrabbit.apache.org"),
                singletonMap("jcr", "http://jackrabbit.apache.org")));
        // map of jcr-path to oak path
        Map<String, String> paths = new HashMap();
        paths.put(null, null);
        paths.put(TEST_PATH, TEST_PATH);
        paths.put("/", "/");
        String oakPath = "/oak:testPath";
        String jcrPath = "/jcr:testPath";
        paths.put(jcrPath, oakPath);
        jcrPath = "/{http://jackrabbit.apache.org}testPath";
        paths.put(jcrPath, oakPath);

        // test if oak-path is properly set.
        for (String path : paths.keySet()) {
            AbstractAccessControlList acl = createACL(path, Collections.<ACE>emptyList(), npMapper);
            assertEquals(paths.get(path), acl.getOakPath());
        }
    }

    @Test
    public void testEmptyAcl() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();

        assertNotNull(acl.getAccessControlEntries());
        assertNotNull(acl.getEntries());

        assertTrue(acl.getAccessControlEntries().length == 0);
        assertEquals(acl.getAccessControlEntries().length, acl.getEntries().size());
        assertEquals(0, acl.size());
        assertTrue(acl.isEmpty());
    }

    @Test
    public void testSize() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries());
        assertEquals(3, acl.size());
    }

    @Test
    public void testIsEmpty() throws RepositoryException {
        AbstractAccessControlList acl = createACL(createTestEntries());
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testGetEntries() throws RepositoryException {
        List<ACE> aces = createTestEntries();
        AbstractAccessControlList acl = createACL(TEST_PATH, aces, getNamePathMapper());

        assertNotNull(acl.getEntries());
        assertNotNull(acl.getAccessControlEntries());

        assertEquals(aces.size(), acl.getEntries().size());
        assertEquals(aces.size(), acl.getAccessControlEntries().length);
        assertTrue(acl.getEntries().containsAll(aces));
        assertTrue(Arrays.asList(acl.getAccessControlEntries()).containsAll(aces));
    }

    @Test
    public void testGetRestrictionNames() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();

        String[] restrNames = acl.getRestrictionNames();
        assertNotNull(restrNames);
        List<String> names = Lists.newArrayList(restrNames);
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(TEST_PATH)) {
            assertTrue(names.remove(getNamePathMapper().getJcrName(def.getName())));
        }
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetRestrictionType() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(TEST_PATH)) {
            int reqType = acl.getRestrictionType(getNamePathMapper().getJcrName(def.getName()));

            assertTrue(reqType > PropertyType.UNDEFINED);
            assertEquals(def.getRequiredType().tag(), reqType);
        }
    }

    @Test
    public void testGetRestrictionTypeForUnknownName() throws RepositoryException {
        AbstractAccessControlList acl = createEmptyACL();
        // for backwards compatibility getRestrictionType(String) must return
        // UNDEFINED for a unknown restriction name:
        assertEquals(PropertyType.UNDEFINED, acl.getRestrictionType("unknownRestrictionName"));
    }


    @Test
    public void testUnknownPrincipal() throws Exception {
        Principal unknownPrincipal = new InvalidTestPrincipal("unknown");
        try {
            acl.addAccessControlEntry(unknownPrincipal, privilegesFromNames(JCR_READ));
            fail("Adding an ACE with an unknown principal should fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testInternalPrincipal() throws RepositoryException {
        Principal internal = new PrincipalImpl("unknown");
        acl.addAccessControlEntry(internal, privilegesFromNames(JCR_READ));
    }

    @Test
    public void testNullPrincipal() throws Exception {

        try {
            acl.addAccessControlEntry(null, privilegesFromNames(JCR_READ));
            fail("Adding an ACE with null principal should fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testEmptyPrincipal() throws Exception {

        try {
            acl.addAccessControlEntry(new PrincipalImpl(""), privilegesFromNames(JCR_READ));
            fail("Adding an ACE with empty-named principal should fail");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testAddEntriesWithCustomPrincipal()  throws Exception {
        Principal oakPrincipal = new PrincipalImpl("anonymous");
        Principal principal = new Principal() {
            @Override
            public String getName() {
                return "anonymous";
            }
        };

        assertTrue(acl.addAccessControlEntry(oakPrincipal, privilegesFromNames(JCR_READ)));
        assertTrue(acl.addAccessControlEntry(principal, privilegesFromNames(JCR_READ_ACCESS_CONTROL)));
        assertEquals(1, acl.getAccessControlEntries().length);

        assertTrue(acl.addEntry(principal, privilegesFromNames(JCR_READ), false));
        assertEquals(2, acl.getAccessControlEntries().length);
        assertArrayEquals(privilegesFromNames(JCR_READ_ACCESS_CONTROL), acl.getAccessControlEntries()[0].getPrivileges());
    }

    @Test
    public void testAddEntryWithoutPrivilege() throws Exception {
        try {
            acl.addAccessControlEntry(testPrincipal, new Privilege[0]);
            fail("Adding an ACE with empty privilege array should fail.");
        } catch (AccessControlException e) {
            // success
        }
        try {
            acl.addAccessControlEntry(testPrincipal, null);
            fail("Adding an ACE with null privileges should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testAddEntryWithInvalidPrivilege() throws Exception {
        try {
            acl.addAccessControlEntry(testPrincipal, new Privilege[]{new InvalidPrivilege()});
            fail("Adding an ACE with invalid privileges should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testAddAccessControlEntry() throws Exception {
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testAddEntry() throws Exception {
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true));
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testAddEntry2() throws Exception {
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap()));
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testAddEntryTwice() throws Exception {
        acl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap());
        assertFalse(acl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap()));
    }

    @Test
    public void testRemoveEntry() throws Exception {
        assertTrue(acl.addAccessControlEntry(testPrincipal, testPrivileges));
        acl.removeAccessControlEntry(acl.getAccessControlEntries()[0]);
        assertTrue(acl.isEmpty());
    }

    @Test
    public void testRemoveEntries() throws Exception {
        JackrabbitAccessControlList acl = createACL(TEST_PATH, createTestEntries(), namePathMapper);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            acl.removeAccessControlEntry(ace);
        }
        assertTrue(acl.isEmpty());
    }

    @Test
    public void testRemoveInvalidEntry() throws Exception {
        try {
            acl.removeAccessControlEntry(new JackrabbitAccessControlEntry() {
                public boolean isAllow() {
                    return false;
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
            acl.removeAccessControlEntry(createEntry(testPrincipal, testPrivileges, true));
            fail("Removing a non-existing ACE should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testReorderToTheEnd() throws Exception {
        Privilege[] read = privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(JCR_WRITE);

        AbstractAccessControlList acl = createEmptyACL();
        acl.addAccessControlEntry(testPrincipal, read);
        acl.addEntry(testPrincipal, write, false);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), write);

        List<? extends JackrabbitAccessControlEntry> entries = acl.getEntries();
        assertEquals(3, entries.size());

        AccessControlEntry first = entries.get(0);
        acl.orderBefore(first, null);

        List<? extends JackrabbitAccessControlEntry> entriesAfter = acl.getEntries();
        assertEquals(first, entriesAfter.get(2));
    }

    @Test
    public void testReorder() throws Exception {
        Privilege[] read = privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(JCR_WRITE);

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
    public void testReorderInvalidEntries() throws Exception {
        Privilege[] read = privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(JCR_WRITE);

        acl.addAccessControlEntry(testPrincipal, read);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), write);

        AccessControlEntry invalid = createEntry(testPrincipal, false, null, JCR_WRITE);
        try {
            acl.orderBefore(invalid, acl.getEntries().get(0));
            fail("src entry not contained in list -> reorder should fail.");
        } catch (AccessControlException e) {
            // success
        }
        try {
            acl.orderBefore(acl.getEntries().get(0), invalid);
            fail("dest entry not contained in list -> reorder should fail.");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testMultipleEntries() throws Exception {
        Privilege[] privileges = privilegesFromNames(JCR_READ);
        acl.addEntry(testPrincipal, privileges, true);

        // new entry extends privileges.
        privileges = privilegesFromNames(JCR_READ, JCR_ADD_CHILD_NODES);
        assertTrue(acl.addEntry(testPrincipal, privileges, true));

        // expected: only a single allow-entry with both privileges
        assertTrue(acl.size() == 1);
        assertACE(acl.getEntries().get(0), true, privileges);
    }

    @Test
    public void testMultipleEntries2() throws Exception {
        Privilege[] privileges = privilegesFromNames(JCR_READ, JCR_ADD_CHILD_NODES);
        acl.addEntry(testPrincipal, privileges, true);

        // adding just ADD_CHILD_NODES -> must not remove READ privilege
        Privilege[] achPrivs = privilegesFromNames(JCR_ADD_CHILD_NODES);
        assertFalse(acl.addEntry(testPrincipal, achPrivs, true));

        // expected: only a single allow-entry with add_child_nodes + read privilege
        assertTrue(acl.size() == 1);
        assertACE(acl.getEntries().get(0), true, privileges);
    }

    @Test
    public void testComplementaryEntry() throws Exception {
        Privilege[] privileges = privilegesFromNames(JCR_READ);
        acl.addEntry(testPrincipal, privileges, true);

        // same entry but with revers 'isAllow' flag
        assertTrue(acl.addEntry(testPrincipal, privileges, false));

        // expected: only a single deny-read entry
        assertEquals(1, acl.size());
        assertACE(acl.getEntries().get(0), false, privileges);
    }

    @Test
    public void testComplementaryEntry1() throws Exception {
        Privilege[] privileges = privilegesFromNames(JCR_READ, JCR_ADD_CHILD_NODES);
        acl.addEntry(testPrincipal, privileges, true);

        // revoke the 'READ' privilege
        privileges = privilegesFromNames(JCR_READ);
        assertTrue(acl.addEntry(testPrincipal, privileges, false));

        // expected: 2 entries one allowing ADD_CHILD_NODES, the other denying READ
        assertTrue(acl.size() == 2);

        assertACE(acl.getEntries().get(0), true, privilegesFromNames(JCR_ADD_CHILD_NODES));
        assertACE(acl.getEntries().get(1), false, privilegesFromNames(JCR_READ));
    }

    @Test
    public void testComplementaryEntry2() throws Exception {
        Privilege[] repwrite = privilegesFromNames(REP_WRITE);
        acl.addAccessControlEntry(testPrincipal, repwrite);

        // add deny entry for mod_props
        Privilege[] modProperties = privilegesFromNames(JCR_MODIFY_PROPERTIES);
        assertTrue(acl.addEntry(testPrincipal, modProperties, false));

        // expected: 2 entries with the allow entry being adjusted
        assertTrue(acl.size() == 2);

        Privilege[] expected = privilegesFromNames(JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE, JCR_NODE_TYPE_MANAGEMENT);
        assertACE(acl.getEntries().get(0), true, expected);
        assertACE(acl.getEntries().get(1), false, modProperties);
    }

    @Test
    public void testComplementaryEntry3() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);

        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_WRITE));
        acl.addEntry(testPrincipal, readPriv, false);

        acl.addAccessControlEntry(testPrincipal, readPriv);

        List<? extends JackrabbitAccessControlEntry> entries = acl.getEntries();
        assertEquals(1, entries.size());
    }

    @Test
    public void testMultiplePrincipals() throws Exception {
        Principal everyone = principalManager.getEveryone();
        Privilege[] privs = privilegesFromNames(JCR_READ);

        acl.addAccessControlEntry(testPrincipal, privs);
        assertFalse(acl.addAccessControlEntry(testPrincipal, privs));

        // add same privileges for another principal -> must modify as well.
        assertTrue(acl.addAccessControlEntry(everyone, privs));
        // .. 2 entries must be present.
        assertTrue(acl.getAccessControlEntries().length == 2);
        assertEquals(everyone, acl.getAccessControlEntries()[1].getPrincipal());
    }

    @Test
    public void testSetEntryForGroupPrincipal() throws Exception {
        Privilege[] privs = privilegesFromNames(JCR_READ);
        Group grPrincipal = (Group) principalManager.getEveryone();

        // adding allow-entry must succeed
        assertTrue(acl.addAccessControlEntry(grPrincipal, privs));

        // adding deny-entry must succeed
        assertTrue(acl.addEntry(grPrincipal, privs, false));
        assertEquals(1, acl.size());
        assertFalse(acl.getEntries().get(0).isAllow());
    }

    @Test
    public void testUpdateGroupEntry() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);

        Principal everyone = principalManager.getEveryone();

        acl.addEntry(testPrincipal, readPriv, true);
        acl.addEntry(everyone, readPriv, true);
        acl.addEntry(testPrincipal, writePriv, false);

        // adding an entry that should update the existing allow-entry for everyone.
        acl.addEntry(everyone, writePriv, true);

        AccessControlEntry[] entries = acl.getAccessControlEntries();
        assertEquals(3, entries.length);
        JackrabbitAccessControlEntry princ2AllowEntry = (JackrabbitAccessControlEntry) entries[1];
        assertEquals(everyone, princ2AllowEntry.getPrincipal());
        assertACE(princ2AllowEntry, true, privilegesFromNames(JCR_READ, JCR_WRITE));
    }

    @Test
    public void testComplementaryGroupEntry() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);
        Principal everyone = principalManager.getEveryone();

        acl.addEntry(testPrincipal, readPriv, true);
        acl.addEntry(everyone, readPriv, true);
        acl.addEntry(testPrincipal, writePriv, false);
        acl.addEntry(everyone, writePriv, true);
        // entry complementary to the first entry
        // -> must remove the allow-READ entry and update the deny-WRITE entry.
        acl.addEntry(testPrincipal, readPriv, false);

        AccessControlEntry[] entries = acl.getAccessControlEntries();

        assertEquals(2, entries.length);

        JackrabbitAccessControlEntry first = (JackrabbitAccessControlEntry) entries[0];
        assertEquals(everyone, first.getPrincipal());

        JackrabbitAccessControlEntry second = (JackrabbitAccessControlEntry) entries[1];
        assertEquals(testPrincipal, second.getPrincipal());
        assertACE(second, false, privilegesFromNames(JCR_READ, JCR_WRITE));
    }

    @Test
    public void testAllowWriteDenyRemoveGroupEntries() throws Exception {
        Principal everyone = principalManager.getEveryone();
        Privilege[] grPriv = privilegesFromNames(REP_WRITE);
        Privilege[] dePriv = privilegesFromNames(JCR_REMOVE_CHILD_NODES);

        acl.addEntry(everyone, grPriv, true, Collections.<String, Value>emptyMap());
        acl.addEntry(everyone, dePriv, false, Collections.<String, Value>emptyMap());

        Set<Privilege> allows = new HashSet<Privilege>();
        Set<Privilege> denies = new HashSet<Privilege>();
        AccessControlEntry[] entries = acl.getAccessControlEntries();
        for (AccessControlEntry en : entries) {
            if (everyone.equals(en.getPrincipal()) && en instanceof JackrabbitAccessControlEntry) {
                JackrabbitAccessControlEntry ace = (JackrabbitAccessControlEntry) en;
                Privilege[] privs = ace.getPrivileges();
                if (ace.isAllow()) {
                    allows.addAll(Arrays.asList(privs));
                } else {
                    denies.addAll(Arrays.asList(privs));
                }
            }
        }

        Privilege[] expected = privilegesFromNames(JCR_ADD_CHILD_NODES, JCR_REMOVE_NODE, JCR_MODIFY_PROPERTIES, JCR_NODE_TYPE_MANAGEMENT);
        assertEquals(expected.length, allows.size());
        assertEquals(ImmutableSet.copyOf(expected), allows);

        assertEquals(1, denies.size());
        assertArrayEquals(privilegesFromNames(JCR_REMOVE_CHILD_NODES), denies.toArray(new Privilege[denies.size()]));
    }

    @Test
    public void testUpdateAndComplementary() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);
        Privilege[] acReadPriv = privilegesFromNames(JCR_READ_ACCESS_CONTROL);

        assertTrue(acl.addEntry(testPrincipal, readPriv, true));
        assertTrue(acl.addEntry(testPrincipal, writePriv, true));
        assertTrue(acl.addEntry(testPrincipal, acReadPriv, true));
        assertEquals(1, acl.size());

        assertTrue(acl.addEntry(testPrincipal, readPriv, false));
        assertEquals(2, acl.size());

        assertACE(acl.getEntries().get(0), true, privilegesFromNames(JCR_WRITE, JCR_READ_ACCESS_CONTROL));
        assertACE(acl.getEntries().get(1), false, readPriv);
    }

    @Test
    public void testDifferentPrivilegeImplementation() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        acl.addEntry(testPrincipal, readPriv, false);

        assertFalse(acl.addEntry(new PrincipalImpl(testPrincipal.getName()), readPriv, false));
        assertFalse(acl.addEntry(new Principal() {
            public String getName() {
                return testPrincipal.getName();
            }
        }, readPriv, false));
    }

    @Test
    public void testNewEntriesAppendedAtEnd() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);

        acl.addEntry(testPrincipal, readPriv, true);
        acl.addEntry(principalManager.getEveryone(), readPriv, true);
        acl.addEntry(testPrincipal, writePriv, false);

        AccessControlEntry[] entries = acl.getAccessControlEntries();

        assertEquals(3, entries.length);

        JackrabbitAccessControlEntry last = (JackrabbitAccessControlEntry) entries[2];
        assertEquals(testPrincipal, last.getPrincipal());
        assertACE(last, false, writePriv);
    }

    @Test
    public void testInsertionOrder() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);
        Privilege[] addNodePriv = privilegesFromNames(JCR_ADD_CHILD_NODES);

        Map<String, Value> restrictions = Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/.*"));

        acl.addEntry(testPrincipal, readPriv, true);
        acl.addEntry(testPrincipal, writePriv, false);
        acl.addEntry(testPrincipal, addNodePriv, true, restrictions);

        List<? extends JackrabbitAccessControlEntry> entries = acl.getEntries();
        assertACE(entries.get(0), true, readPriv);
        assertACE(entries.get(1), false, writePriv);
        assertACE(entries.get(2), true, addNodePriv);
    }

    @Test
    public void testInsertionOrder2() throws Exception {
        Privilege[] readPriv = privilegesFromNames(JCR_READ);
        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);
        Privilege[] addNodePriv = privilegesFromNames(JCR_ADD_CHILD_NODES);

        Map<String, Value> restrictions = Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/.*"));

        acl.addEntry(testPrincipal, readPriv, true);
        acl.addEntry(testPrincipal, addNodePriv, true, restrictions);
        acl.addEntry(testPrincipal, writePriv, false);

        List<? extends JackrabbitAccessControlEntry> entries = acl.getEntries();
        assertACE(entries.get(0), true, readPriv);
        assertACE(entries.get(1), true, addNodePriv);
        assertACE(entries.get(2), false, writePriv);
    }

    @Test
    public void testRestrictions() throws Exception {
        String[] names = acl.getRestrictionNames();
        assertNotNull(names);
        assertEquals(4, names.length);
        assertArrayEquals(new String[] {REP_GLOB, REP_NT_NAMES, REP_PREFIXES, REP_ITEM_NAMES}, names);
        assertEquals(PropertyType.STRING, acl.getRestrictionType(names[0]));
        assertEquals(PropertyType.NAME, acl.getRestrictionType(names[1]));
        assertEquals(PropertyType.STRING, acl.getRestrictionType(names[2]));
        assertEquals(PropertyType.NAME, acl.getRestrictionType(names[3]));

        Privilege[] writePriv = privilegesFromNames(JCR_WRITE);

        // add entry without restr. -> must succeed
        assertTrue(acl.addAccessControlEntry(testPrincipal, writePriv));
        assertEquals(1, acl.getAccessControlEntries().length);

        // ... again -> no modification.
        assertFalse(acl.addAccessControlEntry(testPrincipal, writePriv));
        assertEquals(1, acl.getAccessControlEntries().length);

        // ... again using different method -> no modification.
        assertFalse(acl.addEntry(testPrincipal, writePriv, true));
        assertEquals(1, acl.getAccessControlEntries().length);

        // ... complementary entry -> must modify the acl
        assertTrue(acl.addEntry(testPrincipal, writePriv, false));
        assertEquals(1, acl.getAccessControlEntries().length);

        // add an entry with a restrictions:
        Map<String, Value> restrictions = Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/.*"));
        assertTrue(acl.addEntry(testPrincipal, writePriv, false, restrictions));
        assertEquals(2, acl.getAccessControlEntries().length);

        // ... same again -> no modification.
        assertFalse(acl.addEntry(testPrincipal, writePriv, false, restrictions));
        assertEquals(2, acl.getAccessControlEntries().length);

        // ... complementary entry -> must modify the acl.
        assertTrue(acl.addEntry(testPrincipal, writePriv, true, restrictions));
        assertEquals(2, acl.getAccessControlEntries().length);
    }

    @Test
    public void testMvRestrictions() throws Exception {

        ValueFactory vf = getValueFactory();
        Value[] vs = new Value[] {
                vf.createValue(JcrConstants.NT_FILE, PropertyType.NAME),
                vf.createValue(JcrConstants.NT_FOLDER, PropertyType.NAME)
        };
        Map<String, Value[]> mvRestrictions = Collections.singletonMap(REP_NT_NAMES, vs);
        Map<String, Value> restrictions = Collections.singletonMap(REP_GLOB, vf.createValue("/.*"));

        assertTrue(acl.addEntry(testPrincipal, testPrivileges, false, restrictions, mvRestrictions));
        assertFalse(acl.addEntry(testPrincipal, testPrivileges, false, restrictions, mvRestrictions));
        assertEquals(1, acl.getAccessControlEntries().length);
        JackrabbitAccessControlEntry ace = (JackrabbitAccessControlEntry) acl.getAccessControlEntries()[0];
        try {
            ace.getRestriction(REP_NT_NAMES);
            fail();
        } catch (ValueFormatException e) {
            // success
        }
        Value[] vvs = ace.getRestrictions(REP_NT_NAMES);
        assertArrayEquals(vs, vvs);
    }

    @Test
    public void testUnsupportedRestrictions() throws Exception {
        Map<String, Value> restrictions = Collections.singletonMap("unknownRestriction", getValueFactory().createValue("value"));
        try {
            acl.addEntry(testPrincipal, testPrivileges, false, restrictions);
            fail("Invalid restrictions -> AccessControlException expected");
        } catch (AccessControlException e) {
            // success
        }
    }

    @Test
    public void testUnsupportedRestrictions2() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("restr", Type.NAME, false);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList(), namePathMapper, rp);
        try {
            acl.addEntry(testPrincipal, testPrivileges, false, Collections.<String, Value>singletonMap("unsupported", getValueFactory().createValue("value")));
            fail("Unsupported restriction must be detected.");
        } catch (AccessControlException e) {
            // mandatory restriction missing -> success
        }
    }

    @Test
    public void testInvalidRestrictionType() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("restr", Type.NAME, false);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList(), namePathMapper, rp);
        try {
            acl.addEntry(testPrincipal, testPrivileges, false, Collections.<String, Value>singletonMap("restr", getValueFactory().createValue(true)));
            fail("Invalid restriction type.");
        } catch (AccessControlException e) {
            // mandatory restriction missing -> success
        }
    }

    @Test
    public void testMandatoryRestrictions() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAME, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList(), namePathMapper, rp);
        try {
            acl.addEntry(testPrincipal, testPrivileges, false, Collections.<String, Value>emptyMap());
            fail("Mandatory restriction must be enforced.");
        } catch (AccessControlException e) {
            // mandatory restriction missing -> success
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

    private final class TestRestrictionProvider extends AbstractRestrictionProvider {

        private TestRestrictionProvider(String name, Type type, boolean isMandatory) {
            super(Collections.singletonMap(name, new RestrictionDefinitionImpl(name, type, isMandatory)));
        }

        @Nonnull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Tree tree) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @Nonnull Set<Restriction> restrictions) {
            throw new UnsupportedOperationException();
        }
    }
}