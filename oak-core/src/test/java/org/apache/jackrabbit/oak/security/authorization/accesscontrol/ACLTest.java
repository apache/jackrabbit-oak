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
import java.util.HashSet;
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
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test {@code ACL} implementation.
 */
public class ACLTest extends AbstractAccessControlTest implements PrivilegeConstants, AccessControlConstants {
    
    private ACL acl;
    
    @Override
    public void before() throws Exception {
        super.before();
        acl = createACL(TEST_PATH, Collections.emptyList(), getNamePathMapper());
    }

    private static void assertACE(@NotNull JackrabbitAccessControlEntry ace, boolean isAllow, @NotNull Privilege... privileges) {
        assertEquals(isAllow, ace.isAllow());
        assertEquals(CollectionUtils.toSet(privileges), CollectionUtils.toSet(ace.getPrivileges()));
    }

    @NotNull
    private ACL createACL(@Nullable String jcrPath,
                  @NotNull List<ACE> entries,
                  @NotNull NamePathMapper namePathMapper) {
        return createACL(jcrPath, entries, namePathMapper, getRestrictionProvider());
    }
    
    @NotNull
    private List<ACE> createTestEntries() throws RepositoryException {
        List<ACE> entries = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            entries.add(createEntry(new PrincipalImpl("testPrincipal" + i), true, null, PrivilegeConstants.JCR_READ));
        }
        return entries;
    }

    @Test
    public void testGetNamePathMapper() {
        assertSame(getNamePathMapper(), acl.getNamePathMapper());
        assertSame(NamePathMapper.DEFAULT, createACL(TEST_PATH, ImmutableList.of(), NamePathMapper.DEFAULT).getNamePathMapper());
    }

    @Test
    public void testGetPath() {
        NameMapper nameMapper = new GlobalNameMapper(
                Collections.singletonMap("jr", "http://jackrabbit.apache.org"));
        NamePathMapper npMapper = new NamePathMapperImpl(nameMapper);

        // map of jcr-path to standard jcr-path
        Map<String, String> paths = new HashMap<>();
        paths.put(null, null);
        paths.put(TEST_PATH, TEST_PATH);
        paths.put("/", "/");
        paths.put("/jr:testPath", "/jr:testPath");
        paths.put("/{http://jackrabbit.apache.org}testPath", "/jr:testPath");

        paths.forEach((key, value) -> {
            AbstractAccessControlList acl = createACL(key, Collections.emptyList(), npMapper);
            assertEquals(value, acl.getPath());
        });
    }

    @Test
    public void testGetOakPath() {
        NamePathMapper npMapper = new NamePathMapperImpl(new LocalNameMapper(
                singletonMap("oak", "http://jackrabbit.apache.org"),
                singletonMap("jcr", "http://jackrabbit.apache.org")));
        // map of jcr-path to oak path
        Map<String, String> paths = new HashMap<>();
        paths.put(null, null);
        paths.put(TEST_PATH, TEST_PATH);
        paths.put("/", "/");
        String oakPath = "/oak:testPath";
        String jcrPath = "/jcr:testPath";
        paths.put(jcrPath, oakPath);
        jcrPath = "/{http://jackrabbit.apache.org}testPath";
        paths.put(jcrPath, oakPath);

        // test if oak-path is properly set.
        paths.forEach((key, value) -> {
            AbstractAccessControlList acl = createACL(key, Collections.emptyList(), npMapper);
            assertEquals(value, acl.getOakPath());
        });
    }

    @Test
    public void testEmptyAcl() {
        assertNotNull(acl.getAccessControlEntries());
        assertNotNull(acl.getEntries());

        assertEquals(0, acl.getAccessControlEntries().length);
        assertEquals(acl.getAccessControlEntries().length, acl.getEntries().size());
        assertEquals(0, acl.size());
        assertTrue(acl.isEmpty());
    }

    @Test
    public void testSize() throws RepositoryException {
        AbstractAccessControlList acl = createACL(TEST_PATH, createTestEntries(), getNamePathMapper());
        assertEquals(3, acl.size());
    }

    @Test
    public void testIsEmpty() throws RepositoryException {
        AbstractAccessControlList acl = createACL(TEST_PATH, createTestEntries(), getNamePathMapper());
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
    public void testGetRestrictionNames() {
        String[] restrNames = acl.getRestrictionNames();
        assertNotNull(restrNames);
        List<String> names = new ArrayList<>(Arrays.asList(restrNames));
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(TEST_PATH)) {
            assertTrue(names.remove(getNamePathMapper().getJcrName(def.getName())));
        }
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetRestrictionType() {
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(TEST_PATH)) {
            int reqType = acl.getRestrictionType(getNamePathMapper().getJcrName(def.getName()));

            assertTrue(reqType > PropertyType.UNDEFINED);
            assertEquals(def.getRequiredType().tag(), reqType);
        }
    }

    @Test
    public void testGetRestrictionTypeForUnknownName() {
        // for backwards compatibility getRestrictionType(String) must return
        // UNDEFINED for a unknown restriction name:
        assertEquals(PropertyType.UNDEFINED, acl.getRestrictionType("unknownRestrictionName"));
    }

    @Test
    public void testAddEntriesWithCustomPrincipal() throws Exception {
        Principal oakPrincipal = new PrincipalImpl("anonymous");
        Principal principal = () -> "anonymous";

        assertTrue(acl.addAccessControlEntry(oakPrincipal, privilegesFromNames(JCR_READ)));
        assertTrue(acl.addAccessControlEntry(principal, privilegesFromNames(JCR_READ_ACCESS_CONTROL)));
        assertEquals(1, acl.getAccessControlEntries().length);

        assertTrue(acl.addEntry(principal, privilegesFromNames(JCR_READ), false));
        assertEquals(2, acl.getAccessControlEntries().length);
        assertArrayEquals(privilegesFromNames(JCR_READ_ACCESS_CONTROL), acl.getAccessControlEntries()[0].getPrivileges());
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithoutPrivilege() throws Exception {
        acl.addAccessControlEntry(testPrincipal, new Privilege[0]);
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithNullPrivilege() throws Exception {
        acl.addAccessControlEntry(testPrincipal, null);
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithInvalidPrivilege() throws Exception {
        Privilege invalid = when(mock(Privilege.class).getName()).thenReturn("invalid").getMock();
        acl.addAccessControlEntry(testPrincipal, new Privilege[]{invalid});
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithAbstractPrivilege() throws Exception {
        Privilege abstractPriv = when(mock(Privilege.class).isAbstract()).thenReturn(true).getMock();
        when(abstractPriv.getName()).thenReturn("privName");
        PrivilegeManager privMgr = when(mock(PrivilegeManager.class).getPrivilege(anyString())).thenReturn(abstractPriv).getMock();
        ACL list = createACL(TEST_PATH, Collections.emptyList(), getNamePathMapper(), getRestrictionProvider(), privMgr);
        list.addAccessControlEntry(testPrincipal, new Privilege[] {abstractPriv});
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
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, true, Collections.emptyMap()));
        assertFalse(acl.isEmpty());
    }

    @Test
    public void testAddEntryTwice() throws Exception {
        acl.addEntry(testPrincipal, testPrivileges, true, Collections.emptyMap());
        assertFalse(acl.addEntry(testPrincipal, testPrivileges, true, Collections.emptyMap()));
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

    @Test(expected = AccessControlException.class)
    public void testRemoveInvalidEntry() throws Exception {
        JackrabbitAccessControlEntry ace = mockAccessControlEntry(testPrincipal, testPrivileges);
        acl.removeAccessControlEntry(ace);
        verify(ace, never()).getPrincipal();
        verify(ace, never()).getPrivileges();
    }

    @Test(expected = AccessControlException.class)
    public void testRemoveNonExisting() throws Exception {
        acl.removeAccessControlEntry(createEntry(testPrincipal, testPrivileges, true, Collections.emptySet()));
    }

    @Test
    public void testReorderToTheEnd() throws Exception {
        Privilege[] read = privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL);
        Privilege[] write = privilegesFromNames(JCR_WRITE);

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

    @Test(expected = AccessControlException.class)
    public void testReorderInvalidSourceEntry() throws Exception {
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_WRITE));

        AccessControlEntry invalid = createEntry(testPrincipal, false, null, JCR_WRITE);
        acl.orderBefore(invalid, acl.getEntries().get(0));
    }

    @Test(expected = AccessControlException.class)
    public void testReorderInvalidSourcDestEntry() throws Exception {
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_WRITE));

        AccessControlEntry invalid = createEntry(testPrincipal, false, null, JCR_MODIFY_PROPERTIES);
        acl.orderBefore(acl.getEntries().get(0), invalid);
    }

    @Test
    public void testReorderSourceSameAsDest() throws Exception {
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_READ, JCR_READ_ACCESS_CONTROL));
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_WRITE), false);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_WRITE));

        AccessControlEntry[] entries = acl.getAccessControlEntries();
        assertEquals(3, entries.length);

        acl.orderBefore(entries[1], entries[1]);

        assertArrayEquals(entries, acl.getAccessControlEntries());
    }

    @Test
    public void testMultipleEntries() throws Exception {
        Privilege[] privileges = privilegesFromNames(JCR_READ);
        acl.addEntry(testPrincipal, privileges, true);

        // new entry extends privileges.
        privileges = privilegesFromNames(JCR_READ, JCR_ADD_CHILD_NODES);
        assertTrue(acl.addEntry(testPrincipal, privileges, true));

        // expected: only a single allow-entry with both privileges
        assertEquals(1, acl.size());
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
        assertEquals(1, acl.size());
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
        assertEquals(2, acl.size());

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
        assertEquals(2, acl.size());

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
        assertEquals(2, acl.getAccessControlEntries().length);
        assertEquals(everyone, acl.getAccessControlEntries()[1].getPrincipal());
    }

    @Test
    public void testSetEntryForGroupPrincipal() throws Exception {
        Privilege[] privs = privilegesFromNames(JCR_READ);
        Principal grPrincipal = principalManager.getEveryone();

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

        acl.addEntry(everyone, grPriv, true, Collections.emptyMap());
        acl.addEntry(everyone, dePriv, false, Collections.emptyMap());

        Set<Privilege> allows = new HashSet<>();
        Set<Privilege> denies = new HashSet<>();
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
        assertEquals(Set.of(expected), allows);

        assertEquals(1, denies.size());
        assertArrayEquals(privilegesFromNames(JCR_REMOVE_CHILD_NODES), denies.toArray(new Privilege[0]));
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
        assertFalse(acl.addEntry(() -> testPrincipal.getName(), readPriv, false));
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
        assertArrayEquals(new String[] {REP_GLOB, REP_NT_NAMES, REP_PREFIXES, REP_ITEM_NAMES, REP_CURRENT, REP_GLOBS, REP_SUBTREES}, names);
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

    @Test(expected = AccessControlException.class)
    public void testUnsupportedRestrictions2() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("restr", Type.NAME, false);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        acl.addEntry(testPrincipal, testPrivileges, false, Collections.singletonMap("unsupported", getValueFactory().createValue("value")));
    }

    @Test(expected = AccessControlException.class)
    public void testInvalidRestrictionType() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("restr", Type.NAME, false);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        acl.addEntry(testPrincipal, testPrivileges, false, Collections.singletonMap("restr", getValueFactory().createValue(true)));
    }

    @Test(expected = AccessControlException.class)
    public void testMandatoryRestrictions() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAME, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        acl.addEntry(testPrincipal, testPrivileges, false, Collections.emptyMap(), Collections.emptyMap());
    }

    @Test
    public void testMandatoryRestrictionsPresent() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAME, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, false, Collections.singletonMap("mandatory", getValueFactory(root).createValue("name", PropertyType.NAME)), Collections.emptyMap()));
    }

    @Test(expected = AccessControlException.class)
    public void testMandatoryRestrictionsPresentAsMV() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAME, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        acl.addEntry(testPrincipal, testPrivileges, false, Collections.emptyMap(), Collections.singletonMap("mandatory", new Value[] {getValueFactory(root).createValue("name", PropertyType.NAME)}));
    }

    @Test(expected = AccessControlException.class)
    public void testMandatoryMVRestrictions() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAMES, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        acl.addEntry(testPrincipal, testPrivileges, false, Collections.emptyMap(), Collections.emptyMap());
    }

    @Test(expected = AccessControlException.class)
    public void testMandatoryMVRestrictionsPresentAsSingle() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAMES, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        acl.addEntry(testPrincipal, testPrivileges, false, Collections.singletonMap("mandatory", getValueFactory(root).createValue("name", PropertyType.NAME)), Collections.emptyMap());
    }

    @Test
    public void testMandatoryMVRestrictionsPresent() throws Exception {
        RestrictionProvider rp = new TestRestrictionProvider("mandatory", Type.NAMES, true);

        JackrabbitAccessControlList acl = createACL(TEST_PATH, new ArrayList<>(), getNamePathMapper(), rp);
        assertTrue(acl.addEntry(testPrincipal, testPrivileges, false, Collections.emptyMap(), Collections.singletonMap("mandatory", new Value[] {getValueFactory(root).createValue("name", PropertyType.NAME)})));
    }

    //--------------------------------------------------------------------------

    private static final class TestRestrictionProvider extends AbstractRestrictionProvider {

        private TestRestrictionProvider(@NotNull String name, @NotNull Type type, boolean isMandatory) {
            super(Collections.singletonMap(name, new RestrictionDefinitionImpl(name, type, isMandatory)));
        }

        @NotNull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Tree tree) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public RestrictionPattern getPattern(@Nullable String oakPath, @NotNull Set<Restriction> restrictions) {
            throw new UnsupportedOperationException();
        }
    }
}
