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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinitionImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_ITEM_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NODE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_SUBTREES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PrincipalPolicyImplTest extends AbstractPrincipalBasedTest {

    private static final String TEST_OAK_PATH = "/oak:test";
    private static final String POLICY_OAK_PATH = SUPPORTED_PATH + "/oak:testPath";

    private Principal principal;

    private String testJcrPath;
    private String policyJcrPath;

    private PrincipalPolicyImpl emptyPolicy;
    private PrincipalPolicyImpl policy;

    private PrivilegeBitsProvider privilegeBitsProvider;

    @Before
    public void before() throws Exception {
        super.before();

        testJcrPath = getNamePathMapper().getJcrPath(TEST_OAK_PATH);
        policyJcrPath = getNamePathMapper().getJcrPath(POLICY_OAK_PATH);

        principal = new PrincipalImpl("principalName");

        emptyPolicy = createPolicy(POLICY_OAK_PATH);

        policy = createPolicy(POLICY_OAK_PATH);
        policy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT, PrivilegeConstants.REP_WRITE));
        policy.addEntry(null, privilegesFromNames(PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT));

        privilegeBitsProvider = new PrivilegeBitsProvider(root);
    }

    private PrincipalPolicyImpl createPolicy(@NotNull String oakPath) {
        return new PrincipalPolicyImpl(principal, oakPath, getMgrProvider(root));
    }

    private Tree createEntryTree(@NotNull PrincipalPolicyImpl.EntryImpl entry) {
        Tree t = mock(Tree.class);
        PropertyState path = PropertyStates.createProperty(REP_EFFECTIVE_PATH, Objects.toString(entry.getOakPath(), ""));
        when(t.getProperty(REP_EFFECTIVE_PATH)).thenReturn(path);
        PropertyState privs = PropertyStates.createProperty(REP_PRIVILEGES, privilegeBitsProvider.getPrivilegeNames(entry.getPrivilegeBits()), Type.NAMES);
        when(t.getProperty(REP_PRIVILEGES)).thenReturn(privs);

        Iterable props = Iterables.transform(entry.getRestrictions(), Restriction::getProperty);
        Tree rTree = mock(Tree.class);
        if (props.iterator().hasNext()) {
            when(rTree.exists()).thenReturn(true);
            when(rTree.getProperties()).thenReturn(props);
            when(t.hasChild(REP_RESTRICTIONS)).thenReturn(true);
            when(t.getChild(REP_RESTRICTIONS)).thenReturn(rTree);
        } else {
            when(rTree.exists()).thenReturn(false);
            when(t.hasChild(REP_RESTRICTIONS)).thenReturn(false);
            when(t.getChild(REP_RESTRICTIONS)).thenReturn(rTree);
        }
        return t;
    }

    private Tree createEntryTree(@NotNull String oakPath, @NotNull String... privilegeNames) {
        Tree t = mock(Tree.class);
        PropertyState path = PropertyStates.createProperty(REP_EFFECTIVE_PATH, oakPath);
        when(t.getProperty(REP_EFFECTIVE_PATH)).thenReturn(path);
        PropertyState privs = PropertyStates.createProperty(REP_PRIVILEGES, ImmutableList.copyOf(privilegeNames), Type.NAMES);
        when(t.getProperty(REP_PRIVILEGES)).thenReturn(privs);
        Tree rTree = when(mock(Tree.class).getProperties()).thenReturn(Collections.emptySet()).getMock();
        when(t.getChild(REP_RESTRICTIONS)).thenReturn(rTree);
        return t;
    }

    private Map<String, Value> createGlobRestriction(@NotNull String value) {
        return Map.of(getJcrName(REP_GLOB), getValueFactory(root).createValue(value));
    }

    private Map<String, Value> createRestrictions(@NotNull String oakName, @NotNull String value) {
        return Map.of(getJcrName(oakName), getValueFactory(root).createValue(value));
    }

    private Map<String, Value[]> createMvRestrictions(@NotNull String oakName, int propertyType, @NotNull String... values) throws ValueFormatException {
        ValueFactory vf = getValueFactory(root);
        Value[] vs = new Value[values.length];
        for (int i = 0; i<values.length; i++) {
            vs[i] = vf.createValue(values[i], propertyType);
        }
        return Map.of(getJcrName(oakName), vs);
    }

    private String getJcrName(@NotNull String oakName) {
        return getNamePathMapper().getJcrName(oakName);
    }

    @Test
    public void testGetInitialSize() {
        assertEquals(0, emptyPolicy.size());
    }

    @Test
    public void testGetSize() {
        assertEquals(policy.getEntries().size(), policy.size());
    }

    @Test
    public void testInitiallyIsEmpty() {
        assertTrue(emptyPolicy.isEmpty());
    }

    @Test
    public void testIsEmpty() {
        assertEquals(policy.getEntries().isEmpty(), policy.isEmpty());
    }

    @Test
    public void testGetPath() {
        assertEquals(policyJcrPath, policy.getPath());
    }

    @Test
    public void testGetOakPath() {
        assertEquals(POLICY_OAK_PATH, policy.getOakPath());
    }

    @Test
    public void testGetNamePathMapper() {
        assertSame(getMgrProvider(root).getNamePathMapper(), policy.getNamePathMapper());
    }

    @Test
    public void testGetPrincipal() {
        assertSame(principal, policy.getPrincipal());
    }

    @Test
    public void testAddEntry() throws Exception {
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES)));
        assertEquals(1, emptyPolicy.size());
    }

    @Test
    public void testAddEntryTwice() throws Exception {
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES)));
        assertFalse(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES)));
        assertEquals(1, emptyPolicy.getEntries().size());
    }

    @Test
    public void testAddEntriesForSamePath() throws Exception {
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES)));
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_NODE)));
        List<PrincipalPolicyImpl.EntryImpl> entries = emptyPolicy.getEntries();
        assertEquals(2, entries.size());

        PrivilegeBitsProvider bitsProvider = new PrivilegeBitsProvider(root);

        assertEquals(testJcrPath, entries.get(0).getEffectivePath());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_ADD_CHILD_NODES), entries.get(0).getPrivilegeBits());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_NODE), entries.get(1).getPrivilegeBits());
    }

    @Test
    public void testAddEntriesWithRestrictionsForSamePath() throws Exception {
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES)));
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_REMOVE_CHILD_NODES), Map.of(), createMvRestrictions(REP_ITEM_NAMES, PropertyType.NAME, "removable")));

        PrivilegeBitsProvider bitsProvider = new PrivilegeBitsProvider(root);

        List<PrincipalPolicyImpl.EntryImpl> entries = emptyPolicy.getEntries();
        assertEquals(2, entries.size());

        PrincipalPolicyImpl.EntryImpl entry = entries.get(0);
        assertEquals(testJcrPath, entry.getEffectivePath());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_ADD_CHILD_NODES), entry.getPrivilegeBits());
        assertTrue(entry.getRestrictions().isEmpty());

        entry = entries.get(1);
        assertEquals(testJcrPath, entry.getEffectivePath());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_REMOVE_CHILD_NODES), entry.getPrivilegeBits());
        assertEquals(1, entry.getRestrictions().size());
        assertEquals(REP_ITEM_NAMES, entry.getRestrictions().iterator().next().getDefinition().getName());
    }

    @Test
    public void testAddEntriesWithMultipleRestrictionsForSamePath() throws Exception {
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES), createGlobRestriction("/any*/glob"), Map.of()));
        assertTrue(emptyPolicy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_REMOVE_CHILD_NODES), Map.of(), createMvRestrictions(REP_ITEM_NAMES, PropertyType.NAME, "removable")));

        PrivilegeBitsProvider bitsProvider = new PrivilegeBitsProvider(root);

        List<PrincipalPolicyImpl.EntryImpl> entries = emptyPolicy.getEntries();
        assertEquals(2, entries.size());

        PrincipalPolicyImpl.EntryImpl entry = entries.get(0);
        assertEquals(testJcrPath, entry.getEffectivePath());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_ADD_CHILD_NODES), entry.getPrivilegeBits());
        assertEquals(1, entry.getRestrictions().size());
        assertEquals(REP_GLOB, entry.getRestrictions().iterator().next().getDefinition().getName());

        entry = entries.get(1);
        assertEquals(testJcrPath, entry.getEffectivePath());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_REMOVE_CHILD_NODES), entry.getPrivilegeBits());
        assertEquals(bitsProvider.getBits(PrivilegeConstants.JCR_REMOVE_CHILD_NODES), entry.getPrivilegeBits());
        assertEquals(1, entry.getRestrictions().size());
        assertEquals(REP_ITEM_NAMES, entry.getRestrictions().iterator().next().getDefinition().getName());
    }

    @Test
    public void testAddEntryWithRestrictions() throws Exception {
        Map<String, Value[]> mvRestrictions = createMvRestrictions(AccessControlConstants.REP_ITEM_NAMES, PropertyType.NAME, getNamePathMapper().getJcrName("oak:test"), "abc");

        int expectedSize = policy.getEntries().size()+1;
        assertTrue(policy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_WRITE), Collections.emptyMap(), mvRestrictions));
        assertEquals(expectedSize, policy.size());
    }

    @Test
    public void testAddEntryWithRestrictionsTwice() throws Exception {
        Map<String, Value> restrictions = createGlobRestriction("*/some*glob");

        assertTrue(policy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL), restrictions, Collections.emptyMap()));
        assertFalse(policy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL), restrictions, Collections.emptyMap()));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryMissingMandatoryRestriction() throws Exception {
        RestrictionProvider restrictionProvider = mock(RestrictionProvider.class);
        when(restrictionProvider.getSupportedRestrictions(anyString())).thenReturn(
                Set.of(new RestrictionDefinitionImpl("oak:mandatory", Type.LONG, true)));
        MgrProvider mp = when(mock(MgrProvider.class).getRestrictionProvider()).thenReturn(restrictionProvider).getMock();
        when(mp.getNamePathMapper()).thenReturn(getNamePathMapper());

        PrincipalPolicyImpl plc = new PrincipalPolicyImpl(principal, POLICY_OAK_PATH, mp);
        String jcrName = namePathMapper.getJcrName("oak:mandatory");
        Map<String,Value[]> mvRestrictions = Map.of(jcrName, new Value[] {getValueFactory(root).createValue(1)});
        plc.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), Map.of(), mvRestrictions);
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryMissingMandatoryMVRestriction() throws Exception {
        RestrictionProvider restrictionProvider = mock(RestrictionProvider.class);
        when(restrictionProvider.getSupportedRestrictions(anyString())).thenReturn(
                Set.of(new RestrictionDefinitionImpl("oak:mandatory", Type.LONGS, true)));
        MgrProvider mp = when(mock(MgrProvider.class).getRestrictionProvider()).thenReturn(restrictionProvider).getMock();
        when(mp.getNamePathMapper()).thenReturn(getNamePathMapper());

        PrincipalPolicyImpl plc = new PrincipalPolicyImpl(principal, POLICY_OAK_PATH, mp);
        String jcrName = namePathMapper.getJcrName("oak:mandatory");
        Map<String,Value> svRestrictions = Map.of(jcrName, getValueFactory(root).createValue(1));
        plc.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), svRestrictions, Map.of());
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryMandatoryRestrictionWithOakName() throws Exception {
        RestrictionProvider restrictionProvider = when(mock(RestrictionProvider.class).getSupportedRestrictions(anyString())).thenReturn(
                Set.of(new RestrictionDefinitionImpl("oak:mandatory", Type.LONG, true))).getMock();
        MgrProvider mp = when(mock(MgrProvider.class).getRestrictionProvider()).thenReturn(restrictionProvider).getMock();
        when(mp.getNamePathMapper()).thenReturn(getNamePathMapper());

        PrincipalPolicyImpl plc = new PrincipalPolicyImpl(principal, POLICY_OAK_PATH, mp);
        Map<String,Value> svRestrictions = Map.of("oak:mandatory", getValueFactory(root).createValue(1));
        plc.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), svRestrictions, Map.of());
    }

    @Test
    public void testAddEntryMandatoryRestriction() throws Exception {
        RestrictionDefinition def = new RestrictionDefinitionImpl("mandatory", Type.LONG, true);
        Restriction r = mock(Restriction.class);

        RestrictionProvider restrictionProvider = mock(RestrictionProvider.class);
        when(restrictionProvider.getSupportedRestrictions(anyString())).thenReturn(Set.of(def));
        when(restrictionProvider.createRestriction(anyString(), anyString(), any(Value.class))).thenReturn(r);

        MgrProvider mp = when(mock(MgrProvider.class).getRestrictionProvider()).thenReturn(restrictionProvider).getMock();
        when(mp.getNamePathMapper()).thenReturn(getNamePathMapper());
        when(mp.getPrivilegeManager()).thenReturn(getPrivilegeManager(root));
        when(mp.getPrivilegeBitsProvider()).thenReturn(new PrivilegeBitsProvider(root));

        PrincipalPolicyImpl plc = new PrincipalPolicyImpl(principal, POLICY_OAK_PATH, mp);
        Map<String,Value> svRestrictions = Map.of("mandatory", getValueFactory(root).createValue(1));
        plc.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_VERSION_MANAGEMENT), svRestrictions, Map.of());

        assertTrue(plc.getEntries().get(0).getRestrictions().contains(r));
    }

    @Test
    public void testAddEntryForRepositoryLevel() throws Exception {
        assertTrue(emptyPolicy.addEntry(null, privilegesFromNames(PrivilegeConstants.JCR_WORKSPACE_MANAGEMENT)));
        assertEquals(1, emptyPolicy.getEntries().size());
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithRelativePath() throws Exception {
        emptyPolicy.addEntry("relative/path", privilegesFromNames(PrivilegeConstants.JCR_ADD_CHILD_NODES));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithEmptyPath() throws Exception {
        emptyPolicy.addEntry("", privilegesFromNames(PrivilegeConstants.JCR_REMOVE_NODE));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryEmptyPrivileges() throws Exception {
        policy.addEntry(testJcrPath, new Privilege[0]);
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryUnknownPrivilege() throws Exception {
        Privilege privilege = when(mock(Privilege.class).getName()).thenReturn("unknown").getMock();
        policy.addEntry(testJcrPath, new Privilege[] {privilege});
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryAbstractPrivilege() throws Exception {
        Privilege privilege = when(mock(Privilege.class).isAbstract()).thenReturn(true).getMock();
        when(privilege.getName()).thenReturn("abstract");

        PrivilegeManager privilegeManager = when(mock(PrivilegeManager.class).getPrivilege("abstract")).thenReturn(privilege).getMock();
        MgrProvider mp = when(mock(MgrProvider.class).getPrivilegeManager()).thenReturn(privilegeManager).getMock();
        when(mp.getNamePathMapper()).thenReturn(getNamePathMapper());
        when(mp.getRestrictionProvider()).thenReturn(RestrictionProvider.EMPTY);

        PrincipalPolicyImpl policy = new PrincipalPolicyImpl(principal, POLICY_OAK_PATH, mp);
        policy.addEntry(testJcrPath, new Privilege[] {privilege});
    }

    @Test(expected = AccessControlException.class)
    public void testAddAccessControlEntryDifferentPrincipal() throws Exception {
        policy.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_ALL), true, null, Collections.emptyMap());
    }

    @Test(expected = AccessControlException.class)
    public void testAddDenyingAccessControlEntry() throws Exception {
        policy.addEntry(principal, privilegesFromNames(PrivilegeConstants.REP_READ_NODES), false, Collections.emptyMap(), null);
    }

    @Test(expected = AccessControlException.class)
    public void testAddAccessControlEntryMissingNodePath() throws Exception {
        policy.addEntry(principal, privilegesFromNames(PrivilegeConstants.REP_USER_MANAGEMENT), true, Collections.emptyMap(), Collections.singletonMap(AccessControlConstants.REP_NT_NAMES, new Value[] {getValueFactory(root).createValue(NodeTypeConstants.NT_REP_SYSTEM)}));
    }

    @Test(expected = AccessControlException.class)
    public void testAddAccessControlEntryMissingNodePath2() throws Exception {
        policy.addEntry(principal, privilegesFromNames(PrivilegeConstants.REP_WRITE), true, null, null);
    }

    @Test
    public void testAddAccessControlEntryWithEmptyNodePathRestriction() throws Exception {
        assertTrue(emptyPolicy.addEntry(principal, privilegesFromNames(PrivilegeConstants.REP_ADD_PROPERTIES), true,
                createRestrictions(REP_NODE_PATH, ""), null));

        List<PrincipalPolicyImpl.EntryImpl> entries = emptyPolicy.getEntries();
        assertEquals(1, entries.size());

        PrincipalPolicyImpl.EntryImpl entry = entries.get(0);
        assertNull(entry.getOakPath());
        // effective-path restriction must be filtered out
        assertNull(entry.getRestrictions(getJcrName(REP_NODE_PATH)));
    }

    @Test
    public void testAddAccessControlEntryWithNodePathRestriction() throws Exception {
        assertTrue(emptyPolicy.addEntry(principal, privilegesFromNames(PrivilegeConstants.REP_ADD_PROPERTIES), true,
                createRestrictions(REP_NODE_PATH, testJcrPath),
                createMvRestrictions(AccessControlConstants.REP_ITEM_NAMES, PropertyType.NAME, "itemName")));
        List<PrincipalPolicyImpl.EntryImpl> entries = emptyPolicy.getEntries();
        assertEquals(1, entries.size());

        PrincipalPolicyImpl.EntryImpl entry = entries.get(0);
        assertEquals(TEST_OAK_PATH, entry.getOakPath());
        // effective-path restriction must be filtered out
        assertNull(entry.getRestrictions(getJcrName(REP_NODE_PATH)));
    }

    @Test
    public void testAddAccessControlEntryWithRestrictions() throws Exception {
        ValueFactory vf = getValueFactory(root);
        Map<String, Value> restr = Map.of(getJcrName(REP_NODE_PATH), vf.createValue(testJcrPath), getJcrName(REP_GLOB), vf.createValue("string"));
        assertTrue(emptyPolicy.addEntry(principal, privilegesFromNames(PrivilegeConstants.REP_USER_MANAGEMENT), true,
                restr, null));
        List<PrincipalPolicyImpl.EntryImpl> entries = emptyPolicy.getEntries();
        assertEquals(1, entries.size());

        PrincipalPolicyImpl.EntryImpl entry = entries.get(0);
        assertEquals(TEST_OAK_PATH, entry.getOakPath());
        // effective-path restriction must be filtered out
        assertNull(entry.getRestrictions(getJcrName(REP_NODE_PATH)));
        assertNotNull(entry.getRestrictions(getJcrName(REP_GLOB)));
    }

    @Test
    public void addEntryTree() throws Exception {
        assertTrue(emptyPolicy.addEntry(createEntryTree(TEST_OAK_PATH, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_WRITE), Collections.emptyList()));

        PrincipalPolicyImpl.EntryImpl entry = emptyPolicy.getEntries().get(0);
        assertEquals(testJcrPath, entry.getEffectivePath());
        assertEquals(TEST_OAK_PATH, entry.getOakPath());
        assertEquals(privilegeBitsProvider.getBits(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_WRITE), entry.getPrivilegeBits());
    }

    @Test
    public void addEntryTreeRepositoryLevel() throws Exception {
        assertTrue(emptyPolicy.addEntry(createEntryTree("", PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_WRITE), Collections.emptyList()));

        PrincipalPolicyImpl.EntryImpl entry = emptyPolicy.getEntries().get(0);
        assertNull(entry.getEffectivePath());
        assertNull(entry.getOakPath());
    }

    @Test
    public void addEntryTreeJcrAll() throws Exception {
        assertTrue(emptyPolicy.addEntry(createEntryTree(TEST_OAK_PATH, JCR_ALL), Collections.emptyList()));

        PrincipalPolicyImpl.EntryImpl entry = emptyPolicy.getEntries().get(0);
        assertArrayEquals(privilegesFromNames(JCR_ALL), entry.getPrivileges());
    }

    @Test
    public void addEntryTreeExistingEntry() throws Exception {
        assertFalse(policy.addEntry(createEntryTree(policy.getEntries().get(0)), Collections.emptyList()));
    }


    @Test
    public void addEntryTreeNullPathWithFilter() throws Exception {
        Tree entryTree = createEntryTree("", JCR_ALL);

        assertTrue(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(null)));
        
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList("/not/matching/path")));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(PathUtils.ROOT_PATH)));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH)));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH + "/subtree")));
    }
    
    @Test
    public void addEntryTreeWithFilter() throws Exception {
        Tree entryTree = createEntryTree(TEST_OAK_PATH, JCR_ALL);
        
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(null)));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList("/not/matching/path")));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(PathUtils.ROOT_PATH)));
        
        assertTrue(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH)));
        assertTrue(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH + "/subtree")));
    }

    @Test
    public void addEntryTreeWithPathFilterAndRestrictions() throws Exception {
        PrincipalPolicyImpl policy = createPolicy(POLICY_OAK_PATH);
        policy.addEntry(testJcrPath, privilegesFromNames(PrivilegeConstants.JCR_READ), Collections.emptyMap(), 
                Collections.singletonMap(getJcrName(REP_SUBTREES), new Value[] {getValueFactory(root).createValue("child")}));
        
        // create an entry-tree that limits effect to a subtree of TEST_OAK_PATH
        Tree entryTree = createEntryTree(policy.getEntries().get(0));
        
        // paths that don't match the restriction
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(null)));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList("/not/matching/path")));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(PathUtils.ROOT_PATH)));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH)));
        assertFalse(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH + "/subtree")));

        // paths that match the restriction
        assertTrue(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH + "/child")));
        assertTrue(createPolicy(POLICY_OAK_PATH).addEntry(entryTree, Collections.singletonList(TEST_OAK_PATH + "/child/subtree")));
    }

    @Test
    public void testRemoveEntry() throws Exception {
        for (AccessControlEntry entry : policy.getAccessControlEntries()) {
            assertFalse(policy.isEmpty());
            policy.removeAccessControlEntry(entry);
        }
        assertTrue(policy.isEmpty());
    }

    @Test(expected = AccessControlException.class)
    public void testRemoveEntryTwice() throws Exception  {
        AccessControlEntry entry = policy.getAccessControlEntries()[0];
        policy.removeAccessControlEntry(entry);
        policy.removeAccessControlEntry(entry);
    }

    @Test(expected = AccessControlException.class)
    public void testRemoveEntryInvalidEntry() throws Exception  {
        policy.removeAccessControlEntry(invalidEntry(policy.getEntries().get(0)));
    }

    @Test
    public void testOrderBefore() throws Exception {
        PrincipalPolicyImpl.Entry entryA = policy.getEntries().get(0);
        PrincipalPolicyImpl.Entry entryB = policy.getEntries().get(1);

        policy.orderBefore(entryB, entryA);
        assertArrayEquals(new AccessControlEntry[] {entryB, entryA}, policy.getAccessControlEntries());
    }

    @Test
    public void testOrderBeforeDestNull() throws Exception {
        PrincipalPolicyImpl.Entry entry = policy.getEntries().get(0);
        policy.orderBefore(entry, null);
        assertEquals(entry, policy.getAccessControlEntries()[1]);
    }

    @Test
    public void testOrderBeforeSame() throws Exception {
        policy.orderBefore(policy.getEntries().get(1), policy.getEntries().get(1));
    }

    @Test(expected = AccessControlException.class)
    public void testOrderBeforeNonExistingSrc() throws Exception {
        PrincipalPolicyImpl.Entry entry = policy.getEntries().get(0);
        policy.removeAccessControlEntry(entry);
        policy.orderBefore(entry, null);
    }

    @Test(expected = AccessControlException.class)
    public void testOrderBeforeNonExistingDest() throws Exception {
        PrincipalPolicyImpl.Entry entry = policy.getEntries().get(1);
        policy.removeAccessControlEntry(entry);
        policy.orderBefore(policy.getEntries().get(0), entry);
    }

    @Test(expected = AccessControlException.class)
    public void testOrderBeforeInvalidSrc() throws Exception {
        policy.orderBefore(invalidEntry(policy.getEntries().get(1)), policy.getEntries().get(0));
    }

    @Test(expected = AccessControlException.class)
    public void testOrderBeforeInvalidDest() throws Exception {
        policy.orderBefore(policy.getEntries().get(1), invalidEntry(policy.getEntries().get(0)));
    }

    @Test
    public void testEntry() throws Exception {
        Privilege[] privs = privilegesFromNames(PrivilegeConstants.JCR_ALL);
        emptyPolicy.addEntry(testJcrPath, privs);

        PrincipalPolicyImpl.EntryImpl entry = emptyPolicy.getEntries().get(0);
        assertEquals(TEST_OAK_PATH, entry.getOakPath());
        assertEquals(testJcrPath, entry.getEffectivePath());
        assertArrayEquals(privs, entry.getPrivileges());
        assertEquals(privilegeBitsProvider.getBits(JCR_ALL), entry.getPrivilegeBits());
        assertSame(principal, entry.getPrincipal());
        assertTrue(entry.isAllow());
    }

    @Test
    public void testEntryRepositoryLevel() throws Exception {
        Privilege[] privs = privilegesFromNames(JCR_NAMESPACE_MANAGEMENT);
        emptyPolicy.addEntry(null, privs);

        PrincipalPolicyImpl.EntryImpl entry = emptyPolicy.getEntries().get(0);
        assertEquals(null, entry.getOakPath());
        assertEquals(null, entry.getEffectivePath());
        assertArrayEquals(privs, entry.getPrivileges());
        assertEquals(privilegeBitsProvider.getBits(JCR_NAMESPACE_MANAGEMENT), entry.getPrivilegeBits());
        assertSame(principal, entry.getPrincipal());
        assertTrue(entry.isAllow());
    }

    private static PrincipalAccessControlList.Entry invalidEntry(@NotNull PrincipalAccessControlList.Entry entry) {
        return new PrincipalAccessControlList.Entry() {
            @Override
            public @Nullable String getEffectivePath() {
                return entry.getEffectivePath();
            }

            @Override
            public boolean isAllow() {
                return entry.isAllow();
            }

            @NotNull
            @Override
            public String[] getRestrictionNames() throws RepositoryException {
                return entry.getRestrictionNames();
            }

            @Nullable
            @Override
            public Value getRestriction(@NotNull String s) throws RepositoryException {
                return entry.getRestriction(s);
            }

            @Nullable
            @Override
            public Value[] getRestrictions(@NotNull String s) throws RepositoryException {
                return entry.getRestrictions(s);
            }

            @Override
            public @NotNull PrivilegeCollection getPrivilegeCollection() throws RepositoryException {
                return entry.getPrivilegeCollection();
            }

            @Override
            public Principal getPrincipal() {
                return entry.getPrincipal();
            }

            @Override
            public Privilege[] getPrivileges() {
                return entry.getPrivileges();
            }
        };
    }
}