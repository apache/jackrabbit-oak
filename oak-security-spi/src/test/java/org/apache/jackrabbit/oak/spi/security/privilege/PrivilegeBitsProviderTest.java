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
package org.apache.jackrabbit.oak.spi.security.privilege;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrivilegeBitsProviderTest implements PrivilegeConstants {

    private static final String KNOWN_PRIV_NAME = "prefix:known";

    private final PropertyState ps = PropertyStates.createProperty(REP_BITS, 5000L, Type.LONG);
    private final PrivilegeBits bits = PrivilegeBits.getInstance(ps);

    private Tree privTree;
    private Tree pTree;
    private Root root;
    private PrivilegeBitsProvider bitsProvider;

    @Before
    public void before() {
        pTree = mockPrivilegeDefinitionTree(KNOWN_PRIV_NAME, ps);
        privTree = Mockito.mock(Tree.class);
        when(privTree.exists()).thenReturn(true);
        when(privTree.hasChild(KNOWN_PRIV_NAME)).thenReturn(true);
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);
        when(privTree.getChildren()).thenReturn(Set.of(pTree));

        root = Mockito.mock(Root.class);
        when(root.getTree(PRIVILEGES_PATH)).thenReturn(privTree);

        bitsProvider = new PrivilegeBitsProvider(root);
    }
    
    private static Tree mockPrivilegeDefinitionTree(@NotNull String name, @NotNull PropertyState bitsProp) {
        Tree tree = Mockito.mock(Tree.class);
        when(tree.getName()).thenReturn(name);
        when(tree.getProperty(REP_BITS)).thenReturn(bitsProp);
        return tree;
    }

    @Test
    public void testGetPrivilegesTree() {
        assertNotNull(bitsProvider.getPrivilegesTree());
    }

    @Test
    public void testGetBitsNonExistingPrivilegesTree() {
        when(privTree.exists()).thenReturn(false);
        assertSame(PrivilegeBits.EMPTY, bitsProvider.getBits(KNOWN_PRIV_NAME));
    }

    @Test
    public void testGetBitsEmptyNames() {
        assertSame(PrivilegeBits.EMPTY, bitsProvider.getBits());
    }

    @Test
    public void testGetBitsEmptyArray() {
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits(new String[0]));
    }

    @Test
    public void testGetBitsEmptyString() {
        assertEquals(PrivilegeBits.EMPTY, bitsProvider.getBits(""));
    }

    @Test
    public void testGetBitsEmptyIterable() {
        assertSame(PrivilegeBits.EMPTY, bitsProvider.getBits(ImmutableList.of()));
    }

    @Test
    public void testGetBitsBuiltInSingleName() {
        PrivilegeBits bits = bitsProvider.getBits(JCR_LOCK_MANAGEMENT);
        assertFalse(bits.isEmpty());

        assertEquals(PrivilegeBits.BUILT_IN.get(JCR_LOCK_MANAGEMENT), bits);
    }

    @Test
    public void testGetBitsBuiltInSingleton() {
        PrivilegeBits bits = bitsProvider.getBits(ImmutableList.of(JCR_LOCK_MANAGEMENT));
        assertFalse(bits.isEmpty());

        assertEquals(PrivilegeBits.BUILT_IN.get(JCR_LOCK_MANAGEMENT), bits);
    }

    @Test
    public void testGetBitsBuiltInNames() {
        PrivilegeBits bits = bitsProvider.getBits(JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES);
        assertFalse(bits.isEmpty());

        PrivilegeBits mod = PrivilegeBits.getInstance(bitsProvider.getBits(JCR_ADD_CHILD_NODES)).add(bitsProvider.getBits(JCR_REMOVE_CHILD_NODES));
        assertEquals(bits, mod.unmodifiable());
    }

    @Test
    public void testGetBitsBuiltInIterable() {
        PrivilegeBits bits = bitsProvider.getBits(ImmutableList.of(JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES));
        assertFalse(bits.isEmpty());

        PrivilegeBits mod = PrivilegeBits.getInstance(bitsProvider.getBits(JCR_ADD_CHILD_NODES)).add(bitsProvider.getBits(JCR_REMOVE_CHILD_NODES));
        assertEquals(bits, mod.unmodifiable());
    }

    @Test
    public void testGetBitsNonExistingTree() {
        when(privTree.exists()).thenReturn(true);
        when(privTree.hasChild(KNOWN_PRIV_NAME)).thenReturn(false);
        // privilegesTree has no child for KNOWN_PRIV_NAME
        assertSame(PrivilegeBits.EMPTY, bitsProvider.getBits(KNOWN_PRIV_NAME));
    }

    @Test
    public void testGetBitsKnownPrivName() {
        when(privTree.exists()).thenReturn(true);
        when(privTree.hasChild(KNOWN_PRIV_NAME)).thenReturn(true);
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);

        assertEquals(bits.unmodifiable(), bitsProvider.getBits(KNOWN_PRIV_NAME));
    }

    @Test
    public void testGetBitsTwiceSingleBuiltIn() {
        Iterable<String> names = ImmutableList.of(JCR_ADD_CHILD_NODES);
        PrivilegeBits bits1 = bitsProvider.getBits(names);
        PrivilegeBits bits2 = bitsProvider.getBits(names);

        assertSame(bits1, bits2);
        assertEquals(bits1, bits2);

        verify(root, never()).getTree(PRIVILEGES_PATH);
        verify(privTree, never()).getChild(KNOWN_PRIV_NAME);
    }

    @Test
    public void testGetBitsTwiceMultipleBuiltIn() {
        Iterable<String> names = ImmutableList.of(JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES);
        PrivilegeBits bits1 = bitsProvider.getBits(names);
        PrivilegeBits bits2 = bitsProvider.getBits(names);

        assertNotSame(bits1, bits2);
        assertEquals(bits1, bits2);

        verify(root, never()).getTree(PRIVILEGES_PATH);
        verify(privTree, never()).getChild(KNOWN_PRIV_NAME);
    }

    @Test
    public void testGetBitsTwiceKnown() {
        Iterable<String> names = ImmutableList.of(KNOWN_PRIV_NAME);
        PrivilegeBits bits1 = bitsProvider.getBits(names);
        PrivilegeBits bits2 = bitsProvider.getBits(names);

        assertNotSame(bits1, bits2);
        assertEquals(bits1, bits2);

        verify(root, times(1)).getTree(PRIVILEGES_PATH);
        verify(privTree, times(1)).getChild(KNOWN_PRIV_NAME);
    }

    @Test
    public void testGetBitsTwiceBuitInKnown() {
        Iterable<String> names = ImmutableList.of(KNOWN_PRIV_NAME, JCR_ADD_CHILD_NODES);
        PrivilegeBits bits1 = bitsProvider.getBits(names);
        PrivilegeBits bits2 = bitsProvider.getBits(names);

        assertNotSame(bits1, bits2);
        assertEquals(bits1, bits2);

        verify(root, times(1)).getTree(PRIVILEGES_PATH);
        verify(privTree, times(1)).getChild(KNOWN_PRIV_NAME);
    }

    @Test
    public void testGetBitsTwiceKnownUnknown() {
        Iterable<String> names = ImmutableList.of(KNOWN_PRIV_NAME, "unknown");
        PrivilegeBits bits1 = bitsProvider.getBits(names);
        PrivilegeBits bits2 = bitsProvider.getBits(names);

        assertNotSame(bits1, bits2);
        assertEquals(bits1, bits2);
        assertEquals(bitsProvider.getBits(KNOWN_PRIV_NAME), bits1);

        verify(root, times(2)).getTree(PRIVILEGES_PATH);
        verify(privTree, times(1)).getChild(KNOWN_PRIV_NAME);
    }

    @Test
    public void testBitsValidateEmpty() throws Exception {
        PrivilegeBits bits = bitsProvider.getBits(Collections.emptySet(), true);
        assertSame(PrivilegeBits.EMPTY, bits);

        bits = bitsProvider.getBits(Collections.emptySet(), false);
        assertSame(PrivilegeBits.EMPTY, bits);

        verify(root, never()).getTree(PRIVILEGES_PATH);
        verify(privTree, never()).getChild(anyString());
    }
    
    @Test
    public void testGetBitsValidateTwiceBuitInKnown() throws Exception {
        Iterable<String> names = ImmutableList.of(KNOWN_PRIV_NAME, JCR_ADD_CHILD_NODES);
        PrivilegeBits bits1 = bitsProvider.getBits(names, true);
        PrivilegeBits bits2 = bitsProvider.getBits(names, false);

        assertNotSame(bits1, bits2);
        assertEquals(bits1, bits2);

        verify(root, times(1)).getTree(PRIVILEGES_PATH);
        verify(privTree, times(1)).getChild(KNOWN_PRIV_NAME);
    }
    
    @Test(expected = AccessControlException.class)
    public void testBitsValidateNonExistingTree() throws Exception {
        Iterable<String> names = ImmutableList.of(KNOWN_PRIV_NAME, JCR_ADD_CHILD_NODES);

        when(privTree.exists()).thenReturn(true);
        when(privTree.hasChild(KNOWN_PRIV_NAME)).thenReturn(false);
        // privilegesTree has no child for KNOWN_PRIV_NAME -> must fail
        bitsProvider.getBits(names, true);
    }

    public void testBitsValidateFalseNonExistingTree() throws Exception {
        Iterable<String> names = ImmutableList.of(KNOWN_PRIV_NAME, JCR_ADD_CHILD_NODES);

        when(privTree.exists()).thenReturn(true);
        when(privTree.hasChild(KNOWN_PRIV_NAME)).thenReturn(false);
        
        // validation is disabled => same as getBits(Iterable)
        assertEquals(bitsProvider.getBits(names), bitsProvider.getBits(names, false));
    }

    @Test
    public void testGetBitsFromEmptyPrivileges() {
        assertSame(PrivilegeBits.EMPTY, bitsProvider.getBits(new Privilege[0], NamePathMapper.DEFAULT));
    }

    @Test
    public void testGetBitsFromPrivilegesInvalidMapping() {
        Privilege p = Mockito.mock(Privilege.class);
        when(p.getName()).thenReturn("name");

        NamePathMapper mapper = new NamePathMapper.Default() {
            @Nullable
            @Override
            public String getOakNameOrNull(@NotNull String jcrName) {
                return null;
            }
        };
        assertSame(PrivilegeBits.EMPTY, bitsProvider.getBits(new Privilege[] {p}, mapper));
    }


    @Test
    public void testGetPrivilegeNamesFromEmpty() {
        Set<String> names = bitsProvider.getPrivilegeNames(PrivilegeBits.EMPTY);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetPrivilegeNamesFromNull() {
        Set<String> names = bitsProvider.getPrivilegeNames(null);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetPrivilegeNamesNonExistingPrivilegesTree() {
        when(privTree.exists()).thenReturn(false);

        Set<String> names = bitsProvider.getPrivilegeNames(bits);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetPrivilegeNames() {
        Set<String> names = bitsProvider.getPrivilegeNames(bits);
        assertFalse(names.isEmpty());
        assertEquals(Set.of(KNOWN_PRIV_NAME), names);
    }

    @Test
    public void testGetPrivilegeNamesFromCache() {
        Set<String> names = bitsProvider.getPrivilegeNames(bits);
        assertSame(names, bitsProvider.getPrivilegeNames(bits));
    }

    @Test
    public void testGetPrivilegeNamesWithNewRegistered() {
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(root);
        // fill cache
        Set<String> names = provider.getPrivilegeNames(bits);

        PropertyState priv2ps = PropertyStates.createProperty(REP_BITS, 10000L, Type.LONG); 
        Tree priv2 = mockPrivilegeDefinitionTree("priv2", priv2ps);
        when(privTree.hasChild("priv2")).thenReturn(true);
        when(privTree.getChild("priv2")).thenReturn(priv2);
        when(privTree.getChildren()).thenReturn(Set.of(pTree, priv2));

        PrivilegeBits bits2 = PrivilegeBits.getInstance(priv2ps);
        Set<String> newNames = provider.getPrivilegeNames(bits2);
        assertNotEquals(names, newNames);
        assertEquals(Set.of("priv2"), newNames);
    }

    @Test
    public void testGetPrivilegeNamesWithAggregation() {
        Tree anotherPriv = Mockito.mock(Tree.class);
        when(anotherPriv.exists()).thenReturn(true);
        when(anotherPriv.getName()).thenReturn("name2");
        when(anotherPriv.hasProperty(REP_AGGREGATES)).thenReturn(true);
        when(anotherPriv.getProperty(REP_AGGREGATES)).thenReturn(PropertyStates.createProperty(REP_AGGREGATES, ImmutableList.of(KNOWN_PRIV_NAME), Type.NAMES));
        PropertyState bits2 = PropertyStates.createProperty(REP_BITS, 7500L);
        when(anotherPriv.getProperty(REP_BITS)).thenReturn(bits2);

        when(privTree.getChildren()).thenReturn(Set.of(pTree, anotherPriv));

        // aggregation must be removed from the result set
        Set<String> expected = Set.of("name2");
        Set<String> result = bitsProvider.getPrivilegeNames(PrivilegeBits.getInstance(PrivilegeBits.getInstance(bits), PrivilegeBits.getInstance(bits2)));
        assertEquals(expected, result);
    }

    @Test
    public void testGetAggregatedPrivilegeNamesEmpty() {
        assertTrue(Iterables.isEmpty(bitsProvider.getAggregatedPrivilegeNames()));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesEmptyArray() {
        assertTrue(Iterables.isEmpty(bitsProvider.getAggregatedPrivilegeNames()));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesSingleNonAggregates() {
        for (String name : NON_AGGREGATE_PRIVILEGES) {
            assertEquals(Set.of(name), bitsProvider.getAggregatedPrivilegeNames(name));
        }
    }

    @Test
    public void testGetAggregatedPrivilegeNamesNonAggregates() {
        assertEquals(
                Set.of(REP_READ_NODES, JCR_LIFECYCLE_MANAGEMENT, JCR_READ_ACCESS_CONTROL),
                bitsProvider.getAggregatedPrivilegeNames(REP_READ_NODES, JCR_LIFECYCLE_MANAGEMENT, JCR_READ_ACCESS_CONTROL));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesJcrRead() {
        assertEquals(Set.of(AGGREGATE_PRIVILEGES.get(JCR_READ)), CollectionUtils.toSet(bitsProvider.getAggregatedPrivilegeNames(JCR_READ)));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesJcrWrite() {
        // nested aggregated privileges in this case
        Set<String> result = CollectionUtils.toSet(bitsProvider.getAggregatedPrivilegeNames(JCR_WRITE));
        assertNotEquals(Set.of(AGGREGATE_PRIVILEGES.get(JCR_WRITE)), result);

        String[] expected = new String[] {
                JCR_ADD_CHILD_NODES, JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE,
                REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES
        };
        assertEquals(Set.of(expected), result);
    }

    @Test
    public void testGetAggregatedPrivilegeNamesBuiltInTwice() {
        Iterable<String> agg = bitsProvider.getAggregatedPrivilegeNames(JCR_READ);
        assertSame(agg, bitsProvider.getAggregatedPrivilegeNames(JCR_READ));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesMultipleBuiltIn() {
        Iterable<String> expected = CollectionUtils.toSet(Iterables.concat(
                bitsProvider.getAggregatedPrivilegeNames(JCR_READ),
                bitsProvider.getAggregatedPrivilegeNames(JCR_WRITE)));

        // create new to avoid reading from cache
        PrivilegeBitsProvider bp = new PrivilegeBitsProvider(root);
        Iterable<String> result = bp.getAggregatedPrivilegeNames(JCR_READ, JCR_WRITE);
        assertEquals(expected, CollectionUtils.toSet(result));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesMultipleBuiltIn2() {
        Iterable<String> expected = CollectionUtils.toSet(Iterables.concat(
                bitsProvider.getAggregatedPrivilegeNames(JCR_READ),
                bitsProvider.getAggregatedPrivilegeNames(JCR_WRITE)));

        // read with same provider (i.e. reading from cache)
        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(JCR_READ, JCR_WRITE);
        assertEquals(expected, CollectionUtils.toSet(result));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesMixedBuiltIn() {
        Iterable<String> expected = CollectionUtils.toSet(Iterables.concat(
                Set.of(JCR_LOCK_MANAGEMENT),
                bitsProvider.getAggregatedPrivilegeNames(JCR_WRITE)));

        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(JCR_LOCK_MANAGEMENT, JCR_WRITE);
        assertEquals(expected, CollectionUtils.toSet(result));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesNonExistingTree() {
        Set<String> names = Set.of(JCR_LOCK_MANAGEMENT, JCR_READ_ACCESS_CONTROL);
        when(pTree.getProperty(REP_AGGREGATES)).thenReturn(PropertyStates.createProperty(REP_AGGREGATES, names, Type.NAMES));
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);

        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(KNOWN_PRIV_NAME);
        assertTrue(Iterables.isEmpty(result));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesMissingAggProperty() {
        when(pTree.exists()).thenReturn(true);
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);

        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(KNOWN_PRIV_NAME);
        assertTrue(Iterables.elementsEqual(ImmutableList.of(KNOWN_PRIV_NAME), result));
    }

    @Test
    public void testGetAggregatedPrivilegeNames() {
        Set<String> expected = Set.of(JCR_LOCK_MANAGEMENT, JCR_READ_ACCESS_CONTROL);
        when(pTree.getProperty(REP_AGGREGATES)).thenReturn(PropertyStates.createProperty(REP_AGGREGATES, expected, Type.NAMES));
        when(pTree.exists()).thenReturn(true);
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);

        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(KNOWN_PRIV_NAME);
        assertEquals(expected, CollectionUtils.toSet(result));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesNested() {
        Set<String> values = Set.of(JCR_READ, JCR_ADD_CHILD_NODES);
        when(pTree.getProperty(REP_AGGREGATES)).thenReturn(PropertyStates.createProperty(REP_AGGREGATES, values, Type.NAMES));
        when(pTree.exists()).thenReturn(true);
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);

        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(KNOWN_PRIV_NAME);
        Set<String> expected = Set.of(REP_READ_NODES, REP_READ_PROPERTIES, JCR_ADD_CHILD_NODES);
        assertEquals(expected, CollectionUtils.toSet(result));
    }

    @Test
    public void testGetAggregatedPrivilegeNamesNestedWithCache() {
        Set<String> values = Set.of(JCR_READ, JCR_ADD_CHILD_NODES);
        when(pTree.getProperty(REP_AGGREGATES)).thenReturn(PropertyStates.createProperty(REP_AGGREGATES, values, Type.NAMES));
        when(pTree.exists()).thenReturn(true);
        when(privTree.getChild(KNOWN_PRIV_NAME)).thenReturn(pTree);

        Iterable<String> result = bitsProvider.getAggregatedPrivilegeNames(KNOWN_PRIV_NAME);
        Set<String> expected = CollectionUtils.toSet(Iterables.concat(
                Set.of(JCR_ADD_CHILD_NODES),
                bitsProvider.getAggregatedPrivilegeNames(JCR_READ)));

        assertEquals(expected, CollectionUtils.toSet(result));
    }
}
