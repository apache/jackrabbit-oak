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

import org.apache.jackrabbit.guava.common.primitives.Longs;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.jackrabbit.oak.spi.security.privilege.JcrAllUtil.DYNAMIC_JCR_ALL_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;

public class JcrAllUtilTest implements PrivilegeConstants {

    private static final Long ALL = Long.MAX_VALUE;

    private static final PropertyState ALL_PROPERTY = PropertyStates.createProperty(REP_BITS, Longs.asList(Long.MAX_VALUE), Type.LONGS);
    private final PrivilegeBits ALL_BITS = PrivilegeBits.getInstance(ALL_PROPERTY);

    private static final PropertyState DYNAMIC_ALL_PROPERTY = PropertyStates.createProperty("anyName", Longs.asList(DYNAMIC_JCR_ALL_VALUE), Type.LONGS);

    private Tree privTree;
    private Tree jcrAllDefTree;
    private Root root;
    private PrivilegeBitsProvider bitsProvider;

    @Before
    public void before() {
        jcrAllDefTree = Mockito.mock(Tree.class);
        when(jcrAllDefTree.exists()).thenReturn(true);
        when(jcrAllDefTree.getName()).thenReturn(JCR_ALL);
        when(jcrAllDefTree.getProperty(REP_BITS)).thenReturn(ALL_PROPERTY);

        privTree = Mockito.mock(Tree.class);
        when(privTree.exists()).thenReturn(true);
        when(privTree.hasChild(JCR_ALL)).thenReturn(true);
        when(privTree.getChild(JCR_ALL)).thenReturn(jcrAllDefTree);

        root = Mockito.mock(Root.class);
        when(root.getTree(PRIVILEGES_PATH)).thenReturn(privTree);

        bitsProvider = new PrivilegeBitsProvider(root);
    }

    @Test
    public void testGetInstanceBuiltin() {
        PrivilegeBits readBits = PrivilegeBits.BUILT_IN.get(REP_READ_NODES);
        PropertyState propState = readBits.asPropertyState(("anyName"));

        assertSame(readBits, JcrAllUtil.getPrivilegeBits(propState, bitsProvider));
    }

    @Test
    public void testAsPropertyStateBuiltin() {
        PrivilegeBits readBits = PrivilegeBits.BUILT_IN.get(REP_READ_NODES);
        PropertyState propertyState = JcrAllUtil.asPropertyState("anyName", readBits, bitsProvider);

        assertEquals(readBits.asPropertyState("anyName"), propertyState);
    }

    @Test
    public void testGetInstanceCombined() {
        PrivilegeBits bits = PrivilegeBits.getInstance();
        bits.add(PrivilegeBits.BUILT_IN.get(REP_READ_NODES));
        bits.add(PrivilegeBits.BUILT_IN.get(REP_WRITE));
        PropertyState propState = bits.asPropertyState("anyName");

        assertEquals(bits.unmodifiable(), JcrAllUtil.getPrivilegeBits(propState, bitsProvider));
    }

    @Test
    public void testAsPropertyStateCombined() {
        PrivilegeBits bits = PrivilegeBits.getInstance();
        bits.add(PrivilegeBits.BUILT_IN.get(REP_READ_NODES));
        bits.add(PrivilegeBits.BUILT_IN.get(REP_WRITE));
        PropertyState expected = bits.asPropertyState("anyName");

        assertEquals(expected, JcrAllUtil.asPropertyState("anyName", bits, bitsProvider));
    }

    @Test
    public void testGetInstanceDynamicAll() {
        assertEquals(ALL_BITS, JcrAllUtil.getPrivilegeBits(DYNAMIC_ALL_PROPERTY, bitsProvider));
    }

    @Test
    public void testAsPropertyStateDynamicAll() {
        assertEquals(DYNAMIC_ALL_PROPERTY, JcrAllUtil.asPropertyState("anyName", ALL_BITS, bitsProvider));
    }

    @Test
    public void testDenotesDynamicAllNullPropertyState() {
        assertFalse(JcrAllUtil.denotesDynamicJcrAll(null));
    }

    @Test
    public void testDenotesDynamicAllNotLongsPropertyState() {
        assertFalse(JcrAllUtil.denotesDynamicJcrAll(PropertyStates.createProperty("any", "String")));
        assertFalse(JcrAllUtil.denotesDynamicJcrAll(PropertyStates.createProperty("any",  List.of("mv", "strings"), Type.STRINGS)));
        assertFalse(JcrAllUtil.denotesDynamicJcrAll(PropertyStates.createProperty("any", "-1")));
    }

    @Test
    public void testDenotesDynamicAllMVLongPropertyState() {
        assertFalse(JcrAllUtil.denotesDynamicJcrAll(PropertyStates.createProperty("any", List.of(-1, 2, 3), Type.LONGS)));
    }

    @Test
    public void testDenotesDynamicAllSingleLongPropertyState() {
        assertFalse(JcrAllUtil.denotesDynamicJcrAll(PropertyStates.createProperty("any", DYNAMIC_JCR_ALL_VALUE)));
    }

    @Test
    public void testDenotesDynamicAll() {
        assertTrue(JcrAllUtil.denotesDynamicJcrAll(PropertyStates.createProperty("any", List.of(DYNAMIC_JCR_ALL_VALUE), Type.LONGS)));
    }
}