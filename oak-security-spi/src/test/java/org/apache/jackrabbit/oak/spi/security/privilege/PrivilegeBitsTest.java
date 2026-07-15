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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jcr.security.Privilege;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits.BUILT_IN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class PrivilegeBitsTest implements PrivilegeConstants {

    private static final long NO_PRIVILEGE = 0;
    private static final PrivilegeBits READ_NODES_PRIVILEGE_BITS = BUILT_IN.get(REP_READ_NODES);

    private static final long[] LONGS = new long[]{1, 2, 13, 199, 512, Long.MAX_VALUE / 2, Long.MAX_VALUE - 1, Long.MAX_VALUE};

    private static long getLongValue(PrivilegeBits bits) {
        String s = bits.toString();
        if (s.indexOf('[') == -1) {
            return Long.parseLong(s.substring(15));
        } else {
            return Long.parseLong(s.substring(s.indexOf('[') + 1, s.indexOf(',')));
        }
    }

    private static PropertyState createPropertyState(long l) {
        return PropertyStates.createProperty("name", Collections.singleton(l), Type.LONGS);
    }

    private static void assertEquivalent(PrivilegeBits a, PrivilegeBits b) {
        assertEquals(a.toString(), b.toString());
    }

    @Test
    public void testLongValue() {
        // empty
        assertEquals(NO_PRIVILEGE, getLongValue(PrivilegeBits.EMPTY));

        // long based privilege bits
        for (long l : LONGS) {
            PrivilegeBits bits = PrivilegeBits.getInstance(createPropertyState(l));
            assertEquals(l, getLongValue(bits));
        }

        // long based privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        long l = getLongValue(pb);
        while (l < Long.MAX_VALUE / 2) {
            l = l << 1;
            pb = pb.nextBits();
            assertEquals(l, getLongValue(pb));
        }

        // other privilege bits: long value not available.
        for (int i = 0; i < 10; i++) {
            pb = pb.nextBits();
            assertEquals(0, getLongValue(pb));
        }

        // modifiable privilege bits
        pb = READ_NODES_PRIVILEGE_BITS;
        for (int i = 0; i < 100; i++) {
            PrivilegeBits modifiable = PrivilegeBits.getInstance(pb);
            assertEquals(getLongValue(pb), getLongValue(modifiable));
            pb = pb.nextBits();
        }
    }

    @Test
    public void testNextBitsFromEmpty() {
        // empty
        assertSame(PrivilegeBits.EMPTY, PrivilegeBits.EMPTY.nextBits());

        PrivilegeBits bits = PrivilegeBits.getInstance().unmodifiable();
        assertSame(bits, bits.nextBits());
    }

    @Test
    public void testNextBits() {
        // long based privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        long l = getLongValue(pb);
        while (l < Long.MAX_VALUE / 2) {
            l = l << 1;
            pb = pb.nextBits();
            assertEquals(l, getLongValue(pb));
        }

        // other privilege bits: long value not available.
        for (int i = 0; i < 10; i++) {
            PrivilegeBits nxt = pb.nextBits();
            assertEquals(nxt, pb.nextBits());
            assertNotEquals(pb, nxt);
            pb = nxt;
        }

        // modifiable privilege bits
        pb = READ_NODES_PRIVILEGE_BITS;
        for (int i = 0; i < 100; i++) {
            PrivilegeBits modifiable = PrivilegeBits.getInstance(pb);
            try {
                modifiable.nextBits();
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }
            pb = pb.nextBits();
        }
    }

    @Test
    public void testUnmodifiable() {
        assertSame(PrivilegeBits.EMPTY, PrivilegeBits.EMPTY.unmodifiable());

        // other privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        PrivilegeBits mod = PrivilegeBits.getInstance(pb);

        for (int i = 0; i < 100; i++) {
            PrivilegeBits nxt = pb.nextBits();
            assertSame(nxt, nxt.unmodifiable());
            assertEquals(nxt, nxt.unmodifiable());

            mod.add(nxt);
            assertNotSame(mod, mod.unmodifiable());

            pb = nxt;
        }
    }

    @Test
    public void testModifiable() {
        assertNotSame(PrivilegeBits.EMPTY, PrivilegeBits.EMPTY.modifiable());

        // other privilege bits
        PrivilegeBits mod = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);

        assertSame(mod, mod.modifiable());
        assertNotSame(mod, mod.unmodifiable());
        assertNotEquals(mod, mod.unmodifiable());
    }

    @Test
    public void testIncludes() {
        // empty
        assertTrue(PrivilegeBits.EMPTY.includes(PrivilegeBits.EMPTY));

        // other privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        PrivilegeBits mod = PrivilegeBits.getInstance();

        for (int i = 0; i < 200; i++) {

            assertFalse(PrivilegeBits.EMPTY.includes(pb));
            assertTrue(pb.includes(PrivilegeBits.EMPTY));

            mod.add(pb);
            assertTrue(mod.includes(pb));

            PrivilegeBits nxt = pb.nextBits();
            assertTrue(nxt.includes(nxt));
            assertTrue(nxt.includes(PrivilegeBits.getInstance(nxt)));

            assertFalse(pb + " should not include " + nxt, pb.includes(nxt));
            assertFalse(nxt + " should not include " + pb, nxt.includes(pb));
            assertFalse(mod.includes(nxt));
            assertFalse(nxt.includes(mod));

            pb = nxt;
        }
    }

    @Test
    public void testBuiltIn() {
        for (PrivilegeBits bits : BUILT_IN.values()) {
            assertTrue(bits.isBuiltin());
            assertTrue(PrivilegeBits.getInstance(bits).isBuiltin());
        }
    }

    @Test
    public void testCombinationNotBuiltIn() {
        PrivilegeBits combination = PrivilegeBits.getInstance();
        for (PrivilegeBits bits : BUILT_IN.values()) {
            combination.add(bits);
        }
        assertFalse(combination.isBuiltin());
    }

    @Test
    public void testNextNotBuiltIn() {
        assertFalse(PrivilegeBits.getInstance(PrivilegeBits.NEXT_AFTER_BUILT_INS).isBuiltin());
    }

    @Test
    public void testCombinationWithNextNotBuiltIn() {
        PrivilegeBits bits = PrivilegeBits.NEXT_AFTER_BUILT_INS;
        PrivilegeBits toTest = PrivilegeBits.getInstance(BUILT_IN.get(PrivilegeConstants.JCR_READ));

        for (int i = 0; i<100; i++) {
            bits = bits.nextBits();
            assertFalse(toTest.add(bits).isBuiltin());
        }
    }

    @Test
    public void testIsEmpty() {
        // empty
        assertTrue(PrivilegeBits.EMPTY.isEmpty());
    }

    @Test
    public void testNotIsEmpty() {
        // any other bits should not be empty
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        PrivilegeBits mod = PrivilegeBits.getInstance(pb);
        for (int i = 0; i < 100; i++) {
            assertFalse(pb.isEmpty());
            assertFalse(PrivilegeBits.getInstance(pb).isEmpty());

            pb = pb.nextBits();
            mod.add(pb);
            assertFalse(mod.isEmpty());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEmptyAdd() {
        // empty privilege bits
        PrivilegeBits.EMPTY.add(PrivilegeBits.EMPTY);
    }

    @Test
    public void testAdd() {
        // other privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        PrivilegeBits mod = PrivilegeBits.getInstance(pb);

        for (int i = 0; i < 100; i++) {
            PrivilegeBits nxt = pb.nextBits();

            long before = getLongValue(mod);
            long nxtLong = getLongValue(nxt);

            mod.add(nxt);
            if (getLongValue(nxt) != 0) {
                assertEquals(before | nxtLong, getLongValue(mod));
            }
            assertTrue(mod.includes(nxt));

            PrivilegeBits tmp = PrivilegeBits.getInstance(pb);
            assertTrue(tmp.includes(pb));
            assertFalse(tmp.includes(nxt));
            if (READ_NODES_PRIVILEGE_BITS.equals(pb)) {
                assertTrue(tmp.includes(BUILT_IN.get(PrivilegeConstants.REP_READ_NODES)));
            } else {
                assertFalse(tmp.includes(BUILT_IN.get(PrivilegeConstants.REP_READ_NODES)));
            }
            tmp.add(nxt);
            assertTrue(tmp.includes(pb) && tmp.includes(nxt));
            if (READ_NODES_PRIVILEGE_BITS.equals(pb)) {
                assertTrue(tmp.includes(READ_NODES_PRIVILEGE_BITS));
            } else {
                assertFalse(tmp.includes(READ_NODES_PRIVILEGE_BITS));
            }
            tmp.add(READ_NODES_PRIVILEGE_BITS);
            assertTrue(tmp.includes(READ_NODES_PRIVILEGE_BITS));

            pb = nxt;
        }
    }

    @Test
    public void testAddSame() {
        PrivilegeBits mod = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        assertSame(mod, mod.add(mod));
    }

    @Test
    public void testAddEquivalent() {
        PrivilegeBits mod = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        assertSame(mod, mod.add(READ_NODES_PRIVILEGE_BITS));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddSameToUnmodifiable() {
        READ_NODES_PRIVILEGE_BITS.add(READ_NODES_PRIVILEGE_BITS);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddNextToUnmodifiable() {
        READ_NODES_PRIVILEGE_BITS.add(READ_NODES_PRIVILEGE_BITS.nextBits());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddEmptyToUnmodifiable() {
        READ_NODES_PRIVILEGE_BITS.nextBits().add(PrivilegeBits.EMPTY);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddModifiableToUnmodifiable() {
        PrivilegeBits pb = PrivilegeBits.getInstance(PropertyStates.createProperty("any", List.of(1L, 16L, 512L), Type.LONGS));
        pb.unmodifiable().add(READ_NODES_PRIVILEGE_BITS.modifiable());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEmptyDiff() {
        // empty privilege bits
        PrivilegeBits.EMPTY.diff(PrivilegeBits.EMPTY);
    }

    @Test
    public void testDiff() {
        // other privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        PrivilegeBits mod = PrivilegeBits.getInstance(pb);

        for (int i = 0; i < 100; i++) {
            PrivilegeBits nxt = pb.nextBits();
            try {
                pb.diff(nxt);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }

            try {
                pb.diff(mod);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }

            PrivilegeBits before = PrivilegeBits.getInstance(mod);
            mod.diff(nxt);
            assertEquivalent(before, mod);
            mod.add(nxt);
            assertNotEquals(before, mod);
            mod.diff(nxt);
            assertEquivalent(before, mod);
            mod.add(nxt);

            // diff with same pb must leave original bits empty
            PrivilegeBits tmp = PrivilegeBits.getInstance(pb);
            tmp.add(nxt);
            tmp.add(READ_NODES_PRIVILEGE_BITS);
            tmp.diff(tmp);
            assertEquivalent(PrivilegeBits.EMPTY, tmp);

            tmp = PrivilegeBits.getInstance(pb);
            tmp.add(nxt);
            tmp.add(READ_NODES_PRIVILEGE_BITS);
            tmp.diff(PrivilegeBits.getInstance(tmp));
            assertEquivalent(PrivilegeBits.EMPTY, tmp);

            // diff without intersection -> leave privilege unmodified.
            tmp = PrivilegeBits.getInstance(pb);
            tmp.diff(nxt);
            assertEquivalent(PrivilegeBits.getInstance(pb), tmp);

            // diff with intersection -> privilege must be modified accordingly.
            tmp = PrivilegeBits.getInstance(nxt);
            tmp.add(READ_NODES_PRIVILEGE_BITS);
            assertTrue(tmp.includes(READ_NODES_PRIVILEGE_BITS));
            assertTrue(tmp.includes(nxt));
            tmp.diff(nxt);
            assertEquivalent(READ_NODES_PRIVILEGE_BITS, tmp);
            assertTrue(tmp.includes(READ_NODES_PRIVILEGE_BITS));
            assertFalse(tmp.includes(nxt));

            tmp = PrivilegeBits.getInstance(pb);
            tmp.add(READ_NODES_PRIVILEGE_BITS);
            PrivilegeBits tmp2 = PrivilegeBits.getInstance(pb);
            tmp2.add(nxt);
            PrivilegeBits tmp3 = PrivilegeBits.getInstance(tmp2);
            assertEquivalent(tmp2, tmp3);
            tmp.diff(tmp2);
            if (READ_NODES_PRIVILEGE_BITS.equals(pb)) {
                assertEquivalent(PrivilegeBits.EMPTY, tmp);
            } else {
                assertEquivalent(READ_NODES_PRIVILEGE_BITS, tmp);
            }
            // but pb passed to the diff call must not be modified.
            assertEquivalent(tmp3, tmp2);

            pb = nxt;
        }
    }

    @Test
    public void testDiffEquivalent() {
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        for (int i = 0; i < 100; i++) {
            assertTrue(pb.toString(), PrivilegeBits.getInstance(pb).diff(pb).isEmpty());
            pb = pb.nextBits();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testEmptyAddDifference() {
        // empty privilege bits
        PrivilegeBits.EMPTY.addDifference(PrivilegeBits.EMPTY, PrivilegeBits.EMPTY);
    }

    @Test
    public void testAddDifference() {
        // other privilege bits
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        PrivilegeBits mod = PrivilegeBits.getInstance(pb);

        for (int i = 0; i < 100; i++) {
            PrivilegeBits nxt = pb.nextBits();
            try {
                pb.addDifference(nxt, mod);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }

            try {
                pb.addDifference(nxt, READ_NODES_PRIVILEGE_BITS);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }

            PrivilegeBits tmp = PrivilegeBits.getInstance(mod);
            tmp.addDifference(nxt, READ_NODES_PRIVILEGE_BITS);
            mod.add(nxt);
            assertEquivalent(mod, tmp); // since there is diff(nxt, read) which results in nxt

            if (!pb.equals(READ_NODES_PRIVILEGE_BITS)) {
                tmp = PrivilegeBits.getInstance(nxt);
                PrivilegeBits mod2 = PrivilegeBits.getInstance(mod);
                tmp.addDifference(mod2, READ_NODES_PRIVILEGE_BITS);
                assertNotEquals(nxt, tmp);  // tmp should be modified by addDifference call.
                assertEquivalent(mod2, mod);       // mod2 should not be modified here
                assertTrue(tmp.includes(pb));
                assertFalse(tmp.includes(READ_NODES_PRIVILEGE_BITS));
                assertFalse(tmp.includes(mod));
            }

            tmp = PrivilegeBits.getInstance(nxt);
            PrivilegeBits mod2 = PrivilegeBits.getInstance(mod);
            tmp.addDifference(READ_NODES_PRIVILEGE_BITS, mod2);
            assertEquivalent(nxt, tmp);  // tmp not modified by addDifference call.
            assertEquivalent(mod2, mod); // mod2 should not be modified here
            assertFalse(tmp.includes(pb));
            assertFalse(tmp.includes(READ_NODES_PRIVILEGE_BITS));
            assertFalse(tmp.includes(mod));

            tmp = PrivilegeBits.getInstance(nxt);
            tmp.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
            assertEquivalent(nxt, tmp);  // tmp not modified by addDifference call.
            assertFalse(tmp.includes(READ_NODES_PRIVILEGE_BITS));

            tmp = PrivilegeBits.getInstance(mod);
            tmp.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
            assertEquivalent(mod, tmp);  // tmp not modified by addDifference call.
            assertTrue(tmp.includes(READ_NODES_PRIVILEGE_BITS));

            pb = nxt;
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetainUnmodifiable() {
        READ_NODES_PRIVILEGE_BITS.retain(PrivilegeBits.getInstance());
    }

    @Test
    public void testRetainSimple() {
        PrivilegeBits pb = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        assertEquals(pb, pb.retain(pb));
        assertEquals(pb, pb.retain(READ_NODES_PRIVILEGE_BITS));

        pb = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        pb.retain(PrivilegeBits.getInstance());
        assertTrue(pb.isEmpty());

        pb = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        pb.retain(PrivilegeBits.EMPTY);
        assertTrue(pb.isEmpty());

        PrivilegeBits write = BUILT_IN.get(PrivilegeBits.REP_WRITE);
        pb = PrivilegeBits.getInstance().add(write);
        assertEquals(pb, pb.retain(pb));
        assertEquals(pb, pb.retain(write));

        pb.retain(READ_NODES_PRIVILEGE_BITS);
        assertTrue(pb.isEmpty());

        pb.add(READ_NODES_PRIVILEGE_BITS).add(write);
        pb.retain(write);
        assertEquivalent(write, pb);
        assertFalse(pb.includes(READ_NODES_PRIVILEGE_BITS));

        PrivilegeBits lock = BUILT_IN.get(PrivilegeBits.JCR_LOCK_MANAGEMENT);
        PrivilegeBits lw = PrivilegeBits.getInstance(write, lock);

        pb.add(READ_NODES_PRIVILEGE_BITS).add(write).add(lock);
        pb.retain(lw);
        assertEquivalent(lw, pb);
        assertFalse(pb.includes(READ_NODES_PRIVILEGE_BITS));
    }

    @Test
    public void testRetain() {
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        List<PrivilegeBits> pbs = new ArrayList<>();
        pbs.add(pb);
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            PrivilegeBits nxt = pb.nextBits();

            PrivilegeBits mod = PrivilegeBits.getInstance(nxt, pb);
            mod.retain(nxt);
            assertEquivalent(nxt, mod);

            mod = PrivilegeBits.getInstance(nxt);
            mod.retain(pb);
            assertTrue(nxt.toString(), mod.isEmpty());

            mod = PrivilegeBits.getInstance(nxt);
            mod.retain(READ_NODES_PRIVILEGE_BITS);
            assertTrue(nxt.toString(), mod.isEmpty());

            mod = PrivilegeBits.getInstance(nxt, READ_NODES_PRIVILEGE_BITS);
            mod.retain(nxt);
            assertEquivalent(nxt, mod);

            mod = PrivilegeBits.getInstance(nxt, pb, READ_NODES_PRIVILEGE_BITS);
            mod.retain(READ_NODES_PRIVILEGE_BITS);
            assertEquivalent(READ_NODES_PRIVILEGE_BITS, mod);

            mod = PrivilegeBits.getInstance(nxt);
            PrivilegeBits other = PrivilegeBits.getInstance();
            for (int j = 0; j < pbs.size()/2; j++) {
                other.add(pbs.get(random.nextInt(pbs.size()-1)));
            }
            mod.add(other);
            mod.retain(other);
            assertEquivalent(other, mod);

            other.retain(nxt);
            assertTrue(other.isEmpty());

            pbs.add(nxt);
            pb = nxt;
        }
    }

    @Test
    public void testWriteToTree() {
        Tree tree = when(mock(Tree.class).getName()).thenReturn("anyName").getMock();

        PrivilegeBits bits = READ_NODES_PRIVILEGE_BITS;
        bits.writeTo(tree);

        Mockito.verify(tree, times(1)).setProperty(bits.asPropertyState(REP_BITS));
    }

    @Test
    public void testWriteToPrivilegesRootTree() {
        Tree tree = when(mock(Tree.class).getName()).thenReturn(REP_PRIVILEGES).getMock();

        PrivilegeBits bits = READ_NODES_PRIVILEGE_BITS;
        bits.writeTo(tree);

        Mockito.verify(tree, times(1)).setProperty(bits.asPropertyState(REP_NEXT));
    }

    @Test
    public void testGetInstance() {
        PrivilegeBits pb = PrivilegeBits.getInstance();
        assertEquivalent(PrivilegeBits.EMPTY, pb);
        assertNotSame(PrivilegeBits.EMPTY, pb);
        assertNotSame(pb, pb.unmodifiable());
        pb.add(READ_NODES_PRIVILEGE_BITS);
        pb.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
        pb.diff(READ_NODES_PRIVILEGE_BITS);

        pb = PrivilegeBits.getInstance(PrivilegeBits.EMPTY);
        assertEquivalent(PrivilegeBits.EMPTY, pb);
        assertNotSame(PrivilegeBits.EMPTY, pb);
        assertNotSame(pb, pb.unmodifiable());
        pb.add(READ_NODES_PRIVILEGE_BITS);
        pb.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
        pb.diff(READ_NODES_PRIVILEGE_BITS);

        pb = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        assertEquivalent(READ_NODES_PRIVILEGE_BITS, pb);
        assertNotSame(READ_NODES_PRIVILEGE_BITS, pb);
        assertNotSame(pb, pb.unmodifiable());
        pb.add(READ_NODES_PRIVILEGE_BITS);
        pb.addDifference(READ_NODES_PRIVILEGE_BITS, PrivilegeBits.EMPTY);
        pb.diff(READ_NODES_PRIVILEGE_BITS);

        pb = PrivilegeBits.EMPTY;
        assertEquivalent(pb, PrivilegeBits.EMPTY);
        assertSame(PrivilegeBits.EMPTY, pb);
        assertSame(pb, pb.unmodifiable());
        try {
            pb.add(READ_NODES_PRIVILEGE_BITS);
            fail("UnsupportedOperation expected");
        } catch (UnsupportedOperationException e) {
            // success
        }
        try {
            pb.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
            fail("UnsupportedOperation expected");
        } catch (UnsupportedOperationException e) {
            // success
        }
        try {
            pb.diff(READ_NODES_PRIVILEGE_BITS);
            fail("UnsupportedOperation expected");
        } catch (UnsupportedOperationException e) {
            // success
        }
    }

    @Test
    public void testGetInstanceFromBase() {
        PrivilegeBits pb = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS);
        pb.add(BUILT_IN.get(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        pb.add(BUILT_IN.get(PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT));

        PrivilegeBits pb2 = PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS,
                BUILT_IN.get(PrivilegeConstants.JCR_READ_ACCESS_CONTROL),
                BUILT_IN.get(PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT));

        assertEquivalent(pb, pb2);
    }

    @Test
    public void testGetInstanceFromPropertyState() {
        for (long l : LONGS) {
            PropertyState property = createPropertyState(l);
            PrivilegeBits pb = PrivilegeBits.getInstance(property);
            assertEquivalent(pb, PrivilegeBits.getInstance(property));
            assertSame(pb, pb.unmodifiable());

            assertEquivalent(pb, PrivilegeBits.getInstance(pb));
            assertEquivalent(PrivilegeBits.getInstance(pb), pb);
            assertNotSame(pb, PrivilegeBits.getInstance(pb));

            try {
                pb.add(READ_NODES_PRIVILEGE_BITS);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }
            try {
                pb.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }
            try {
                pb.diff(READ_NODES_PRIVILEGE_BITS);
                fail("UnsupportedOperation expected");
            } catch (UnsupportedOperationException e) {
                // success
            }
        }
    }

    @Test
    public void testGetInstanceFromMvPropertyState() {
        PropertyState property = PropertyStates.createProperty("name", Set.of(Long.MAX_VALUE, Long.MIN_VALUE / 2), Type.LONGS);

        PrivilegeBits pb = PrivilegeBits.getInstance(property);

        assertEquivalent(pb, PrivilegeBits.getInstance(property));
        assertSame(pb, pb.unmodifiable());

        assertEquivalent(pb, PrivilegeBits.getInstance(pb));
        assertEquivalent(PrivilegeBits.getInstance(pb), pb);
        assertNotSame(pb, PrivilegeBits.getInstance(pb));

        try {
            pb.add(READ_NODES_PRIVILEGE_BITS);
            fail("UnsupportedOperation expected");
        } catch (UnsupportedOperationException e) {
            // success
        }
        try {
            pb.addDifference(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
            fail("UnsupportedOperation expected");
        } catch (UnsupportedOperationException e) {
            // success
        }
        try {
            pb.diff(READ_NODES_PRIVILEGE_BITS);
            fail("UnsupportedOperation expected");
        } catch (UnsupportedOperationException e) {
            // success
        }
    }

    @Test
    public void testGetInstanceFromNullPropertyState() {
        assertSame(PrivilegeBits.EMPTY, PrivilegeBits.getInstance((PropertyState) null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetInstanceFromNegativeLong() {
        PrivilegeBits.getInstance(PropertyStates.createProperty("name", Collections.singleton(NO_PRIVILEGE-1), Type.LONGS));
    }

    @Test
    public void testGetInstanceFromTreeCustomPriv() {
        PrivilegeBits next = PrivilegeBits.NEXT_AFTER_BUILT_INS;

        Tree tmp = mock(Tree.class);
        when(tmp.getName()).thenReturn("tmpPrivilege");
        when(tmp.getProperty(REP_BITS)).thenReturn(next.asPropertyState(REP_BITS));

        assertEquals(next, PrivilegeBits.getInstance(tmp));
    }

    @Test
    public void testGetInstanceFromTreeJcrRead() {
        Tree readPrivTree = mock(Tree.class);
        when(readPrivTree.getName()).thenReturn(JCR_READ);

        assertEquals(BUILT_IN.get(JCR_READ), PrivilegeBits.getInstance(readPrivTree));
    }

    @Test
    public void testGetInstanceFromNullTree() {
        assertSame(PrivilegeBits.EMPTY, PrivilegeBits.getInstance((Tree) null));
    }

    @Test
    public void testGetInstanceFromPrivilegesRootTree() {
        Tree tree = when(mock(Tree.class).getName()).thenReturn(REP_PRIVILEGES).getMock();

        PrivilegeBits.getInstance(tree);
        Mockito.verify(tree, times(1)).getProperty(REP_NEXT);
    }

    @Test
    public void testCalculatePermissionsFromSimpleBuiltIn() {
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(mock(Root.class));
        Map<PrivilegeBits, Long> simple = new HashMap<>();
        simple.put(PrivilegeBits.EMPTY, Permissions.NO_PERMISSION);
        simple.put(PrivilegeBits.BUILT_IN.get(JCR_READ), Permissions.READ);
        simple.put(PrivilegeBits.BUILT_IN.get(JCR_LOCK_MANAGEMENT), Permissions.LOCK_MANAGEMENT);
        simple.put(PrivilegeBits.BUILT_IN.get(JCR_VERSION_MANAGEMENT), Permissions.VERSION_MANAGEMENT);
        simple.put(PrivilegeBits.BUILT_IN.get(JCR_READ_ACCESS_CONTROL), Permissions.READ_ACCESS_CONTROL);
        simple.put(provider.getBits(JCR_MODIFY_ACCESS_CONTROL), Permissions.MODIFY_ACCESS_CONTROL);
        simple.put(provider.getBits(REP_READ_NODES), Permissions.READ_NODE);
        simple.put(provider.getBits(REP_READ_PROPERTIES), Permissions.READ_PROPERTY);
        simple.put(provider.getBits(REP_USER_MANAGEMENT), Permissions.USER_MANAGEMENT);
        simple.put(provider.getBits(JCR_MODIFY_PROPERTIES), Permissions.SET_PROPERTY);
        simple.put(provider.getBits(REP_ADD_PROPERTIES), Permissions.ADD_PROPERTY);
        simple.put(provider.getBits(REP_ALTER_PROPERTIES), Permissions.MODIFY_PROPERTY);
        simple.put(provider.getBits(REP_REMOVE_PROPERTIES), Permissions.REMOVE_PROPERTY);
        simple.put(provider.getBits(REP_INDEX_DEFINITION_MANAGEMENT), Permissions.INDEX_DEFINITION_MANAGEMENT);
        simple.put(provider.getBits(JCR_NODE_TYPE_DEFINITION_MANAGEMENT), Permissions.NODE_TYPE_DEFINITION_MANAGEMENT);
        simple.put(provider.getBits(JCR_NAMESPACE_MANAGEMENT), Permissions.NAMESPACE_MANAGEMENT);
        simple.put(provider.getBits(REP_PRIVILEGE_MANAGEMENT), Permissions.PRIVILEGE_MANAGEMENT);
        simple.put(provider.getBits(JCR_RETENTION_MANAGEMENT), Permissions.RETENTION_MANAGEMENT);
        simple.put(provider.getBits(JCR_LIFECYCLE_MANAGEMENT), Permissions.LIFECYCLE_MANAGEMENT);
        simple.put(provider.getBits(JCR_NODE_TYPE_MANAGEMENT), Permissions.NODE_TYPE_MANAGEMENT);
        simple.put(provider.getBits(JCR_WORKSPACE_MANAGEMENT), Permissions.WORKSPACE_MANAGEMENT);
        Map.Entry<PrivilegeBits, Long> previous = null;
        for (Map.Entry<PrivilegeBits, Long> entry : simple.entrySet()) {
            long expected = entry.getValue();
            PrivilegeBits pb = entry.getKey();
            assertEquals(expected, PrivilegeBits.calculatePermissions(pb, PrivilegeBits.EMPTY, true));
            assertEquals(expected, PrivilegeBits.calculatePermissions(pb, pb, false));
            if (previous != null) {
                assertEquals(expected, PrivilegeBits.calculatePermissions(pb, previous.getKey(), false));
                assertEquals(expected|previous.getValue(), PrivilegeBits.calculatePermissions(PrivilegeBits.getInstance(pb, previous.getKey()), PrivilegeBits.EMPTY, true));
            }
            previous = entry;
        }
    }

    @Test
    public void testCalculatePermissionsModifyPropertyAggregated() {
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(mock(Root.class));

        // jcr:modifyProperty aggregate
        PrivilegeBits add_change = provider.getBits(REP_ADD_PROPERTIES, REP_ALTER_PROPERTIES);
        long permissions = (Permissions.ADD_PROPERTY | Permissions.MODIFY_PROPERTY);
        assertEquals(permissions, PrivilegeBits.calculatePermissions(add_change, PrivilegeBits.EMPTY, true));
        assertEquals(permissions, PrivilegeBits.calculatePermissions(add_change, add_change, true));

        PrivilegeBits add_rm = provider.getBits(REP_ADD_PROPERTIES, REP_REMOVE_PROPERTIES);
        permissions = (Permissions.ADD_PROPERTY | Permissions.REMOVE_PROPERTY);
        assertEquals(permissions, PrivilegeBits.calculatePermissions(add_rm, PrivilegeBits.EMPTY, true));
        assertEquals(permissions, PrivilegeBits.calculatePermissions(add_rm, add_rm, true));

        PrivilegeBits ch_rm = provider.getBits(REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES);
        permissions = (Permissions.MODIFY_PROPERTY | Permissions.REMOVE_PROPERTY);
        assertEquals(permissions, PrivilegeBits.calculatePermissions(ch_rm, PrivilegeBits.EMPTY, true));
        assertEquals(permissions, PrivilegeBits.calculatePermissions(ch_rm, add_rm, true));
    }

    @Test
    public void testCalculatePermissionsParentAwareAllow() {
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(mock(Root.class));

        // parent aware permissions
        // a) jcr:addChildNodes
        PrivilegeBits addChild = provider.getBits(JCR_ADD_CHILD_NODES);
        assertNotEquals(Permissions.ADD_NODE, PrivilegeBits.calculatePermissions(addChild, PrivilegeBits.EMPTY, true));
        assertEquals(Permissions.ADD_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, addChild, true));

        // b) jcr:removeChildNodes and jcr:removeNode
        PrivilegeBits removeChild = provider.getBits(JCR_REMOVE_CHILD_NODES);
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(removeChild, PrivilegeBits.EMPTY, true));
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, removeChild, true));

        PrivilegeBits removeNode = provider.getBits(JCR_REMOVE_NODE);
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(removeNode, PrivilegeBits.EMPTY, true));
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, removeNode, true));

        PrivilegeBits remove = provider.getBits(JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE);
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(remove, PrivilegeBits.EMPTY, true));
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, remove, true));
        assertEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(remove, remove, true));
    }

    @Test
    public void testCalculatePermissionsParentAwareDeny() {
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(mock(Root.class));

        // parent aware permissions
        // a) jcr:addChildNodes
        PrivilegeBits addChild = provider.getBits(JCR_ADD_CHILD_NODES);
        assertNotEquals(Permissions.ADD_NODE, PrivilegeBits.calculatePermissions(addChild, PrivilegeBits.EMPTY, false));
        assertEquals(Permissions.ADD_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, addChild, false));

        // b) jcr:removeChildNodes and jcr:removeNode
        PrivilegeBits removeChild = provider.getBits(JCR_REMOVE_CHILD_NODES);
        assertEquals(Permissions.NO_PERMISSION, PrivilegeBits.calculatePermissions(removeChild, PrivilegeBits.EMPTY, false));
        assertEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, removeChild, false));

        PrivilegeBits removeNode = provider.getBits(JCR_REMOVE_NODE);
        assertEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(removeNode, PrivilegeBits.EMPTY, false));
        assertNotEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, removeNode, false));

        PrivilegeBits remove = provider.getBits(JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE);
        assertEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(remove, PrivilegeBits.EMPTY, false));
        assertEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(PrivilegeBits.EMPTY, remove, false));
        assertEquals(Permissions.REMOVE_NODE, PrivilegeBits.calculatePermissions(remove, remove, false));
    }

    @Test
    public void testCalculatePermissionsAddAndRemoveChild() {
        PrivilegeBits addRemoveChild = PrivilegeBits.getInstance(BUILT_IN.get(JCR_ADD_CHILD_NODES), BUILT_IN.get(JCR_REMOVE_CHILD_NODES));
        assertEquals(Permissions.MODIFY_CHILD_NODE_COLLECTION, PrivilegeBits.calculatePermissions(addRemoveChild, PrivilegeBits.EMPTY, true));
        assertEquals(Permissions.MODIFY_CHILD_NODE_COLLECTION, PrivilegeBits.calculatePermissions(addRemoveChild, PrivilegeBits.EMPTY, false));
    }

    @Test
    public void testEquals() {
        assertEquals(READ_NODES_PRIVILEGE_BITS, READ_NODES_PRIVILEGE_BITS);
        assertEquals(READ_NODES_PRIVILEGE_BITS, PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS).unmodifiable());

        assertNotEquals(READ_NODES_PRIVILEGE_BITS, PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS));
        assertNotEquals(PrivilegeBits.getInstance(READ_NODES_PRIVILEGE_BITS), READ_NODES_PRIVILEGE_BITS);
        assertNotEquals(READ_NODES_PRIVILEGE_BITS, when(mock(Privilege.class).getName()).thenReturn(REP_READ_NODES).getMock());
        assertNotEquals(READ_NODES_PRIVILEGE_BITS, null);
    }

    @Test
    public void testHashCode() {
        PrivilegeBits pb = READ_NODES_PRIVILEGE_BITS;
        for (int i = 0; i < 100; i++) {
            PrivilegeBits modifiable = PrivilegeBits.getInstance(pb);
            assertEquals(pb.hashCode(), modifiable.unmodifiable().hashCode());
            assertNotEquals(pb.hashCode(), modifiable.hashCode());

            PrivilegeBits nxt = pb.nextBits();
            assertNotEquals(pb.hashCode(), nxt.hashCode());

            pb = nxt;
        }
    }
}