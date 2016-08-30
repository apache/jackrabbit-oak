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
package org.apache.jackrabbit.oak.plugins.document;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class RevisionVectorTest {

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgument() {
        Revision rev1 = new Revision(1, 0, 1);
        new RevisionVector(rev1, rev1);
    }

    @Test
    public void construct() {
        RevisionVector rv = new RevisionVector();
        assertEquals(newHashSet(), newHashSet(rv));

        Revision rev1 = new Revision(1, 0, 1);
        Revision rev2 = new Revision(1, 0, 2);
        rv = new RevisionVector(newHashSet(rev1, rev2));
        assertEquals(newHashSet(rev1, rev2), newHashSet(rv));

        rv = new RevisionVector(Lists.newArrayList(rev1, rev2));
        assertEquals(newHashSet(rev1, rev2), newHashSet(rv));
    }

    @Test
    public void update() {
        Revision rev1 = new Revision(1, 0, 1);
        RevisionVector rv = new RevisionVector(rev1);
        assertEquals(1, Iterables.size(rv));
        assertSame(rv, rv.update(rev1));

        Revision rev2 = new Revision(2, 0, 1);
        rv = rv.update(rev2);
        assertEquals(newHashSet(rev2), newHashSet(rv));

        Revision rev3 = new Revision(3, 0, 2);
        rv = rv.update(rev3);
        assertEquals(newHashSet(rev2, rev3), newHashSet(rv));

        rev3 = rev3.asBranchRevision();
        rv = rv.update(rev3);
        assertEquals(newHashSet(rev2, rev3), newHashSet(rv));
    }

    @Test
    public void remove() {
        RevisionVector rv = new RevisionVector();
        assertSame(rv, rv.remove(1));

        Revision rev1 = new Revision(1, 0, 1);
        Revision rev2 = new Revision(1, 0, 2);
        Revision rev3 = new Revision(1, 0, 3);
        rv = new RevisionVector(rev1);
        assertSame(rv, rv.remove(2));
        assertEquals(new RevisionVector(), rv.remove(rev1.getClusterId()));
        rv = new RevisionVector(rev1, rev2, rev3);
        assertEquals(new RevisionVector(rev2, rev3), rv.remove(rev1.getClusterId()));
        assertEquals(new RevisionVector(rev1, rev3), rv.remove(rev2.getClusterId()));
        assertEquals(new RevisionVector(rev1, rev2), rv.remove(rev3.getClusterId()));
    }

    @Test
    public void isNewer() {
        Revision rev1 = new Revision(1, 0, 1);
        Revision rev2 = new Revision(1, 0, 2);
        Revision rev3 = new Revision(1, 0, 3);
        RevisionVector rv = new RevisionVector(rev1, rev2);

        assertFalse(rv.isRevisionNewer(rev1));
        assertFalse(rv.isRevisionNewer(rev2));
        assertTrue(rv.isRevisionNewer(rev3));

        assertTrue(rv.isRevisionNewer(new Revision(2, 0, 1)));
        assertTrue(rv.isRevisionNewer(new Revision(2, 0, 2)));
        assertFalse(rv.isRevisionNewer(new Revision(0, 0, 1)));
        assertFalse(rv.isRevisionNewer(new Revision(0, 0, 2)));
    }

    @Test
    public void pmin() {
        RevisionVector rv1 = new RevisionVector();
        RevisionVector rv2 = new RevisionVector();
        assertEquals(newHashSet(), newHashSet(rv1.pmin(rv2)));

        Revision rev11 = new Revision(1, 0, 1);
        Revision rev21 = new Revision(2, 0, 1);
        Revision rev12 = new Revision(1, 0, 2);
        Revision rev22 = new Revision(2, 0, 2);

        rv1 = rv1.update(rev11);
        // rv1: [r1-0-1], rv2: []
        assertEquals(newHashSet(), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(), newHashSet(rv2.pmin(rv1)));

        rv2 = rv2.update(rev12);
        // rv1: [r1-0-1], rv2: [r1-0-2]
        assertEquals(newHashSet(), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(), newHashSet(rv2.pmin(rv1)));

        rv1 = rv1.update(rev12);
        // rv1: [r1-0-1, r1-0-2], rv2: [r1-0-2]
        assertEquals(newHashSet(rev12), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(rev12), newHashSet(rv2.pmin(rv1)));

        rv2 = rv2.update(rev22);
        // rv1: [r1-0-1, r1-0-2], rv2: [r2-0-2]
        assertEquals(newHashSet(rev12), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(rev12), newHashSet(rv2.pmin(rv1)));

        rv2 = rv2.update(rev11);
        // rv1: [r1-0-1, r1-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv2.pmin(rv1)));

        rv1 = rv1.update(rev21);
        // rv1: [r2-0-1, r1-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv2.pmin(rv1)));

        rv1 = rv1.update(rev22);
        // rv1: [r2-0-1, r2-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(newHashSet(rev11, rev22), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(rev11, rev22), newHashSet(rv2.pmin(rv1)));

        rv2 = rv2.update(rev21);
        // rv1: [r2-0-1, r2-0-2], rv2: [r2-0-1, r2-0-2]
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv1.pmin(rv2)));
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv2.pmin(rv1)));
    }

    @Test
    public void pmax() {
        RevisionVector rv1 = new RevisionVector();
        RevisionVector rv2 = new RevisionVector();
        assertEquals(newHashSet(), newHashSet(rv1.pmax(rv2)));

        Revision rev11 = new Revision(1, 0, 1);
        Revision rev21 = new Revision(2, 0, 1);
        Revision rev12 = new Revision(1, 0, 2);
        Revision rev22 = new Revision(2, 0, 2);

        rv1 = rv1.update(rev11);
        // rv1: [r1-0-1], rv2: []
        assertEquals(newHashSet(rev11), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev11), newHashSet(rv2.pmax(rv1)));

        rv2 = rv2.update(rev12);
        // rv1: [r1-0-1], rv2: [r1-0-2]
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv2.pmax(rv1)));

        rv1 = rv1.update(rev12);
        // rv1: [r1-0-1, r1-0-2], rv2: [r1-0-2]
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev11, rev12), newHashSet(rv2.pmax(rv1)));

        rv2 = rv2.update(rev22);
        // rv1: [r1-0-1, r1-0-2], rv2: [r2-0-2]
        assertEquals(newHashSet(rev11, rev22), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev11, rev22), newHashSet(rv2.pmax(rv1)));

        rv2 = rv2.update(rev11);
        // rv1: [r1-0-1, r1-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(newHashSet(rev11, rev22), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev11, rev22), newHashSet(rv2.pmax(rv1)));

        rv1 = rv1.update(rev21);
        // rv1: [r2-0-1, r1-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv2.pmax(rv1)));

        rv1 = rv1.update(rev22);
        // rv1: [r2-0-1, r2-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv2.pmax(rv1)));

        rv2 = rv2.update(rev21);
        // rv1: [r2-0-1, r2-0-2], rv2: [r2-0-1, r2-0-2]
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv1.pmax(rv2)));
        assertEquals(newHashSet(rev21, rev22), newHashSet(rv2.pmax(rv1)));
    }

    @Test
    public void difference() {
        RevisionVector rv1 = new RevisionVector();
        RevisionVector rv2 = new RevisionVector();
        assertEquals(new RevisionVector(), rv1.difference(rv2));

        Revision r11 = new Revision(1, 0, 1);
        rv1 = rv1.update(r11);
        // rv1: [r1-0-1]
        assertEquals(new RevisionVector(r11), rv1.difference(rv2));
        assertEquals(new RevisionVector(), rv2.difference(rv1));

        rv2 = rv2.update(r11);
        // rv1: [r1-0-1], rv2: [r1-0-1]
        assertEquals(new RevisionVector(), rv1.difference(rv2));
        assertEquals(new RevisionVector(), rv2.difference(rv1));

        Revision r12 = new Revision(1, 0, 2);
        rv1 = rv1.update(r12);
        // rv1: [r1-0-1, r1-0-2], rv2: [r1-0-1]
        assertEquals(new RevisionVector(r12), rv1.difference(rv2));
        assertEquals(new RevisionVector(), rv2.difference(rv1));

        Revision r22 = new Revision(2, 0, 2);
        rv2 = rv2.update(r22);
        // rv1: [r1-0-1, r1-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(new RevisionVector(r12), rv1.difference(rv2));
        assertEquals(new RevisionVector(r22), rv2.difference(rv1));

        Revision r21 = new Revision(2, 0, 1);
        rv1 = rv1.update(r21);
        // rv1: [r2-0-1, r1-0-2], rv2: [r1-0-1, r2-0-2]
        assertEquals(new RevisionVector(r21, r12), rv1.difference(rv2));
        assertEquals(new RevisionVector(r11, r22), rv2.difference(rv1));
    }

    @Test
    public void isBranch() {
        RevisionVector rv = new RevisionVector();
        assertFalse(rv.isBranch());
        Revision r1 = new Revision(1, 0, 1);
        rv = rv.update(r1);
        assertFalse(rv.isBranch());
        Revision r2 = new Revision(1, 0, 2, true);
        rv = rv.update(r2);
        assertTrue(rv.isBranch());
    }

    @Test
    public void getBranchRevision() {
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(1, 0, 2, true);
        RevisionVector rv = new RevisionVector(r1, r2);
        assertEquals(r2, rv.getBranchRevision());
    }

    @Test(expected = IllegalStateException.class)
    public void exceptionOnGetBranchRevision() {
        RevisionVector rv = new RevisionVector();
        rv.getBranchRevision();
    }

    @Test
    public void compareTo() {
        RevisionVector rv1 = new RevisionVector();
        RevisionVector rv2 = new RevisionVector();
        assertEquals(0, rv1.compareTo(rv2));

        Revision r11 = new Revision(1, 0, 1);
        rv1 = rv1.update(r11); // [r1-0-1]
        assertTrue(rv1.compareTo(rv2) > 0);
        assertTrue(rv2.compareTo(rv1) < 0);

        Revision r12 = new Revision(1, 0, 2);
        rv2 = rv2.update(r12); // [r1-0-2]
        assertTrue(rv1.compareTo(rv2) > 0);
        assertTrue(rv2.compareTo(rv1) < 0);

        rv2 = rv2.update(r11); // [r1-0-1, r1-0-2]
        assertTrue(rv1.compareTo(rv2) < 0);
        assertTrue(rv2.compareTo(rv1) > 0);

        rv1 = rv1.update(r12); // [r1-0-1, r1-0-2]
        assertEquals(0, rv1.compareTo(rv2));
        assertEquals(0, rv2.compareTo(rv1));

        Revision r22 = new Revision(2, 0, 2);
        rv2 = rv2.update(r22); // [r1-0-1, r2-0-2]
        assertTrue(rv1.compareTo(rv2) < 0);
        assertTrue(rv2.compareTo(rv1) > 0);

        Revision rb22 = r22.asBranchRevision();
        rv1 = rv1.update(rb22);
        assertTrue(rv1.compareTo(rv2) < 0);
        assertTrue(rv2.compareTo(rv1) > 0);
    }

    @Test
    public void equals() {
        RevisionVector rv1 = new RevisionVector();
        RevisionVector rv2 = new RevisionVector();
        assertEquals(rv1, rv2);
        Revision r11 = new Revision(1, 0, 1);
        rv1 = rv1.update(r11);
        assertNotEquals(rv1, rv2);
        rv2 = rv2.update(r11);
        assertEquals(rv1, rv2);
        Revision r12 = new Revision(1, 0, 2);
        rv1 = rv1.update(r12);
        assertNotEquals(rv1, rv2);
        rv2 = rv2.update(r12);
        assertEquals(rv1, rv2);

        //Check basic cases which are short circuited
        assertEquals(rv1, rv1);
        assertNotEquals(rv1, null);
        assertNotEquals(rv1, new Object());
    }

    @Test
    public void hashCodeTest() {
        RevisionVector rv1 = new RevisionVector();
        RevisionVector rv2 = new RevisionVector();
        assertEquals(rv1.hashCode(), rv2.hashCode());

        //Check again once lazily initialized hash is initialized
        assertEquals(rv1.hashCode(), rv2.hashCode());
        Revision r11 = new Revision(1, 0, 1);
        rv1 = rv1.update(r11);
        rv2 = rv2.update(r11);
        assertEquals(rv1.hashCode(), rv2.hashCode());
        Revision r12 = new Revision(1, 0, 2);
        rv1 = rv1.update(r12);
        rv2 = rv2.update(r12);
        assertEquals(rv1.hashCode(), rv2.hashCode());
    }

    @Test
    public void getRevision() {
        RevisionVector rv = new RevisionVector();
        assertNull(rv.getRevision(1));
        Revision r11 = new Revision(1, 0, 1);
        rv = rv.update(r11);
        assertEquals(r11, rv.getRevision(1));
        assertNull(rv.getRevision(2));
        Revision r13 = new Revision(1, 0, 3);
        rv = rv.update(r13);
        assertEquals(r13, rv.getRevision(3));
        assertNull(rv.getRevision(2));
    }

    @Test
    public void asTrunkRevision() {
        RevisionVector rv = new RevisionVector();
        assertFalse(rv.asTrunkRevision().isBranch());
        rv = rv.update(new Revision(1, 0, 1, true));
        assertTrue(rv.isBranch());
        assertFalse(rv.asTrunkRevision().isBranch());
    }

    @Test(expected = IllegalArgumentException.class)
    public void asBranchRevision1() {
        new RevisionVector().asBranchRevision(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void asBranchRevision2() {
        new RevisionVector(new Revision(1, 0, 1)).asBranchRevision(2);
    }

    @Test
    public void asBranchRevision3() {
        Revision r11 = new Revision(1, 0, 1);
        Revision br11 = r11.asBranchRevision();
        RevisionVector rv = new RevisionVector(r11);
        assertEquals(new RevisionVector(br11), rv.asBranchRevision(1));
        rv = rv.asTrunkRevision();
        Revision r12 = new Revision(1, 0, 2);
        rv = rv.update(r12);
        assertEquals(new RevisionVector(br11, r12), rv.asBranchRevision(1));
    }

    @Test
    public void fromString() throws Exception{
        RevisionVector rv = new RevisionVector(
                new Revision(1, 0, 1),
                new Revision(2, 0, 2)
        );

        String rvstr = rv.asString();
        RevisionVector rvFromStr = RevisionVector.fromString(rvstr);
        assertEquals(rv, rvFromStr);
    }

    @Test
    public void toStringBuilder() throws Exception {
        RevisionVector rv = new RevisionVector();
        StringBuilder sb = new StringBuilder();
        rv.toStringBuilder(sb);
        assertEquals("", sb.toString());

        rv = new RevisionVector(
                new Revision(1, 0, 1),
                new Revision(2, 0, 2)
        );
        rv.toStringBuilder(sb);
        assertEquals(rv.toString(), sb.toString());
    }

    @Test
    public void getDimensions() throws Exception {
        RevisionVector rv = new RevisionVector();
        assertEquals(0, rv.getDimensions());
        rv = new RevisionVector(
                new Revision(1, 0, 1),
                new Revision(2, 0, 2)
        );
        assertEquals(2, rv.getDimensions());
        rv = rv.remove(1);
        assertEquals(1, rv.getDimensions());

    }
}
