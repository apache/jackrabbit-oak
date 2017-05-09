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

import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for UpdateOp
 */
public class UpdateOpTest {

    @Test
    public void keyEquals() {
        Revision r1 = Revision.newRevision(1);
        Revision r2 = Revision.newRevision(1);

        UpdateOp.Key k1 = new UpdateOp.Key("foo", null);
        UpdateOp.Key k2 = new UpdateOp.Key("bar", null);
        assertFalse(k1.equals(k2));
        assertFalse(k2.equals(k1));

        UpdateOp.Key k3 = new UpdateOp.Key("foo", null);
        assertTrue(k1.equals(k3));
        assertTrue(k3.equals(k1));

        UpdateOp.Key k4 = new UpdateOp.Key("foo", r1);
        assertFalse(k4.equals(k3));
        assertFalse(k3.equals(k4));

        UpdateOp.Key k5 = new UpdateOp.Key("foo", r2);
        assertFalse(k5.equals(k4));
        assertFalse(k4.equals(k5));

        UpdateOp.Key k6 = new UpdateOp.Key("foo", r1);
        assertTrue(k6.equals(k4));
        assertTrue(k4.equals(k6));

        UpdateOp.Key k7 = new UpdateOp.Key("bar", r1);
        assertFalse(k7.equals(k6));
        assertFalse(k6.equals(k7));
    }

    @Test
    public void combine() {
        UpdateOp op1 = new UpdateOp("id", false);
        op1.set("p", "value");
        UpdateOp op2 = new UpdateOp("id", false);
        Revision r = Revision.newRevision(1);
        op2.containsMapEntry("e", r, true);

        UpdateOp combined = UpdateOp.combine("id", newArrayList(op1, op2));
        assertTrue(combined.hasChanges());
        assertEquals(1, combined.getChanges().size());
        assertEquals(1, combined.getConditions().size());
    }

    @Test
    public void containsMapEntry() {
        Revision r = Revision.newRevision(1);
        UpdateOp op = new UpdateOp("id", true);
        try {
            op.containsMapEntry("p", r, true);
            fail("expected " + IllegalStateException.class.getName());
        } catch (IllegalStateException e) {
            // expected
        }
        op = new UpdateOp("id", false);
        op.containsMapEntry("p", r, true);
        assertEquals(1, op.getConditions().size());
        UpdateOp.Key key = op.getConditions().keySet().iterator().next();
        assertEquals(r, key.getRevision());
        assertEquals("p", key.getName());
        UpdateOp.Condition c = op.getConditions().get(key);
        assertEquals(UpdateOp.Condition.EXISTS, c);

        op = new UpdateOp("id", false);
        op.containsMapEntry("p", r, false);
        assertEquals(1, op.getConditions().size());
        key = op.getConditions().keySet().iterator().next();
        assertEquals(r, key.getRevision());
        assertEquals("p", key.getName());
        c = op.getConditions().get(key);
        assertEquals(UpdateOp.Condition.MISSING, c);
    }

    @Test
    public void copy() {
        UpdateOp op1 = new UpdateOp("id", false);
        op1.set("p", "value");

        UpdateOp op2 = op1.copy();
        assertTrue(op2.hasChanges());
        assertEquals(1, op2.getChanges().size());
        assertEquals(0, op2.getConditions().size());

        Revision r = Revision.newRevision(1);
        op1.containsMapEntry("e", r, true);

        op2 = op1.copy();
        assertEquals(1, op2.getConditions().size());
    }

    @Test
    public void equalsTest() {
        Revision r = Revision.newRevision(1);
        UpdateOp op = new UpdateOp("id", true);
        try {
            op.equals("p", r, "v");
            fail("expected " + IllegalStateException.class.getName());
        } catch (IllegalStateException e) {
            // expected
        }
        op = new UpdateOp("id", false);
        op.equals("p", r, "v");
        assertEquals(1, op.getConditions().size());
        UpdateOp.Key key = op.getConditions().keySet().iterator().next();
        assertEquals(r, key.getRevision());
        assertEquals("p", key.getName());
        UpdateOp.Condition c = op.getConditions().get(key);
        assertEquals(UpdateOp.Condition.Type.EQUALS, c.type);
        assertEquals("v", c.value);
    }

    @Test
    public void notEqualsTest() {
        Revision r = Revision.newRevision(1);
        UpdateOp op = new UpdateOp("id", true);
        try {
            op.notEquals("p", r, "v");
            fail("expected " + IllegalStateException.class.getName());
        } catch (IllegalStateException e) {
            // expected
        }
        op = new UpdateOp("id", false);
        op.notEquals("p", r, "v");
        assertEquals(1, op.getConditions().size());
        UpdateOp.Key key = op.getConditions().keySet().iterator().next();
        assertEquals(r, key.getRevision());
        assertEquals("p", key.getName());
        UpdateOp.Condition c = op.getConditions().get(key);
        assertEquals(UpdateOp.Condition.Type.NOTEQUALS, c.type);
        assertEquals("v", c.value);

        op = new UpdateOp("id", false);
        op.notEquals("p", r, null);
        assertEquals(1, op.getConditions().size());
        key = op.getConditions().keySet().iterator().next();
        assertEquals(r, key.getRevision());
        assertEquals("p", key.getName());
        c = op.getConditions().get(key);
        assertEquals(UpdateOp.Condition.Type.NOTEQUALS, c.type);
        assertEquals(null, c.value);
    }

    @Test
    public void getChanges() {
        UpdateOp op = new UpdateOp("id", false);
        assertEquals(0, op.getChanges().size());
        assertTrue(!op.hasChanges());
        op.set("p", "value");
        assertEquals(1, op.getChanges().size());
        assertTrue(op.hasChanges());
    }

    @Test
    public void shallowCopy() {
        UpdateOp op1 = new UpdateOp("id", false);
        op1.set("p", "value");

        UpdateOp op2 = op1.shallowCopy("id");
        assertTrue(op2.hasChanges());
        assertEquals(1, op2.getChanges().size());
        assertEquals(0, op2.getConditions().size());

        Revision r = Revision.newRevision(1);
        op1.containsMapEntry("e", r, true);

        op2 = op1.shallowCopy("id");
        assertEquals(1, op2.getConditions().size());
    }
}
