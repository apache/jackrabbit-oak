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

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * <code>UpdateUtilsTest</code>...
 */
public class UpdateUtilsTest {

    @Test
    public void applyChanges() {
        Revision r = Revision.newRevision(1);
        String id = Utils.getIdFromPath("/foo");
        Document d = new Document();
        d.put(Document.ID, id);

        UpdateOp op = newUpdateOp(id);
        op.set("p", 42L);

        UpdateUtils.applyChanges(d, op);
        assertEquals(42L, d.get("p"));

        op = newUpdateOp(id);
        op.max("p", 23L);

        UpdateUtils.applyChanges(d, op);
        assertEquals(42L, d.get("p"));

        op = newUpdateOp(id);
        op.max("p", 58L);

        UpdateUtils.applyChanges(d, op);
        assertEquals(58L, d.get("p"));

        op = newUpdateOp(id);
        op.increment("p", 3);

        UpdateUtils.applyChanges(d, op);
        assertEquals(61L, d.get("p"));

        op = newUpdateOp(id);
        op.setMapEntry("t", r, "value");

        UpdateUtils.applyChanges(d, op);
        assertEquals("value", getMapEntry(d, "t", r));

        op = newUpdateOp(id);
        op.removeMapEntry("t", r);

        UpdateUtils.applyChanges(d, op);
        assertNull(getMapEntry(d, "t", r));
    }

    @Test
    public void checkConditions() {
        Revision r = Revision.newRevision(1);
        String id = Utils.getIdFromPath("/foo");
        Document d = new Document();
        d.put(Document.ID, id);

        UpdateOp op = newUpdateOp(id);
        op.set("p", 42L);
        op.setMapEntry("t", r, "value");
        UpdateUtils.applyChanges(d, op);

        op = newUpdateOp(id);
        op.containsMapEntry("t", r, true);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.containsMapEntry("t", r, false);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.containsMapEntry("q", r, true);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.containsMapEntry("q", r, false);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("t", r, "value");
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("t", r, "value");
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("t", r, "foo");
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("t", r, "foo");
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("t", Revision.newRevision(1), "value");
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("t", Revision.newRevision(1), "value");
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("t", "value");
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("t", "value");
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("p", r, 42L);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("p", r, 42L);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("p", 42L);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("p", 42L);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("p", 7L);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("p", 7L);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        // check on non-existing property
        op = newUpdateOp(id);
        op.notEquals("other", 7L);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("other", r, 7L);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("other", r, null);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.equals("other", r, null);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        // check null
        op = newUpdateOp(id);
        op.notEquals("p", null);
        assertTrue(UpdateUtils.checkConditions(d, op.getConditions()));

        op = newUpdateOp(id);
        op.notEquals("other", r, null);
        assertFalse(UpdateUtils.checkConditions(d, op.getConditions()));
    }

    private static UpdateOp newUpdateOp(String id) {
        return new UpdateOp(id, false);
    }

    private static Object getMapEntry(Document d, String name, Revision r) {
        Object obj = d.get(name);
        if (obj instanceof Map) {
            return ((Map) obj).get(r);
        }
        return null;
    }
}
