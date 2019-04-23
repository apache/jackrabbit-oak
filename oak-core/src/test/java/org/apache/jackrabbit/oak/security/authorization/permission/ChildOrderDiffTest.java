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
package org.apache.jackrabbit.oak.security.authorization.permission;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ChildOrderDiffTest {

    @NotNull
    private static PropertyState createPropertyState(@NotNull String... names) {
        return PropertyStates.createProperty("any", ImmutableList.copyOf(names), Type.NAMES);
    }

    @Test
    public void testBeforeEmptyAfterEmpty() {
        PropertyState before = createPropertyState();
        PropertyState after = createPropertyState();
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testBeforeEmpty() {
        PropertyState before = createPropertyState();
        PropertyState after = createPropertyState("n1", "n2");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testAfterEmpty() {
        PropertyState before = createPropertyState("n1", "n2");
        PropertyState after = createPropertyState();
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
        public void testAfterEqualsBefore() {
        PropertyState eq = createPropertyState("n1", "n2");
        assertNull(ChildOrderDiff.firstReordered(eq, eq));
    }

    @Test
    public void testAppendedAtEnd() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n1", "n2", "n3", "n4");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testInsertedAtBeginning() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n0", "n1", "n2", "n3");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testInserted() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n1", "n11", "n2", "n3");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testLastReplaced() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n1", "n2", "n4");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testFirstRemoved() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n2", "n3");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testSecondRemoved() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n1", "n3");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testLastRemoved() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n1", "n2");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }


    @Test
    public void testReorderedFirstToEnd() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n2", "n3", "n1");
        assertEquals("n2", ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testReorderedLastBeforeSecond() {
        PropertyState before = createPropertyState("n1", "n2", "n3");
        PropertyState after = createPropertyState("n1", "n3", "n2");
        assertEquals("n3", ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testRemovedAndReordered() {
        PropertyState before = createPropertyState("n1", "n2", "n3", "n4");
        PropertyState after = createPropertyState("n1", "n4", "n3");
        assertEquals("n4", ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testInsertedRemovedAndReordered() {
        PropertyState before = createPropertyState("n1", "n2", "n3", "n4");
        PropertyState after = createPropertyState("n1", "n11", "n4", "n3");
        assertEquals("n4", ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testRemovedAndReorderedAppended() {
        PropertyState before = createPropertyState("n1", "n2", "n3", "n4");
        PropertyState after = createPropertyState("n1", "n4", "n3", "n33");
        assertEquals("n4", ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testReorderedAndReplaced() {
        PropertyState before = createPropertyState("n1", "n2", "n3", "n4");
        PropertyState after = createPropertyState("n4", "n1", "n6");
        assertEquals("n4", ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testOnlyLastEquals() {
        PropertyState before = createPropertyState("n1", "n2");
        PropertyState after = createPropertyState("n5", "n6", "n7", "n2");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }

    @Test
    public void testAllDifferent() {
        PropertyState before = createPropertyState("n1", "n2", "n3", "n4");
        PropertyState after = createPropertyState("n5", "n6", "n7", "n8", "n9");
        assertNull(ChildOrderDiff.firstReordered(before, after));
    }
}