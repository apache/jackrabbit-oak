/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class JsopDiffTest {

    @Test
    public void testPropertyChanges() {
        JsopDiff diff;
        PropertyState before = PropertyStates.stringProperty("foo", "bar");

        diff = new JsopDiff(null);
        diff.propertyAdded(before);
        assertEquals("^\"/foo\":\"bar\"", diff.toString());

        diff = new JsopDiff(null);
        diff.propertyChanged(before, PropertyStates.longProperty("foo", 123L));
        assertEquals("^\"/foo\":123", diff.toString());

        diff = new JsopDiff(null);
        diff.propertyChanged(before, PropertyStates.doubleProperty("foo", 1.23));
        assertEquals("^\"/foo\":\"dou:1.23\"", diff.toString()); // TODO: 1.23?

        diff = new JsopDiff(null);
        diff.propertyChanged(before, PropertyStates.booleanProperty("foo", true));
        assertEquals("^\"/foo\":true", diff.toString());

        diff = new JsopDiff(null);
        diff.propertyDeleted(before);
        assertEquals("^\"/foo\":null", diff.toString());
    }

    @Test
    public void testNodeChanges() {
        JsopDiff diff;
        NodeState before = MemoryNodeState.EMPTY_NODE;
        NodeState after = new MemoryNodeState(
                ImmutableMap.<String, PropertyState>of(
                        "a", PropertyStates.longProperty("a", 1L)),
                ImmutableMap.of(
                        "x", MemoryNodeState.EMPTY_NODE));


        diff = new JsopDiff(null);
        diff.childNodeAdded("test", before);
        assertEquals("+\"/test\":{}", diff.toString());

        diff = new JsopDiff(null);
        diff.childNodeChanged("test", before, after);
        assertEquals("^\"/test/a\":1+\"/test/x\":{}", diff.toString());

        diff = new JsopDiff(null);
        diff.childNodeDeleted("test", after);
        assertEquals("-\"/test\"", diff.toString());
    }

}
