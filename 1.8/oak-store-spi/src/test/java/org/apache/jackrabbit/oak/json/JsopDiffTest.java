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
package org.apache.jackrabbit.oak.json;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class JsopDiffTest {

    @Test
    public void testPropertyChanges() {
        JsopDiff diff;
        PropertyState before = StringPropertyState.stringProperty("foo", "bar");

        diff = new JsopDiff();
        diff.propertyAdded(before);
        assertEquals("^\"/foo\":\"bar\"", diff.toString());

        diff = new JsopDiff();
        diff.propertyChanged(before, LongPropertyState.createLongProperty("foo", 123L));
        assertEquals("^\"/foo\":123", diff.toString());

        diff = new JsopDiff();
        diff.propertyChanged(before, DoublePropertyState.doubleProperty("foo", 1.23));
        assertEquals("^\"/foo\":1.23", diff.toString());

        diff = new JsopDiff();
        diff.propertyChanged(before, BooleanPropertyState.booleanProperty("foo", true));
        assertEquals("^\"/foo\":true", diff.toString());

        diff = new JsopDiff();
        diff.propertyDeleted(before);
        assertEquals("^\"/foo\":null", diff.toString());
    }

    @Test
    public void testNodeChanges() {
        JsopDiff diff;
        NodeState before = EMPTY_NODE;
        NodeBuilder builder = before.builder();
        builder.setProperty("a", 1L);
        builder.child("x");
        NodeState after = builder.getNodeState();

        diff = new JsopDiff();
        diff.childNodeAdded("test", before);
        assertEquals("+\"/test\":{}", diff.toString());

        diff = new JsopDiff();
        diff.childNodeChanged("test", before, after);
        assertEquals("^\"/test/a\":1+\"/test/x\":{}", diff.toString());

        diff = new JsopDiff();
        diff.childNodeDeleted("test", after);
        assertEquals("-\"/test\"", diff.toString());
    }

}
