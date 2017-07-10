/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.util.Arrays;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class AsyncLaneSwitcherTest {

    private NodeBuilder builder = EMPTY_NODE.builder();

    @Test
    public void switchNone() throws Exception{
        AsyncLaneSwitcher.switchLane(builder, "foo");

        PropertyState previous = builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS);
        assertNotNull(previous);
        assertEquals(AsyncLaneSwitcher.ASYNC_PREVIOUS_NONE, previous.getValue(Type.STRING));
    }

    @Test
    public void switchSingleAsync() throws Exception{
        builder.setProperty(ASYNC_PROPERTY_NAME, "async");

        AsyncLaneSwitcher.switchLane(builder, "foo");

        PropertyState previous = builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS);
        assertNotNull(previous);
        assertEquals("async", previous.getValue(Type.STRING));
    }

    @Test
    public void switchAsyncArray() throws Exception{
        builder.setProperty(ASYNC_PROPERTY_NAME, asList("async", "nrt"), Type.STRINGS);

        AsyncLaneSwitcher.switchLane(builder, "foo");

        PropertyState previous = builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS);
        assertNotNull(previous);
        assertEquals(asList("async", "nrt"), previous.getValue(Type.STRINGS));
    }

    @Test
    public void multipleSwitch() throws Exception{
        builder.setProperty(ASYNC_PROPERTY_NAME, "async");

        AsyncLaneSwitcher.switchLane(builder, "foo");
        AsyncLaneSwitcher.switchLane(builder, "foo");

        PropertyState previous = builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS);
        assertNotNull(previous);
        assertEquals("async", previous.getValue(Type.STRING));
    }

    @Test
    public void revert() throws Exception{
        builder.setProperty(ASYNC_PROPERTY_NAME, "async");

        AsyncLaneSwitcher.switchLane(builder, "foo");
        assertNotNull(builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS));

        AsyncLaneSwitcher.revertSwitch(builder, "/fooIndex");
        assertNull(builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS));
        assertEquals("async", builder.getString(ASYNC_PROPERTY_NAME));
    }

    @Test
    public void revert_Sync() throws Exception{
        AsyncLaneSwitcher.switchLane(builder, "foo");

        AsyncLaneSwitcher.revertSwitch(builder, "/fooIndex");
        assertNull(builder.getProperty(AsyncLaneSwitcher.ASYNC_PREVIOUS));
        assertNull(builder.getProperty(ASYNC_PROPERTY_NAME));
    }

    @Test
    public void switchAndRevertMulti() throws Exception{
        builder.setProperty(ASYNC_PROPERTY_NAME, asList("async", "nrt"), Type.STRINGS);

        AsyncLaneSwitcher.switchLane(builder, "foo");
        AsyncLaneSwitcher.revertSwitch(builder, "foo");
    }

}