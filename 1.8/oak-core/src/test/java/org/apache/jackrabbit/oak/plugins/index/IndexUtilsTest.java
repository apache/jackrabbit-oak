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

package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;

public class IndexUtilsTest {

    @Test
    public void asyncName() throws Exception {
        assertNull(IndexUtils.getAsyncLaneName(EMPTY_NODE, "/fooIndex"));

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("async", newArrayList("async2", "sync"), Type.STRINGS);
        assertEquals("async2", IndexUtils.getAsyncLaneName(builder.getNodeState(), "/fooIndex"));

        builder.setProperty("async", newArrayList("async3"), Type.STRINGS);
        assertEquals("async3", IndexUtils.getAsyncLaneName(builder.getNodeState(), "/fooIndex"));
    }

}