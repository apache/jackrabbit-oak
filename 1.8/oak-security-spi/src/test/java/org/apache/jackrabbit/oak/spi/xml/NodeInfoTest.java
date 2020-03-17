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
package org.apache.jackrabbit.oak.spi.xml;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class NodeInfoTest {

    private NodeInfo nodeInfo = new NodeInfo("name", "primaryType", ImmutableList.of("mixin1", "mixin2"), "uuid");

    @Test
    public void testGetName() {
        assertEquals("name", nodeInfo.getName());
    }

    @Test
    public void testGetPrimaryTypeName() {
        assertEquals("primaryType", nodeInfo.getPrimaryTypeName());
    }

    @Test
    public void testGetMixinTypeName() {
        assertEquals(ImmutableList.of("mixin1", "mixin2"), nodeInfo.getMixinTypeNames());
    }

    @Test
    public void testGetMixinTypeNameEmpty() {
        NodeInfo ni = new NodeInfo("name", "primaryType", null, "uuid");
        assertFalse(ni.getMixinTypeNames().iterator().hasNext());
    }

    @Test
    public void testGetUUID() {
        assertEquals("uuid", nodeInfo.getUUID());
    }
}