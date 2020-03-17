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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AllTreePermissionTest {

    private final TreePermission all = TreePermission.ALL;
    private final PropertyState property = PropertyStates.createProperty("prop", "value");

    @Test
    public void testGetChildPermission() {
        assertSame(TreePermission.ALL, all.getChildPermission("name", EmptyNodeState.EMPTY_NODE));
    }

    @Test
    public void testCanRead() {
        assertTrue(all.canRead());
    }

    @Test
    public void testCanReadProperty() {
        assertTrue(all.canRead(property));
    }

    @Test
    public void testCanReadAll() {
        assertTrue(all.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        assertTrue(all.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        assertTrue(all.isGranted(Permissions.ALL));
    }

    @Test
    public void testIsGrantedProperty() {
        assertTrue(all.isGranted(Permissions.ALL, property));
    }
}