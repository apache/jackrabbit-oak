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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class EmptyTreePermissionTest {

    private final TreePermission empty = TreePermission.EMPTY;
    private final PropertyState property = PropertyStates.createProperty("prop", "value");

    @Test
    public void testGetChildPermission() {
        assertSame(TreePermission.EMPTY, empty.getChildPermission("name", EmptyNodeState.EMPTY_NODE));
    }

    @Test
    public void testCanRead() {
        assertFalse(empty.canRead());
    }

    @Test
    public void testCanReadProperty() {
        assertFalse(empty.canRead(property));
    }

    @Test
    public void testCanReadAll() {
        assertFalse(empty.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        assertFalse(empty.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        assertFalse(empty.isGranted(Permissions.ALL));
    }

    @Test
    public void testIsGrantedProperty() {
        assertFalse(empty.isGranted(Permissions.ALL, property));
    }

}