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

public class NoRecurseTreePermissionTest {

    private final TreePermission noRecurse = TreePermission.NO_RECOURSE;
    private final PropertyState property = PropertyStates.createProperty("prop", "value");

    @Test(expected = UnsupportedOperationException.class)
    public void testGetChildPermission() {
        noRecurse.getChildPermission("name", EmptyNodeState.EMPTY_NODE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCanRead() {
        noRecurse.canRead();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCanReadProperty() {
        noRecurse.canRead(property);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCanReadAll() {
        noRecurse.canReadAll();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCanReadProperties() {
        noRecurse.canReadProperties();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsGranted() {
        noRecurse.isGranted(Permissions.ALL);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsGrantedProperty() {
        noRecurse.isGranted(Permissions.ALL, property);
    }
}