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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class InternalTreePermissonTest {

    @Test
    public void testGetChildPermission() {
        assertSame(InternalTreePermission.INSTANCE, InternalTreePermission.INSTANCE.getChildPermission("name", mock(NodeState.class)));
    }

    @Test
    public void testCanRead() {
        assertEquals(TreePermission.EMPTY.canRead(), InternalTreePermission.INSTANCE.canRead());
    }

    @Test
    public void testCanReadProperty() {
        PropertyState prop = mock(PropertyState.class);
        assertEquals(TreePermission.EMPTY.canRead(prop), InternalTreePermission.INSTANCE.canRead(prop));
    }

    @Test
    public void testCanReadAll() {
        assertEquals(TreePermission.EMPTY.canReadAll(), InternalTreePermission.INSTANCE.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        assertEquals(TreePermission.EMPTY.canReadProperties(), InternalTreePermission.INSTANCE.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        assertEquals(TreePermission.EMPTY.isGranted(Permissions.READ_NODE), InternalTreePermission.INSTANCE.isGranted(Permissions.READ_NODE));
    }

    @Test
    public void testIsGrantedProperty() {
        PropertyState prop = mock(PropertyState.class);
        assertEquals(TreePermission.EMPTY.isGranted(Permissions.READ_PROPERTY, prop), InternalTreePermission.INSTANCE.isGranted(Permissions.READ_PROPERTY, prop));
    }
}