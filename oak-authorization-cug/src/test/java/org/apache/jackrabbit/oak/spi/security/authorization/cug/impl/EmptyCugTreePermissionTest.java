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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class EmptyCugTreePermissionTest extends AbstractCugTest {

    private EmptyCugTreePermission tp;

    @Override
    public void before() throws Exception {
        super.before();

        createCug(SUPPORTED_PATH, EveryonePrincipal.getInstance());
        root.commit();

        PermissionProvider pp = createCugPermissionProvider(
                ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2),
                getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        tp = new EmptyCugTreePermission(root.getTree("/"), pp);
    }

    @Test
    public void testGetChildPermission() throws Exception {
        TreePermission child = tp.getChildPermission(Text.getName(SUPPORTED_PATH2), EmptyNodeState.EMPTY_NODE);
        assertTrue(child instanceof EmptyCugTreePermission);

        child = tp.getChildPermission(Text.getName(SUPPORTED_PATH), EmptyNodeState.EMPTY_NODE);
        assertFalse(child instanceof EmptyCugTreePermission);
        assertTrue(child instanceof CugTreePermission);

        TreePermission child2 = tp.getChildPermission(Text.getName(UNSUPPORTED_PATH), EmptyNodeState.EMPTY_NODE);
        assertFalse(child2 instanceof EmptyCugTreePermission);
        assertSame(child2, TreePermission.NO_RECOURSE);
    }

    @Test
    public void testCanRead() {
        assertFalse(tp.canRead());
    }

    @Test
    public void testCanReadProperty() {
        assertFalse(tp.canRead(PropertyStates.createProperty("prop", "val")));
    }

    @Test
    public void testCanReadAll() {
        assertFalse(tp.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        assertFalse(tp.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        assertFalse(tp.isGranted(Permissions.ALL));
        assertFalse(tp.isGranted(Permissions.WRITE));
        assertFalse(tp.isGranted(Permissions.READ));
    }

    @Test
    public void testIsGrantedProperty() {
        PropertyState ps = PropertyStates.createProperty("prop", "val");
        assertFalse(tp.isGranted(Permissions.ALL, ps));
        assertFalse(tp.isGranted(Permissions.WRITE, ps));
        assertFalse(tp.isGranted(Permissions.READ, ps));
    }

}