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

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CugTreePermissionTest extends AbstractCugTest {

    private CugTreePermission allowedTp;
    private CugTreePermission deniedTp;

    @Override
    public void before() throws Exception {
        super.before();

        createCug(SUPPORTED_PATH, EveryonePrincipal.getInstance());
        root.commit();

        Set<Principal> principals = ImmutableSet.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        Set<String> supportedPaths = ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2);

        allowedTp = getCugTreePermission(root, getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        deniedTp = getCugTreePermission(root);
    }

    private static CugTreePermission getCugTreePermission(@Nonnull Root root, @Nonnull Principal... principals) {
        PermissionProvider pp = new CugPermissionProvider(root, ImmutableSet.copyOf(principals), ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2), CugContext.INSTANCE);
        TreePermission rootTp = pp.getTreePermission(root.getTree("/"), TreePermission.EMPTY);
        TreePermission targetTp = pp.getTreePermission(root.getTree(SUPPORTED_PATH), rootTp);
        assertTrue(targetTp instanceof CugTreePermission);
        return (CugTreePermission) targetTp;
    }

    @Test
    public void testGetChildPermission() throws Exception {
        TreePermission child = allowedTp.getChildPermission("subtree", EmptyNodeState.EMPTY_NODE);
        assertTrue(child instanceof CugTreePermission);

        TreePermission cugChild = allowedTp.getChildPermission(REP_CUG_POLICY, EmptyNodeState.EMPTY_NODE);
        assertSame(TreePermission.NO_RECOURSE, cugChild);

        child = allowedTp.getChildPermission("subtree", EmptyNodeState.EMPTY_NODE);
        assertTrue(child instanceof CugTreePermission);
    }

    @Test
    public void testCanRead() {
        assertTrue(allowedTp.canRead());
        assertFalse(deniedTp.canRead());
    }

    @Test
    public void testCanReadProperty() {
        PropertyState ps = PropertyStates.createProperty("prop", "val");

        assertTrue(allowedTp.canRead(ps));
        assertFalse(deniedTp.canRead(ps));
    }

    @Test
    public void testCanReadAll() {
        assertFalse(allowedTp.canReadAll());
        assertFalse(deniedTp.canReadAll());
    }

    @Test
    public void testCanReadProperties() {
        assertFalse(allowedTp.canReadProperties());
        assertFalse(deniedTp.canReadProperties());
    }

    @Test
    public void testIsGranted() {
        assertFalse(allowedTp.isGranted(Permissions.ALL));
        assertFalse(allowedTp.isGranted(Permissions.WRITE));
        assertTrue(allowedTp.isGranted(Permissions.READ_NODE));

        assertFalse(deniedTp.isGranted(Permissions.ALL));
        assertFalse(deniedTp.isGranted(Permissions.WRITE));
        assertFalse(deniedTp.isGranted(Permissions.READ_NODE));

    }

    @Test
    public void testIsGrantedProperty() {
        PropertyState ps = PropertyStates.createProperty("prop", "val");

        assertFalse(allowedTp.isGranted(Permissions.ALL, ps));
        assertFalse(allowedTp.isGranted(Permissions.WRITE, ps));
        assertTrue(allowedTp.isGranted(Permissions.READ_PROPERTY, ps));

        assertFalse(deniedTp.isGranted(Permissions.ALL, ps));
        assertFalse(deniedTp.isGranted(Permissions.WRITE, ps));
        assertFalse(deniedTp.isGranted(Permissions.READ_PROPERTY, ps));
    }
}