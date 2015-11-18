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
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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

        allowedTp = getCugTreePermission(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        deniedTp = getCugTreePermission();
    }

    private CugTreePermission getCugTreePermission(@Nonnull Principal... principals) {
        return getCugTreePermission(SUPPORTED_PATH, principals);
    }

    private CugTreePermission getCugTreePermission(@Nonnull String path, @Nonnull Principal... principals) {
        CugPermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH, SUPPORTED_PATH2), principals);
        TreePermission targetTp = getTreePermission(root, path, pp);
        assertTrue(targetTp instanceof CugTreePermission);
        return (CugTreePermission) targetTp;
    }

    @Test
    public void testGetChildPermission() throws Exception {
        NodeState ns = ((AbstractTree) root.getTree(SUPPORTED_PATH + "/subtree")).getNodeState();
        TreePermission child = allowedTp.getChildPermission("subtree", ns);
        assertTrue(child instanceof CugTreePermission);

        child = deniedTp.getChildPermission("subtree", ns);
        assertTrue(child instanceof CugTreePermission);

        NodeState cugNs = ((AbstractTree) root.getTree(SUPPORTED_PATH + "/" + REP_CUG_POLICY)).getNodeState();
        TreePermission cugChild = allowedTp.getChildPermission(REP_CUG_POLICY, cugNs);
        assertSame(TreePermission.NO_RECOURSE, cugChild);
    }

    @Test
    public void testIsAllow() throws Exception {
        assertTrue(allowedTp.isAllow());
        assertFalse(deniedTp.isAllow());

        CugTreePermission tp = getCugTreePermission(SUPPORTED_PATH2);
        assertFalse(tp.isAllow());
        tp = getCugTreePermission(SUPPORTED_PATH2, getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        assertFalse(tp.isAllow());
    }

    @Test
    public void testIsInCug() {
        assertTrue(allowedTp.isInCug());
        assertTrue(deniedTp.isInCug());

        CugTreePermission tp = getCugTreePermission(SUPPORTED_PATH2);
        assertFalse(tp.isInCug());
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
        assertTrue(allowedTp.canReadProperties());
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