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
import java.util.Collections;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.security.authorization.permission.ControlFlag;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class CugPermissionProviderTest extends AbstractCugTest {

    private CugPermissionProvider ppSufficient;
    private CugPermissionProvider ppRequisite;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        Set<Principal> principals = Collections.emptySet(); // TODO
        String[] supportedPaths = new String[0]; // TODO

        ppSufficient = new CugPermissionProvider(root, principals, supportedPaths, ControlFlag.SUFFICIENT, CugContext.INSTANCE);
        ppRequisite = new CugPermissionProvider(root, principals, supportedPaths, ControlFlag.REQUISITE, CugContext.INSTANCE);
    }

    @Test
    public void testGetFlag() {
        assertSame(ControlFlag.SUFFICIENT, ppSufficient.getFlag());
        assertSame(ControlFlag.REQUISITE, ppRequisite.getFlag());
    }

    @Test
    public void testHandlesPath() {
        // TODO
    }

    @Test
    public void testHandlesTree() {
        // TODO
    }

    @Test
    public void testHandlesRepositoryPermissions() {
        assertFalse(ppSufficient.handlesRepositoryPermissions());
        assertFalse(ppRequisite.handlesRepositoryPermissions());
    }

    @Test
    public void getPrivileges() {
        // TODO
    }

    @Test
    public void hasPrivileges() {
        // TODO
    }
}