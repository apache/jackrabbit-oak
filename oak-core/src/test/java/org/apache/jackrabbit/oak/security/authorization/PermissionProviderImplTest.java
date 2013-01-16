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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Collections;

import org.apache.jackrabbit.oak.spi.security.authorization.AllPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.CompiledPermissions;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * PermissionProviderImplTest... TODO
 */
public class PermissionProviderImplTest {

    private PermissionProvider pp = new PermissionProviderImpl();
    private NodeStore nodeStore = null; // TODO

    @Test
    public void testGetPermissions() {
        // TODO
    }

    @Test
    public void testGetCompilePermissions() {
        // TODO
    }

    @Test
    public void testGetSystemPermissions() {
        CompiledPermissions cp = pp.getCompiledPermissions(nodeStore,
                Collections.<Principal>singleton(SystemPrincipal.INSTANCE));
        assertTrue(cp instanceof AllPermissions);
    }

    @Test
    public void testGetAdminPermissions() {
        CompiledPermissions cp = pp.getCompiledPermissions(nodeStore,
                Collections.<Principal>singleton(new AdminPrincipal() {
                    @Override
                    public String getName() {
                        return "someAdminName";
                    }
                }));
        assertTrue(cp instanceof AllPermissions);
    }
}