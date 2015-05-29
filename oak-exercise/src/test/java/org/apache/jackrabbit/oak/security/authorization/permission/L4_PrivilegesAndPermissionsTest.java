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

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.junit.Test;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Privileges and Permissions
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * TODO
 *
 * Exercises:
 *
 * - TODO
 *
 * - {@link #testAddAndRemoveChildNodes()}
 *   TODO
 *
 * - {@link #testPrivilegeBitsToPermissions()}
 *   Understand the internal mechanism to map {@link javax.jcr.security.Privilege}
 *   to {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions}
 *   in the {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits}
 *   object.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * TODO
 *
 * </pre>
 *
 * @see TODO
 */
public class L4_PrivilegesAndPermissionsTest extends AbstractSecurityTest {

    @Test
    public void testAddAndRemoveChildNodes() throws Exception {
        // TODO
    }

    @Test
    public void testPrivilegeBitsToPermissions() throws Exception {
        // TODO : angela make a sensible test

        PrivilegeBits pb = null; // TODO
        PrivilegeBits parentPb = null; // TODO
        boolean isAllow = false; // TODO

        long permissions = PrivilegeBits.calculatePermissions(pb, parentPb, isAllow);
        String permStr = Permissions.getString(permissions);
    }
}