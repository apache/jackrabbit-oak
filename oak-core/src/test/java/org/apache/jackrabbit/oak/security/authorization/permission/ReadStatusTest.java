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
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authorization.permission.ReadStatus.ALLOW_ALL;
import static org.apache.jackrabbit.oak.security.authorization.permission.ReadStatus.DENY_ALL;
import static org.apache.jackrabbit.oak.security.authorization.permission.ReadStatus.DENY_THIS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ReadStatusTest extends AbstractSecurityTest  {

    private PrivilegeBitsProvider bitsProvider;

    private PermissionEntry grantAll;
    private PermissionEntry denyAll;

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        bitsProvider = new PrivilegeBitsProvider(root);

        grantAll = createPermissionEntry(true, JCR_ALL);
        denyAll = createPermissionEntry(false, JCR_ALL);
    }

    @NotNull
    private PermissionEntry createPermissionEntry(boolean isAllow, @NotNull String... privNames) {
        return createPermissionEntry(isAllow, RestrictionPattern.EMPTY, privNames);
    }

    @NotNull
    private PermissionEntry createPermissionEntry(boolean isAllow, @NotNull RestrictionPattern pattern, @NotNull String... privNames) {
        return new PermissionEntry("/path", isAllow, 0, bitsProvider.getBits(privNames), pattern);
    }

    private static void assertDenied(@NotNull ReadStatus rs) {
        assertFalse(rs.allowsThis());
        assertFalse(rs.allowsProperties());
        assertFalse(rs.allowsAll());
    }

    private static void assertAllowed(@NotNull ReadStatus rs, boolean canReadProperties) {
        assertTrue(rs.allowsThis());
        assertEquals(canReadProperties, rs.allowsProperties());
        assertFalse(rs.allowsAll());
    }

    @Test
    public void testSkippedAllowed() {
        ReadStatus rs = ReadStatus.create(grantAll, Permissions.ALL, true);

        assertAllowed(rs, false);
        assertSame(rs, ReadStatus.create(grantAll, Permissions.READ, true));
    }

    @Test
    public void testSkippedDenied() {
        ReadStatus rs = ReadStatus.create(denyAll, Permissions.ALL, true);

        assertDenied(rs);
        assertSame(rs, ReadStatus.create(denyAll, Permissions.READ, true));
    }

    @Test
    public void testReadAcTargetPermissionAllow() {
        ReadStatus rs = ReadStatus.create(grantAll, Permissions.READ_ACCESS_CONTROL, false);

        assertAllowed(rs, false);
        assertSame(rs, ReadStatus.create(grantAll, Permissions.READ_ACCESS_CONTROL, true));
    }

    @Test
    public void testReadAcTargetPermissionDeny() {
        ReadStatus rs = ReadStatus.create(denyAll, Permissions.READ_ACCESS_CONTROL, false);

        assertDenied(rs);
        assertSame(rs, ReadStatus.create(denyAll, Permissions.READ_ACCESS_CONTROL, true));
    }

    @Test
    public void testNonEmptyPatternAllow() {
        PermissionEntry entry = createPermissionEntry(true, mock(RestrictionPattern.class), JCR_ALL);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertAllowed(rs, false);
    }

    @Test
    public void testNonEmptyPatternDeny() {
        PermissionEntry entry = createPermissionEntry(false, mock(RestrictionPattern.class), JCR_ALL);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertDenied(rs);
    }

    @Test
    public void testOnlyReadNodesGranted() {
        PermissionEntry entry = createPermissionEntry(true, REP_READ_NODES);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertAllowed(rs, false);
    }

    @Test
    public void testOnlyReadNodesDenied() {
        PermissionEntry entry = createPermissionEntry(false, REP_READ_NODES);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertDenied(rs);
        assertSame(DENY_THIS, rs);
    }

    @Test
    public void testOnlyReadPropertiesGranted() {
        PermissionEntry entry = createPermissionEntry(true, REP_READ_PROPERTIES);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertAllowed(rs, true);
    }

    @Test
    public void testOnlyReadPropertiesDenied() {
        PermissionEntry entry = createPermissionEntry(false, REP_READ_PROPERTIES);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertDenied(rs);
    }

    @Test
    public void testReadGranted() {
        PermissionEntry entry = createPermissionEntry(true, JCR_READ);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertAllowed(rs, true);
        assertSame(ALLOW_ALL, rs);
    }

    @Test
    public void testReadDenied() {
        PermissionEntry entry = createPermissionEntry(false, JCR_READ);
        ReadStatus rs = ReadStatus.create(entry, Permissions.ALL, false);

        assertDenied(rs);
        assertSame(DENY_ALL, rs);
    }
}