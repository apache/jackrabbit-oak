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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DefaultSyncedIdentityTest {

    private final ExternalIdentityProvider idp = new TestIdentityProvider();

    private ExternalUser externalUser;
    private ExternalGroup externalGroup;

    private SyncedIdentity si;
    private SyncedIdentity siGroup;

    @Before
    public void before() throws Exception {
        externalUser = idp.getUser(TestIdentityProvider.ID_TEST_USER);
        assertNotNull(externalUser);
        si = new DefaultSyncedIdentity(externalUser.getId(), externalUser.getExternalId(), false, 234);

        externalGroup = idp.listGroups().next();
        siGroup = new DefaultSyncedIdentity(externalGroup.getId(), externalGroup.getExternalId(), true, 234);
    }

    @Test
    public void testGetId() {
        assertEquals(externalUser.getId(), si.getId());
        assertEquals(externalGroup.getId(), siGroup.getId());

        SyncedIdentity siOtherId = new DefaultSyncedIdentity("otherId", externalUser.getExternalId(), false, -1);
        assertEquals("otherId", siOtherId.getId());
    }

    @Test
    public void testGetExternalIdRef() {
        assertEquals(externalUser.getExternalId(), si.getExternalIdRef());
        assertEquals(externalGroup.getExternalId(), siGroup.getExternalIdRef());

        SyncedIdentity siNullExtRef = new DefaultSyncedIdentity(TestIdentityProvider.ID_TEST_USER, null, false, 234);
        assertNull(siNullExtRef.getExternalIdRef());
    }

    @Test
    public void testIsGroup() {
        assertFalse(si.isGroup());
        assertTrue(siGroup.isGroup());
    }

    @Test
    public void testLastSynced() {
        assertEquals(234, si.lastSynced());
        assertEquals(234, siGroup.lastSynced());

        SyncedIdentity siNeverSynced = new DefaultSyncedIdentity(TestIdentityProvider.ID_TEST_USER, externalUser.getExternalId(), false, -1);
        assertEquals(-1, siNeverSynced.lastSynced());
    }
}