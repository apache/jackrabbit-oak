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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.jmx;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncedIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncedIdentity;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.DEFAULT_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_TEST_USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ErrorSyncResultTest {
    
    private static final Exception ERROR = new ExternalIdentityException();
    private static final ExternalIdentityRef EXTERNAL_IDENTITY_REF = new ExternalIdentityRef(ID_TEST_USER, DEFAULT_IDP_NAME);

    private final ErrorSyncResult result = new ErrorSyncResult(EXTERNAL_IDENTITY_REF, ERROR);
    private final ErrorSyncResult result2 = new ErrorSyncResult(ID_TEST_USER, null, ERROR);

    @Test
    public void testGetIdentity() {
        SyncedIdentity si = result.getIdentity();
        assertTrue(si instanceof DefaultSyncedIdentity);
        assertSame(EXTERNAL_IDENTITY_REF, si.getExternalIdRef());
        assertEquals(ID_TEST_USER, si.getId());
        assertFalse(si.isGroup());
        assertEquals(-1, si.lastSynced());
    }

    @Test
    public void testGetIdentity2() {
        SyncedIdentity si = result2.getIdentity();
        assertTrue(si instanceof DefaultSyncedIdentity);
        assertNull(si.getExternalIdRef());
        assertEquals(ID_TEST_USER, si.getId());
        assertFalse(si.isGroup());
        assertEquals(-1, si.lastSynced());
    }
    
    @Test
    public void testGetStatus() {
        assertSame(SyncResult.Status.NOP, result.getStatus());
    }
    
    @Test
    public void testGetException() {
        assertSame(ERROR,result.getException());
    }
}