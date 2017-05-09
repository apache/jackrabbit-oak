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

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class DefaultSyncResultImplTest {

    @Test
    public void testGetIdentityFromNull() {
        SyncResult res = new DefaultSyncResultImpl(null, SyncResult.Status.NOP);
        assertNull(res.getIdentity());
    }

    @Test
    public void testGetIdentity() {
        List<DefaultSyncedIdentity> l = new ArrayList();
        l.add(new DefaultSyncedIdentity("id", null, true, -1));
        l.add(new DefaultSyncedIdentity("id", new ExternalIdentityRef("id", "idp"), false, 500));

        for (DefaultSyncedIdentity si : l) {
            assertEquals(si, new DefaultSyncResultImpl(si, SyncResult.Status.NOP).getIdentity());
        }
    }

    @Test
    public void testGetStatus() {
        for (SyncResult.Status s : SyncResult.Status.values()) {
            assertSame(s, new DefaultSyncResultImpl(null, s).getStatus());
        }
    }

    @Test
    public void testSetStatus() {
        DefaultSyncResultImpl res = new DefaultSyncResultImpl(null, SyncResult.Status.NOP);
        for (SyncResult.Status s : SyncResult.Status.values()) {
            res.setStatus(s);
            assertSame(s, res.getStatus());
        }
    }
}