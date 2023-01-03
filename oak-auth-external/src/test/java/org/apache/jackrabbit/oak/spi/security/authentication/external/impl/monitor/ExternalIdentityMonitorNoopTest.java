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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor;

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;

public class ExternalIdentityMonitorNoopTest {

    private final ExternalIdentityMonitor monitor = ExternalIdentityMonitor.NOOP;

    @Test
    public void testDoneSyncExternalIdentity() {
        SyncResult result = mock(SyncResult.class);
        monitor.doneSyncExternalIdentity(1, result, 2);
        verifyNoInteractions(result);
    }
    @Test
    public void testDoneSyncId() {
        SyncResult result = mock(SyncResult.class);
        monitor.doneSyncId(1, result);
        verifyNoInteractions(result);
    }

    @Test
    public void testSyncFailed() {
        SyncException ex = spy(new SyncException(new RuntimeException()));
        monitor.syncFailed(ex);
        verifyNoInteractions(ex);
    }
    
    @Test
    public void testGetMonitorClass() {
        assertEquals(ExternalIdentityMonitor.class, monitor.getMonitorClass());
    }

    @Test
    public void testGetMonitorProperties() {
        assertTrue(monitor.getMonitorProperties().isEmpty());
    }
}