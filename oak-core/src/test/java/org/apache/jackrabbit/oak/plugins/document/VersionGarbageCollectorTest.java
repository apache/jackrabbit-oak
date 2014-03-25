/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Before;
import org.junit.Test;

public class VersionGarbageCollectorTest {
    private Clock clock;

    private DocumentNodeStore store;

    private VersionGarbageCollector gc;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        store = new DocumentMK.Builder().clock(clock).getNodeStore();
        gc = store.getVersionGarbageCollector();

        //Baseline the clock
        clock.waitUntil(Revision.getCurrentTimestamp());
    }

    @Test
    public void gcIgnoredForCheckpoint() throws Exception {
        long expiryTime = 100, maxAge = 20;

        Revision cp = Revision.fromString(store.checkpoint(expiryTime));
        gc.setMaxRevisionAge(maxAge);

        //Fast forward time to future but before expiry of checkpoint
        clock.waitUntil(cp.getTimestamp() + expiryTime - maxAge);
        VersionGCStats stats = gc.gc();
        assertTrue(stats.ignoredGCDueToCheckPoint);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc.gc();
        assertFalse("GC should be performed", stats.ignoredGCDueToCheckPoint);
    }
}