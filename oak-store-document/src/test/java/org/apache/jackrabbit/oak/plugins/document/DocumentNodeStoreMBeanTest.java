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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_VER_GC_MAX_AGE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DocumentNodeStoreMBeanTest {

    private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);

    private static final long REVISION_GC_MAX_AGE_MILLIS = TimeUnit.SECONDS.toMillis(DEFAULT_VER_GC_MAX_AGE);

    private static final long SLIGHTLY_OLDER_THAN_REVISION_GC_MAX_AGE_MILLIS = TimeUnit.SECONDS.toMillis(DEFAULT_VER_GC_MAX_AGE) + 5000;

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;

    private DocumentStore store;

    private DocumentNodeStore ns;

    private DocumentNodeStoreMBean bean;

    @Before
    public void init() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        store = spy(new MemoryDocumentStore());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        ns = builderProvider.newBuilder()
                .setDocumentStore(store)
                .setRevisionGCMaxAge(REVISION_GC_MAX_AGE_MILLIS)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .clock(clock)
                .getNodeStore();
        bean = ns.getMBean();
        clock.waitUntil(clock.getTime() + REVISION_GC_MAX_AGE_MILLIS * 2);
        ns.runBackgroundOperations();
    }

    @AfterClass
    public static void after() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void createCheckpointWithInvalidRevisionFormat() {
        assertThrows(IllegalArgumentException.class, () ->
                bean.createCheckpoint("invalid", ONE_HOUR, false));
    }

    @Test
    public void createCheckpointWithFutureRevision() {
        Revision r = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(r);
        String revisionString = new Revision(r.getTimestamp() + TimeUnit.HOURS.toMillis(1), 0, r.getClusterId()).toString();
        assertThrows(IllegalArgumentException.class, () ->
                bean.createCheckpoint(revisionString, ONE_HOUR, false));
    }

    @Test
    public void createCheckpointWithRevisionTooOld() {
        Revision r = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(r);
        String revisionString = new Revision(r.getTimestamp() - SLIGHTLY_OLDER_THAN_REVISION_GC_MAX_AGE_MILLIS, 0, r.getClusterId()).toString();
        assertThrows(IllegalArgumentException.class, () ->
                bean.createCheckpoint(revisionString, ONE_HOUR, false));
    }

    @Test
    public void createCheckpointForceWithRevisionTooOld() {
        Revision r = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(r);
        String revisionString = new Revision(r.getTimestamp() - SLIGHTLY_OLDER_THAN_REVISION_GC_MAX_AGE_MILLIS, 0, r.getClusterId()).toString();
        String result = bean.createCheckpoint(revisionString, ONE_HOUR, true);
        String checkpoint = result.substring(result.indexOf("[") + 1, result.indexOf("]"));
        assertNotNull(ns.retrieve(checkpoint));
    }

    @Test
    public void createCheckpointBeforeExistingCheckpoint() {
        ns.checkpoint(ONE_HOUR);
        Revision r = ns.getHeadRevision().getRevision(ns.getClusterId());
        assertNotNull(r);
        String revisionString = new Revision(r.getTimestamp() - ONE_HOUR, 0, r.getClusterId()).toString();
        String result = bean.createCheckpoint(revisionString, ONE_HOUR, false);
        String checkpoint = result.substring(result.indexOf("[") + 1, result.indexOf("]"));
        assertNotNull(ns.retrieve(checkpoint));
    }

    @Test
    public void cleanAllCaches() {
        verify(store, never()).invalidateCache();
        bean.cleanAllCaches();
        verify(store, times(1)).invalidateCache();
    }

    @Test
    public void cleanIndividualCache() {
        bean.cleanIndividualCache("NODE");
        verify(store, never()).invalidateCache();
        bean.cleanIndividualCache("DOCUMENT");
        verify(store, times(1)).invalidateCache();
    }
}
