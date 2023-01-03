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
package org.apache.jackrabbit.oak.plugins.document.util;

import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.Throttler;
import org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollector;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.jackrabbit.oak.plugins.document.Collection.BLOBS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Junit for {@link ThrottlingDocumentStoreWrapper}
 */
@RunWith(MockitoJUnitRunner.class)
public class ThrottlingDocumentStoreWrapperTest {

    private static final UpdateOp UPDATE_OP = new UpdateOp("1", false);
    private DocumentStore memStore;
    private Throttler throttler;

    private ThrottlingStatsCollector statsCollector;


    @Before
    public void setUp() {
        memStore = mock(MemoryDocumentStore.class);
        statsCollector = mock(ThrottlingStatsCollector.class);
        throttler = mock(Throttler.class);
        when(memStore.throttler()).thenReturn(throttler);
    }

    @Test
    public void testDefaultThrottler() {
        DocumentStore store = new ThrottlingDocumentStoreWrapper(new MemoryDocumentStore(), statsCollector);
        Throttler throttler = store.throttler();
        assertEquals(0, throttler.throttlingTime());
    }

    @Test
    public void testNoThrottlingForClusterNodes() {
        DocumentStore store = new ThrottlingDocumentStoreWrapper(memStore, statsCollector);
        store.createOrUpdate(CLUSTER_NODES, UPDATE_OP);
        verify(memStore, never()).throttler();
    }

    @Test
    public void testThrottlingForNodes() {
        when(throttler.throttlingTime()).thenReturn(10L);
        DocumentStore store = new ThrottlingDocumentStoreWrapper(memStore, statsCollector);
        store.createOrUpdate(NODES, UPDATE_OP);
        verify(memStore, atLeastOnce()).throttler();
    }

    @Test
    public void testThrottlingForJournal() {
        when(throttler.throttlingTime()).thenReturn(10L);
        DocumentStore store = new ThrottlingDocumentStoreWrapper(memStore, statsCollector);
        store.createOrUpdate(JOURNAL, UPDATE_OP);
        verify(memStore, atLeastOnce()).throttler();
    }

    @Test
    public void testThrottlingForSettings() {
        when(throttler.throttlingTime()).thenReturn(10L);
        DocumentStore store = new ThrottlingDocumentStoreWrapper(memStore, statsCollector);
        store.createOrUpdate(SETTINGS, UPDATE_OP);
        verify(memStore, atLeastOnce()).throttler();
    }

    @Test
    public void testThrottlingForBlobs() {
        when(throttler.throttlingTime()).thenReturn(10L);
        DocumentStore store = new ThrottlingDocumentStoreWrapper(memStore, statsCollector);
        store.createOrUpdate(BLOBS, UPDATE_OP);
        verify(memStore, atLeastOnce()).throttler();
    }

}
