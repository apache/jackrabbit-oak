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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_GENERATION_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;

/**
 * Unit tests for {@link VersionGarbageCollector}
 */
public class VersionGarbageCollectorTest {

    final DocumentStore ds = Mockito.mock(DocumentStore.class);
    final DocumentNodeStore ns = Mockito.mock(DocumentNodeStore.class);
    final VersionGCSupport gcSupport = Mockito.mock(VersionGCSupport.class);
    final int fullGcGen = 2;
    VersionGarbageCollector vgc;

    @Before
    public void before() {
        Mockito.when(gcSupport.getDocumentStore()).thenReturn(ds);
    }

    @Test
    public void testResetFullGcIfGenChangeWithFullGcDisabled() {
        // Setup: ensure no settings document exists
        final Document doc = Mockito.mock(Document.class);
        Mockito.when(doc.get(SETTINGS_COLLECTION_FULL_GC_GENERATION_PROP)).thenReturn(5);
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(doc);

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, false, false, true, 3, 0.0, 100, 1000, 86400, 2);

        // no database query if generation is default value.
        Mockito.verifyNoInteractions(ds);
    }

    @Test
    public void testResetFullGcIfGenChangeWithDefaultValue() {
        // Setup: ensure no settings document exists
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(null);

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, true, false, true, 3, 0.0, 100, 1000, 86400, 0);

        // no database query if generation is default value.
        Mockito.verifyNoInteractions(ds);
    }

    @Test
    public void testResetFullGcIfGenChangeWithNoDocument() {
        // Setup: ensure no settings document exists
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(null);

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, true, false, true, 3, 0.0, 100, 1000, 86400, fullGcGen);

        Mockito.verify(ds, Mockito.times(1)).find(SETTINGS, SETTINGS_COLLECTION_ID);
        Mockito.verify(ds, Mockito.times(1)).createOrUpdate(Mockito.eq(SETTINGS), (UpdateOp) Mockito.any());
        // verify no calls to specific methods
        Mockito.verify(ds, Mockito.never()).remove(Mockito.any(), (String) Mockito.any());
        Mockito.verify(ds, Mockito.never()).findAndUpdate(Mockito.any(), (UpdateOp) Mockito.any());
    }

    @Test
    public void testResetFullGcIfGenChangeWithEmptyGeneration() {
        // Setup: document exists but has non-numeric generation value
        final Document doc = Mockito.mock(Document.class);
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(doc);
        Mockito.when(doc.get(SETTINGS_COLLECTION_FULL_GC_GENERATION_PROP)).thenReturn(null);

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, true, false, true, 3, 0.0, 100, 1000, 86400, fullGcGen);

        // Verify: logs warning and persists new value
        Mockito.verify(ds).find(SETTINGS, SETTINGS_COLLECTION_ID);
        Mockito.verify(ds, Mockito.times(1)).findAndUpdate(Mockito.any(), (UpdateOp) Mockito.any());
        Mockito.verify(ds).createOrUpdate(Mockito.eq(SETTINGS), (UpdateOp) Mockito.any());

        // verify no calls to specific methods
        Mockito.verify(ds, Mockito.never()).remove(Mockito.any(), (String) Mockito.any());
    }

    @Test
    public void testResetFullGcIfGenChangeWithNonNumberGeneration() {
        // Setup: document exists but has non-numeric generation value
        final Document doc = Mockito.mock(Document.class);
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(doc);
        Mockito.when(doc.get(SETTINGS_COLLECTION_FULL_GC_GENERATION_PROP)).thenReturn("not-a-number");

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, true, false, true, 3, 0.0, 100, 1000, 86400, fullGcGen);

        // Verify: logs warning and persists new value
        Mockito.verify(ds).find(SETTINGS, SETTINGS_COLLECTION_ID);
        Mockito.verify(ds, Mockito.times(1)).findAndUpdate(Mockito.any(), (UpdateOp) Mockito.any());
        Mockito.verify(ds).createOrUpdate(Mockito.eq(SETTINGS), (UpdateOp) Mockito.any());

        // verify no calls to specific methods
        Mockito.verify(ds, Mockito.never()).remove(Mockito.any(), (String) Mockito.any());
    }

    @Test
    public void testResetFullGcIfGenChangeWithLowerGeneration() {
        // Setup: document exists with higher generation
        final Document doc = Mockito.mock(Document.class);
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(doc);
        Mockito.when(doc.get(SETTINGS_COLLECTION_FULL_GC_GENERATION_PROP)).thenReturn(5);

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, true, false, true, 3, 0.0, 100, 1000, 86400, fullGcGen);

        // Verify: logs warning and persists new value
        Mockito.verify(ds).find(SETTINGS, SETTINGS_COLLECTION_ID);
        Mockito.verify(ds, Mockito.never()).createOrUpdate(Mockito.eq(SETTINGS), (UpdateOp) Mockito.any());

        // verify no calls to specific methods
        Mockito.verify(ds, Mockito.never()).remove(Mockito.any(), (String) Mockito.any());
        Mockito.verify(ds, Mockito.never()).findAndUpdate(Mockito.any(), (UpdateOp) Mockito.any());
    }

    @Test
    public void testResetFullGcIfGenChangeWithHigherGeneration() {
        // Setup: document exists with lower generation
        final Document doc = Mockito.mock(Document.class);
        Mockito.when(ds.find(SETTINGS, SETTINGS_COLLECTION_ID)).thenReturn(doc);
        Mockito.when(doc.get(SETTINGS_COLLECTION_FULL_GC_GENERATION_PROP)).thenReturn(1);

        // Execute
        vgc = new VersionGarbageCollector(ns, gcSupport, true, false, true, 3, 0.0, 100, 1000, 86400, fullGcGen);

        // Verify: logs warning and persists new value
        Mockito.verify(ds, Mockito.times(1)).find(SETTINGS, SETTINGS_COLLECTION_ID);
        Mockito.verify(ds, Mockito.times(1)).findAndUpdate(Mockito.eq(SETTINGS), (UpdateOp) Mockito.any());
        Mockito.verify(ds, Mockito.times(1)).createOrUpdate(Mockito.eq(SETTINGS), (UpdateOp) Mockito.any());

        // verify no calls to specific methods
        Mockito.verify(ds, Mockito.never()).remove(Mockito.any(), (String) Mockito.any());
    }

}