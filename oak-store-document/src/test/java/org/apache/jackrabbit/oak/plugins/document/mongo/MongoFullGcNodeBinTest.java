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

package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoFullGcNodeBinTest {

    private static final List<NodeDocument> FIND_AND_UPDATE_RETURN_VALUE = List.of();
    @Mock
    DocumentStore documentStore;

    @InjectMocks
    MongoFullGcNodeBin fullGcBin;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        //mock return the list argument size
        when(documentStore.create(eq(Collection.SETTINGS), anyList())).thenReturn(true);
        when(documentStore.remove(eq(Collection.NODES), anyMap())).thenAnswer(invocation -> {
            Map<String, Long> map = invocation.getArgument(1);
            return map.size();
        });

        when(documentStore.findAndUpdate(eq(Collection.NODES), anyList())).thenAnswer(invocation -> {
            return FIND_AND_UPDATE_RETURN_VALUE;
        });

        fullGcBin.setEnabled(true);
    }

//    @Test
    public void remove() {
        Map<String, Long> orphanOrDeletedRemovalMap = new HashMap<>();
        orphanOrDeletedRemovalMap.put("key1", 1L);
        orphanOrDeletedRemovalMap.put("key2", 2L);

        int removed = fullGcBin.remove(orphanOrDeletedRemovalMap);
        //verify returned value
        assertEquals(orphanOrDeletedRemovalMap.size(), removed);

        //verify removed documents are added to bin
        ArgumentCaptor<List<UpdateOp>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(documentStore).create(eq(Collection.SETTINGS), argumentCaptor.capture());
        assertEquals(orphanOrDeletedRemovalMap.size(), argumentCaptor.getValue().size());
        assertEquals("/bin/key1", argumentCaptor.getValue().get(0).getId());
        assertTrue(argumentCaptor.getValue().get(0).isNew());
        assertEquals("/bin/key2", argumentCaptor.getValue().get(1).getId());
        assertTrue(argumentCaptor.getValue().get(1).isNew());

        //verify documents are removed
        verify(documentStore).remove(Collection.NODES, orphanOrDeletedRemovalMap);
    }

    @Test
    public void removeWhenCopyToBinFails() {
        Map<String, Long> orphanOrDeletedRemovalMap = new HashMap<>();
        orphanOrDeletedRemovalMap.put("key", 1L);
        when(documentStore.create(eq(Collection.SETTINGS), anyList())).thenThrow(new RuntimeException("Error while adding documents to bin"));

        int removed = fullGcBin.remove(orphanOrDeletedRemovalMap);

        assertEquals(0, removed);
        verify(documentStore, never()).remove(Collection.NODES, orphanOrDeletedRemovalMap);
    }

    @Test
    public void removeEmptyMap() {
        int removed = fullGcBin.remove(Map.of());
        assertEquals(0, removed);
        Mockito.verifyNoInteractions(documentStore);
    }

//    @Test
    public void removeWhenBinDisabled() {
        fullGcBin.setEnabled(false);
        Map<String, Long> orphanOrDeletedRemovalMap = new HashMap<>();
        orphanOrDeletedRemovalMap.put("key", 1L);

        fullGcBin.remove(orphanOrDeletedRemovalMap);

        verify(documentStore, never()).create(eq(Collection.SETTINGS), anyList());
    }

//    @Test
    public void findAndUpdate() {
        UpdateOp doc1 = new UpdateOp("doc1", false);
        doc1.remove("prop1.1");
        doc1.set("prop1.2", "value1.2");
        UpdateOp doc2 = new UpdateOp("doc2", false);
        doc2.remove("prop2.1");
        doc2.remove("prop2.2");

        List<UpdateOp> properties = List.of(doc1, doc2);
        List<NodeDocument> modifiedDocs = fullGcBin.findAndUpdate(properties);

        //verify removed properties are added to bin
        ArgumentCaptor<List<UpdateOp>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(documentStore).createOrUpdate(eq(Collection.SETTINGS), argumentCaptor.capture());

        List<UpdateOp> binOpList = argumentCaptor.getValue();
        UpdateOp binDoc1 = binOpList.get(0);
        assertTrue(binDoc1.isNew());

        assertEquals("/bin/doc1", binDoc1.getId());
        assertEquals(UpdateOp.Operation.Type.SET, binDoc1.getChanges().get(new UpdateOp.Key("prop1.1", null)).type);
        assertFalse(binDoc1.getChanges().containsKey(new UpdateOp.Key("prop1.2", null)));//only removed props are saved
        assertGcTimestampAdded(binDoc1);

        UpdateOp binDoc2 = binOpList.get(1);
        assertTrue(binDoc2.isNew());
        assertEquals("/bin/doc2", binDoc2.getId());

        assertEquals(UpdateOp.Operation.Type.SET, binDoc2.getChanges().get(new UpdateOp.Key("prop2.1", null)).type);
        assertEquals(UpdateOp.Operation.Type.SET, binDoc2.getChanges().get(new UpdateOp.Key("prop2.2", null)).type);
        assertGcTimestampAdded(binDoc2);


        //verify removed properties are removed from the original document
        verify(documentStore).findAndUpdate(Collection.NODES, properties);

        //verify returned value
        assertEquals(FIND_AND_UPDATE_RETURN_VALUE, modifiedDocs);
    }

    private static void assertGcTimestampAdded(UpdateOp binDoc2) {
        for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> keyOperationEntry : binDoc2.getChanges().entrySet()) {
            if (keyOperationEntry.getKey().getName().equals("_gcCollectedAt")) {
                assertEquals(UpdateOp.Operation.Type.SET, keyOperationEntry.getValue().type);
                return;
            }
        }
        fail("No _fullGcTime property found in the document");
    }

    @Test
    public void findAndUpdateWhenCopyToBinFails() {
        when(documentStore.createOrUpdate(eq(Collection.SETTINGS), anyList())).thenThrow(new RuntimeException("Error while adding documents to bin"));
        UpdateOp doc1 = new UpdateOp("doc1", false);
        doc1.remove("prop1");
        fullGcBin.findAndUpdate(List.of(doc1));
        verify(documentStore, never()).findAndUpdate(eq(Collection.NODES), anyList());
    }

//    @Test
    public void findAndUpdateWhenBinDisabled() {
        fullGcBin.setEnabled(false);
        UpdateOp doc1 = new UpdateOp("doc1", false);
        doc1.remove("prop1");
        fullGcBin.findAndUpdate(List.of(doc1));
        verify(documentStore, never()).createOrUpdate(eq(Collection.SETTINGS), anyList());
    }

    @Test
    public void findAndUpdateWhenEmptyList() {
        fullGcBin.findAndUpdate(List.of());
        verifyNoInteractions(documentStore);
    }
}