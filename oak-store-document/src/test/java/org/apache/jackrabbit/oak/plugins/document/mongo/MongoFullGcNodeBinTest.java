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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.FullGcNodeBin;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoFullGcNodeBinTest {

    private static final List<NodeDocument> FIND_AND_UPDATE_RETURN_VALUE = List.of();
    @Mock
    MongoDocumentStore documentStore;


    MongoFullGcNodeBin fullGcBin;

    @Mock MongoCollection<BasicDBObject> mockBinCollection;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        fullGcBin = new MongoFullGcNodeBin(documentStore, true);
        when(documentStore.remove(eq(Collection.NODES), anyMap())).thenAnswer(invocation -> {
            Map<String, Long> map = invocation.getArgument(1);
            return map.size();
        });

        when(documentStore.findAndUpdate(eq(Collection.NODES), anyList())).thenAnswer(invocation -> {
            return FIND_AND_UPDATE_RETURN_VALUE;
        });

        when(documentStore.getBinCollection()).thenReturn(mockBinCollection);
    }

    @After
    public void tearDown() {
        Mockito.reset(documentStore, mockBinCollection);
    }

    @Test
    public void defaultDisabled() {
        assertFalse(new MongoFullGcNodeBin(this.documentStore).isEnabled());
    }

    @Test
    public void enableWithConstructor() {
        assertTrue(new MongoFullGcNodeBin(this.documentStore, true).isEnabled());
    }

    @Test
    public void remove() {
        Map<String, Long> orphanOrDeletedRemovalMap = new HashMap<>();
        orphanOrDeletedRemovalMap.put("key1", 1L);
        orphanOrDeletedRemovalMap.put("key2", 2L);


        int removed = fullGcBin.remove(orphanOrDeletedRemovalMap);

        //verify returned value
        assertEquals(orphanOrDeletedRemovalMap.size(), removed);

        //verify removed documents are added to bin
        ArgumentCaptor<List<BasicDBObject>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBinCollection).insertMany(argumentCaptor.capture());
        assertEquals(orphanOrDeletedRemovalMap.size(), argumentCaptor.getValue().size());
        assertTrue(argumentCaptor.getValue().get(0).get(Document.ID).toString().matches("^\\/bin\\/key1\\-\\d+$"));
        assertTrue(argumentCaptor.getValue().get(1).get(Document.ID).toString().matches("^\\/bin\\/key2\\-\\d+$"));

        //verify documents are removed
        verify(documentStore).remove(Collection.NODES, orphanOrDeletedRemovalMap);
    }

    @Test
    public void removeWhenCopyToBinFails() {
        Map<String, Long> orphanOrDeletedRemovalMap = new HashMap<>();
        orphanOrDeletedRemovalMap.put("key", 1L);
        doThrow(new RuntimeException("Error while adding documents to bin")).when(mockBinCollection).insertMany(anyList());

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

    @Test
    public void removeWhenBinDisabled() {
        fullGcBin.setEnabled(false);
        Map<String, Long> orphanOrDeletedRemovalMap = new HashMap<>();
        orphanOrDeletedRemovalMap.put("key", 1L);

        fullGcBin.remove(orphanOrDeletedRemovalMap);

        verify(mockBinCollection, never()).insertMany(anyList());
    }

    @Test
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
        ArgumentCaptor<List<BasicDBObject>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBinCollection).insertMany(argumentCaptor.capture());

        List<BasicDBObject> binOpList = argumentCaptor.getValue();
        BasicDBObject binDoc1 = binOpList.get(0);


        assertTrue(binDoc1.get(Document.ID).toString().matches("^\\/bin\\/doc1\\-\\d+$"));
        assertTrue(binDoc1.containsField("prop1.1"));
        assertFalse(binDoc1.containsField("prop1.2"));//only removed props are saved
        assertGcTimestampAdded(binDoc1);

        BasicDBObject binDoc2 = binOpList.get(1);
        assertTrue(binDoc2.get(Document.ID).toString().matches("^\\/bin\\/doc2\\-\\d+$"));
        assertTrue(binDoc2.containsField("prop2.1"));
        assertTrue(binDoc2.containsField("prop2.2"));

        assertGcTimestampAdded(binDoc2);


        //verify removed properties are removed from the original document
        verify(documentStore).findAndUpdate(Collection.NODES, properties);

        //verify returned value
        assertEquals(FIND_AND_UPDATE_RETURN_VALUE, modifiedDocs);
    }

    private static void assertGcTimestampAdded(BasicDBObject binDoc2) {
        assertTrue(binDoc2.containsField("_gcCollectedAt"));
        assertTrue(binDoc2.get("_gcCollectedAt") instanceof Date);
    }

    @Test
    public void findAndUpdateWhenCopyToBinFails() {
        doThrow(new RuntimeException("Error while adding documents to bin")).when(mockBinCollection).insertMany(anyList());
        UpdateOp doc1 = new UpdateOp("doc1", false);
        doc1.remove("prop1");
        fullGcBin.findAndUpdate(List.of(doc1));
        verify(documentStore, never()).findAndUpdate(eq(Collection.NODES), anyList());
    }

    @Test
    public void findAndUpdateWhenBinDisabled() {
        fullGcBin.setEnabled(false);
        UpdateOp doc1 = new UpdateOp("doc1", false);
        doc1.remove("prop1");
        fullGcBin.findAndUpdate(List.of(doc1));
        verify(mockBinCollection, never()).insertMany(anyList());
    }

    @Test
    public void findAndUpdateWhenEmptyList() {
        fullGcBin.findAndUpdate(List.of());
        verifyNoInteractions(documentStore);
    }
}