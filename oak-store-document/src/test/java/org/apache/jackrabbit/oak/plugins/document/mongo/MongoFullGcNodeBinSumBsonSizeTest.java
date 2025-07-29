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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MongoFullGcNodeBinSumBsonSizeTest {

    @Mock
    private MongoFullGcNodeBin delegate;

    @Mock
    private MongoDocumentStore store;

    @Mock
    private MongoCollection<BasicDBObject> nodesCollection;

    @Mock
    private MongoCollection<BasicDBObject> settingsCollection;

    @Mock
    private AggregateIterable<BasicDBObject> aggregateIterable;

    private MongoFullGcNodeBinSumBsonSize wrapper;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(delegate.getMongoDocumentStore()).thenReturn(store);
        when(store.getDBCollection(Collection.NODES)).thenReturn(nodesCollection);
        when(store.getDBCollection(Collection.SETTINGS)).thenReturn(settingsCollection);
        when(nodesCollection.aggregate(any())).thenReturn(aggregateIterable);
        wrapper = new MongoFullGcNodeBinSumBsonSize(delegate);
    }

    private void mockBsonSizeCalculation(long... totalSizes) {
        List<BasicDBObject> results = new ArrayList<>();
        for (long totalSize : totalSizes) {
            BasicDBObject aggregateResult = new BasicDBObject("_id", null)
                .append("totalSize", totalSize);
            results.add(aggregateResult);
        }
        if (totalSizes.length > 2) {
            throw new IllegalArgumentException("Max 2 arguments are supported");
        }
        //return based on argument number
        if (totalSizes.length == 2) {
            when(aggregateIterable.first())
                .thenReturn(results.get(0))
                .thenReturn(results.get(1));
        } else {
            when(aggregateIterable.first()).thenReturn(results.get(0));
        }
    }

    @Test
    public void testSetEnabled() {
        wrapper.setEnabled(true);
        verify(delegate).setEnabled(true);
    }

    @Test
    public void testFindAndUpdateWithSuccessfulUpdate() {
        // Setup
        List<UpdateOp> updateOps = new ArrayList<>();
        UpdateOp op1 = new UpdateOp("doc1", false);
        updateOps.add(op1);

        List<NodeDocument> expectedDocs = new ArrayList<>();
        expectedDocs.add(NodeDocument.NULL);

        List<NodeDocument> docs = new ArrayList<>();
        docs.add(NodeDocument.NULL);
        when(delegate.findAndUpdate(updateOps)).thenReturn(docs);
        //before size 100, after update -> size 50
        mockBsonSizeCalculation(100L, 50);
        
        // Execute
        List<NodeDocument> result = wrapper.findAndUpdate(updateOps);
        
        // Verify
        assertEquals(expectedDocs, result);
        verify(delegate).findAndUpdate(updateOps);
        
        // Verify bson size update
        ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
        ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);
        verify(settingsCollection).updateOne(queryCaptor.capture(), updateCaptor.capture());
        
        // Verify query
        Bson query = queryCaptor.getValue();
        Document queryDoc = Document.parse(query.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).toJson());
        assertEquals("versionGC", queryDoc.get("_id"));
        
        // Verify update
        Bson update = updateCaptor.getValue();
        Document updateDoc = Document.parse(update.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).toJson());
        Document inc = updateDoc.get("$inc", Document.class);
        assertEquals(50, ((Number) inc.get("fullGcRemovedTotalBsonSize")).intValue());
    }

    @Test
    public void testFindAndUpdateWithInitialBsonSizeLessThenUpdatedBsonSize() {
        // Setup
        List<UpdateOp> updateOps = new ArrayList<>();
        UpdateOp op1 = new UpdateOp("doc1", false);
        updateOps.add(op1);

        List<NodeDocument> docs = new ArrayList<>();
        docs.add(NodeDocument.NULL);
        when(delegate.findAndUpdate(updateOps)).thenReturn(docs);
        //before size 100, after update -> size 150
        mockBsonSizeCalculation(100L, 150L);

        // Execute
        wrapper.findAndUpdate(updateOps);

        // Verify
        verify(delegate).findAndUpdate(updateOps);
        verify(settingsCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }

    @Test
    public void testFindAndUpdateWithNoUpdates() {
        // Setup
        List<UpdateOp> updateOps = new ArrayList<>();
        UpdateOp op1 = new UpdateOp("doc1", false);
        updateOps.add(op1);
        
        when(delegate.findAndUpdate(updateOps)).thenReturn(new ArrayList<>());
        mockBsonSizeCalculation(100L);
        
        // Execute
        List<NodeDocument> result = wrapper.findAndUpdate(updateOps);
        
        // Verify
        assertTrue(result.isEmpty());
        verify(delegate).findAndUpdate(updateOps);
        verify(settingsCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }

    @Test
    public void testFindAndUpdateWithEmptyList() {
        // Setup
        List<UpdateOp> updateOps = new ArrayList<>();
        
        when(delegate.findAndUpdate(updateOps)).thenReturn(new ArrayList<>());
        
        // Execute
        List<NodeDocument> result = wrapper.findAndUpdate(updateOps);
        
        // Verify
        assertTrue(result.isEmpty());
        verify(delegate).findAndUpdate(updateOps);
        verify(settingsCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }

    @Test
    public void testRemoveWithSuccessfulRemoval() {
        // Setup
        Map<String, Long> removalMap = new HashMap<>();
        removalMap.put("doc1", 1L);
        removalMap.put("doc2", 2L);
        
        when(delegate.remove(removalMap)).thenReturn(2);
        mockBsonSizeCalculation(200L);
        
        // Execute
        int result = wrapper.remove(removalMap);
        
        // Verify
        assertEquals(2, result);
        verify(delegate).remove(removalMap);
        
        // Verify bson size update
        ArgumentCaptor<Bson> queryCaptor = ArgumentCaptor.forClass(Bson.class);
        ArgumentCaptor<Bson> updateCaptor = ArgumentCaptor.forClass(Bson.class);
        verify(settingsCollection).updateOne(queryCaptor.capture(), updateCaptor.capture());
        
        // Verify query
        Bson query = queryCaptor.getValue();
        Document queryDoc = Document.parse(query.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).toJson());
        assertEquals("versionGC", queryDoc.get("_id"));
        
        // Verify update
        Bson update = updateCaptor.getValue();
        Document updateDoc = Document.parse(update.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).toJson());
        Document inc = updateDoc.get("$inc", Document.class);
        assertEquals(200, ((Number) inc.get("fullGcRemovedTotalBsonSize")).intValue());
    }

    @Test
    public void testRemoveWithNoBsonSize() {
        // Setup
        Map<String, Long> removalMap = new HashMap<>();
        removalMap.put("doc1", 1L);
        
        when(delegate.remove(removalMap)).thenReturn(1);
        mockBsonSizeCalculation(0L);
        
        // Execute
        int result = wrapper.remove(removalMap);
        
        // Verify
        assertEquals(1, result);
        verify(delegate).remove(removalMap);
        verify(settingsCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }

    @Test
    public void testRemoveWithEmptyMap() {
        // Setup
        Map<String, Long> removalMap = new HashMap<>();
        
        when(delegate.remove(removalMap)).thenReturn(0);
        
        // Execute
        int result = wrapper.remove(removalMap);
        
        // Verify
        assertEquals(0, result);
        verify(delegate).remove(removalMap);
        verify(settingsCollection, never()).updateOne(any(Bson.class), any(Bson.class));
    }

}