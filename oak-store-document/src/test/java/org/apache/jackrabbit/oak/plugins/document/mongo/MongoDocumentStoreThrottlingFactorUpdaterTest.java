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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit cases for {@link MongoDocumentStoreThrottlingFactorUpdater}
 */
public class MongoDocumentStoreThrottlingFactorUpdaterTest {

    private MongoDatabase mockDb;
    private MongoDocumentStoreThrottlingFactorUpdater reader;

    @Before
    public void setUp() {
        mockDb = Mockito.mock(MongoDatabase.class);
        AtomicReference<Integer> factor = new AtomicReference<>(0);
        reader = new MongoDocumentStoreThrottlingFactorUpdater(mockDb, factor, 20);
    }

    @After
    public void tearDown() throws IOException {
        reader.close();
    }

    @Test
    public void testReadThrottlingFactorValid() {
        Document doc = new Document("enable", true)
                .append("factor", 5)
                .append("ts", System.currentTimeMillis());
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(5, reader.updateFactor());
    }

    @Test
    public void testReadThrottlingFactorMissing() {
        Document doc = new Document();
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }

    @Test
    public void testReadThrottlingFactorMissingEnable() {
        Document doc = new Document("factor", 5)
                .append("ts", System.currentTimeMillis());
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }

    @Test
    public void testReadThrottlingFactorWhenThrottlingDisabled() {
        Document doc = new Document("enable", false)
                .append("factor", 5)
                .append("ts", System.currentTimeMillis());
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }

    @Test
    public void testReadThrottlingFactorMissingFactor() {
        Document doc = new Document("enable", true)
                .append("ts", System.currentTimeMillis());
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }

    @Test
    public void testReadThrottlingFactorMissingTS() {
        Document doc = new Document("enable", true)
                .append("factor", 5);
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }

    @Test
    public void testThrottlingFactorTimestampOlderThanOneHour() {
        long oldTs = System.currentTimeMillis() - 3600001; // 1 hour + 1 ms ago
        Document doc = new Document("enable", true)
                .append("factor", 5)
                .append("ts", oldTs);
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }

    @Test
    public void testReadThrottlingFactorNegative() {
        Document doc = new Document("enable", true)
                .append("factor", -2)
                .append("ts", System.currentTimeMillis());
        Mockito.when(mockDb.runCommand(Mockito.any(Document.class))).thenReturn(doc);

        Assert.assertEquals(0, reader.updateFactor());
    }
}