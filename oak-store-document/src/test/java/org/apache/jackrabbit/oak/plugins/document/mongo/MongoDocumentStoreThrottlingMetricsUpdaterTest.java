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

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;

import static java.lang.System.currentTimeMillis;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreThrottlingMetricsUpdater.TS_TIME;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStoreThrottlingMetricsUpdater.updateOplogWindow;
import static org.junit.Assert.assertEquals;

public class MongoDocumentStoreThrottlingMetricsUpdaterTest {

    @Test
    public void testUpdateOplogWindow() {

        double oplogWindow = updateOplogWindow(1024, 512,
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis(), 0)),
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis() + 3600, 0)));

        assertEquals(2.0, oplogWindow, 0.00001);
    }

    @Test
    public void testUpdateOplogWindow_2() {

        double oplogWindow = updateOplogWindow(1024, 1024,
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis(), 0)),
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis() + 3600, 0)));

        assertEquals(1.0, oplogWindow, 0.001);
    }

    @Test
    public void testUpdateOplogWindow_3() {

        double oplogWindow = updateOplogWindow(1024, 102.4,
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis(), 0)),
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis() + 3600, 0)));

        assertEquals(10.0, oplogWindow, 0.001);
    }

    @Test
    public void testUpdateOplogWindow_ZeroUsedSize() {

        double oplogWindow = updateOplogWindow(1024, 0,
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis(), 0)),
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis() + 3600, 0)));

        assertEquals(Integer.MAX_VALUE, oplogWindow, 0.001);
    }

    @Test
    public void testUpdateOplogWindow_OneOplogEntry() {

        double oplogWindow = updateOplogWindow(1024, 1024,
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis(), 0)),
                new Document(TS_TIME, new BsonTimestamp((int) currentTimeMillis(), 0)));

        assertEquals(Integer.MAX_VALUE, oplogWindow, 0.001);
    }

}
