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

import com.mongodb.client.ClientSession;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.mockito.Mockito;

public class MongoClockTest {

    private final MongoClock clock = new MongoClock();

    @Test
    public void advanceSessionAndClock() {
        BsonTimestamp opT1 = new BsonTimestamp(42, 1);
        BsonDocument cT1 = new BsonDocument("clusterTime", opT1);
        ClientSession s1 = Mockito.mock(ClientSession.class);
        Mockito.when(s1.getClusterTime()).thenReturn(cT1);
        Mockito.when(s1.getOperationTime()).thenReturn(opT1);

        clock.advanceSessionAndClock(s1);

        ClientSession s2 = Mockito.mock(ClientSession.class);
        clock.advanceSession(s2);
        Mockito.verify(s2, Mockito.times(1)).advanceOperationTime(opT1);
        Mockito.verify(s2, Mockito.times(1)).advanceClusterTime(cT1);

        ClientSession s3 = Mockito.mock(ClientSession.class);
        BsonTimestamp opT2 = new BsonTimestamp(42, 2);
        Mockito.when(s3.getClusterTime()).thenReturn(cT1);
        Mockito.when(s3.getOperationTime()).thenReturn(opT2);
        clock.advanceSessionAndClock(s3);

        ClientSession s4 = Mockito.mock(ClientSession.class);
        clock.advanceSession(s4);
        Mockito.verify(s4, Mockito.times(1)).advanceOperationTime(opT2);
        Mockito.verify(s4, Mockito.times(1)).advanceClusterTime(cT1);
    }

}
