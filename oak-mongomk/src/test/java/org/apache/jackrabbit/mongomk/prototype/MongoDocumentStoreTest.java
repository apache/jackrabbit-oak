/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.jackrabbit.mongomk.AbstractMongoConnectionTest;
import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.junit.Test;

public class MongoDocumentStoreTest extends AbstractMongoConnectionTest {

    @Test
    public void addGetAndRemove() throws Exception {
        DocumentStore docStore = new MongoDocumentStore(mongoConnection.getDB());

        UpdateOp updateOp = new UpdateOp("/");
        updateOp.addMapEntry("property1", "key1", "value1");
        updateOp.increment("property2", 1);
        updateOp.set("property3", "value3");
        docStore.createOrUpdate(Collection.NODES, updateOp);
        Map<String, Object> obj = docStore.find(Collection.NODES, "/");

        Map property1 = (Map)obj.get("property1");
        String value1 = (String)property1.get("key1");
        assertEquals("value1", value1);

        Long value2 = (Long)obj.get("property2");
        assertEquals(Long.valueOf(1), value2);

        String value3 = (String)obj.get("property3");
        assertEquals("value3", value3);

        docStore.remove(Collection.NODES, "/");
        obj = docStore.find(Collection.NODES, "/");
        assertTrue(obj == null);
    }
}