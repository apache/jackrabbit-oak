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

import com.mongodb.DBObject;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import static com.google.common.collect.Iterables.transform;

/**
 * <code>MongoDocumentStoreCheckHelper</code>...
 */
public class MongoDocumentStoreCheckHelper {

    public static long getEstimatedDocumentCount(MongoDocumentStore store) {
        return store.getDBCollection(Collection.NODES).estimatedDocumentCount();
    }

    public static Iterable<NodeDocument> getAllNodeDocuments(MongoDocumentStore store) {
        return transform(store.getDBCollection(Collection.NODES)
                        .find(DBObject.class)
                        .sort(new BsonDocument("$natural", new BsonInt32(1))),
                input -> store.convertFromDBObject(Collection.NODES, input)
        );
    }
}
