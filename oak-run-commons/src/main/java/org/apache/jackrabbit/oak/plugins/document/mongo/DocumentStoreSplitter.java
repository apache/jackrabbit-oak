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

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DocumentStoreSplitter {

    private static final Logger log = LoggerFactory.getLogger(DocumentStoreSplitter.class);

    MongoDocumentStore mongoStore;

    public DocumentStoreSplitter(MongoDocumentStore mongoStore) {
        this.mongoStore = mongoStore;
    }

    public <T extends Document> List<Long> split(Collection<T> collection, long modifiedSinceLowerLimit, int parts) {
        MongoCollection<BasicDBObject> dbCollection = mongoStore.getDBCollection(collection);
        BsonDocument query = new BsonDocument();
        query.append(NodeDocument.MODIFIED_IN_SECS, new BsonDocument().append("$ne", new BsonNull()));
        long oldest;
        Iterator<BasicDBObject> cursor;
        if (modifiedSinceLowerLimit <= 0) {
            cursor = dbCollection.find(query).sort(new BsonDocument(NodeDocument.MODIFIED_IN_SECS,
                    new BsonInt64(1))).limit(1).iterator();
            if (!cursor.hasNext()) {
                return Collections.emptyList();
            }
            oldest = cursor.next().getLong(NodeDocument.MODIFIED_IN_SECS);
        } else {
            oldest = modifiedSinceLowerLimit;
        }
        cursor = dbCollection.find(query).sort(new BsonDocument(NodeDocument.MODIFIED_IN_SECS,
                new BsonInt64(-1))).limit(1).iterator();
        if (!cursor.hasNext()) {
            return Collections.emptyList();
        }
        long latest = cursor.next().getLong(NodeDocument.MODIFIED_IN_SECS);
        return simpleSplit(oldest, latest, parts);
    }

    public static List<Long> simpleSplit(long start, long end, int parts) {
        if (end < start) {
            throw new IllegalArgumentException("start(" + start + ") can't be greater than end (" + end + ")");
        }
        if (start == end) {
            return Collections.singletonList(start);
        }
        if (parts > end - start) {
            log.debug("Adjusting parts according to given range {} - {}", start, end);
            parts = (int)(end - start);
        }
        long stepSize = (end - start)/parts;
        List<Long> steps = new ArrayList<>();
        StringBuilder splitPoints = new StringBuilder();
        for (long i = start; i <= end; i+=stepSize) {
            steps.add(i);
            splitPoints.append(" ").append(i);
        }
        if (steps.size() > 0 && steps.get(steps.size() - 1) != end) {
            steps.add(end);
            splitPoints.append(" ").append(end);
        }
        log.info("Split points of _modified values {}", splitPoints.toString());
        return steps;
    }
}
