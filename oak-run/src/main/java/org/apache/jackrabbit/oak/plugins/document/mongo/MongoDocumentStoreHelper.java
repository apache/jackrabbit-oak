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

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteResult;

import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;

import static com.google.common.collect.Maps.newTreeMap;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocumentHelper.commitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocumentHelper.getLocalCommitRoot;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocumentHelper.getLocalMap;

/**
 * Helper class to access package private methods on MongoDocumentStore.
 */
public class MongoDocumentStoreHelper {
    
    public static void repair(MongoDocumentStore store, String path) {
        DBCollection col = store.getDBCollection(NODES);
        String id = Utils.getIdFromPath(path);

        NodeDocument doc = store.find(NODES, id);
        if (doc == null) {
            System.out.println("No document for path " + path);
            return;
        }
        
        Set<Revision> changes = Sets.newHashSet();
        for (String key : doc.keySet()) {
            if (Utils.isPropertyName(key) || NodeDocument.isDeletedEntry(key)) {
                changes.addAll(getLocalMap(doc, key).keySet());
            }
        }

        SortedMap<Revision, String> commitRoot = newTreeMap(getLocalCommitRoot(doc));
        if (!commitRoot.keySet().retainAll(changes)) {
            System.out.println("Nothing to repair on " + path);
            return;
        }
        
        Number modCount = doc.getModCount();
        if (modCount == null) {
            System.err.println("Document does not have a modCount " + path);
            return;
        }
        DBObject query = QueryBuilder.start(Document.ID).is(id)
                .and(Document.MOD_COUNT).is(modCount).get();
        DBObject cr = new BasicDBObject();
        for (Map.Entry<Revision, String> entry : commitRoot.entrySet()) {
            cr.put(entry.getKey().toString(), entry.getValue());
        }
        
        DBObject update = new BasicDBObject();
        update.put("$set", new BasicDBObject(commitRoot(), cr));
        update.put("$inc", new BasicDBObject(Document.MOD_COUNT, 1L));
                
        WriteResult result = col.update(query, update);
        if (result.getN() == 1) {
            int num = getLocalCommitRoot(doc).size() - commitRoot.size();
            System.out.println("Removed " + num + " _commitRoot entries on " + path);
        } else {
            System.out.println("Unable to repair " + path + " (concurrent update).");
        }
        
    }

}
