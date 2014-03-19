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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A diff cache implementation using a capped collection as a secondary cache.
 */
public class MongoDiffCache extends MemoryDiffCache {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDiffCache.class);

    private static final long MB = 1024 * 1024;

    private static final String COLLECTION_NAME = "changeLog";

    // TODO: make configurable
    private static final DBObject COLLECTION_OPTIONS = BasicDBObjectBuilder.start()
            .add("capped", true).add("size", 256 * MB).get();

    private final DBCollection changes;

    public MongoDiffCache(DB db, DocumentMK.Builder builder) {
        super(builder);
        if (db.collectionExists(COLLECTION_NAME)) {
            changes = db.getCollection(COLLECTION_NAME);
        } else {
            changes = db.createCollection(COLLECTION_NAME, COLLECTION_OPTIONS);
        }
    }

    @CheckForNull
    @Override
    public String getChanges(@Nonnull Revision from,
                             @Nonnull Revision to,
                             @Nonnull String path) {
        // first try to serve from cache
        String diff = super.getChanges(from, to, path);
        if (diff != null) {
            return diff;
        }
        // grab from mongo
        DBObject obj = changes.findOne(new BasicDBObject("_id", to.toString()));
        if (obj == null) {
            return null;
        }
        if (obj.get("_b").equals(from.toString())) {
            // apply to diff cache and serve later requests from cache
            Entry entry = super.newEntry(from, to);
            applyToDiffCache(obj, "/", entry);
            entry.done();

            DBObject current = obj;
            for (String name : PathUtils.elements(path)) {
                String n = Utils.unescapePropertyName(name);
                current = (DBObject) obj.get(n);
                if (current == null) {
                    break;
                }
            }
            if (current == null || !current.containsField("_c")) {
                // no changes here
                return "";
            } else {
                return current.get("_c").toString();
            }
        }
        // diff request goes across multiple commits
        // TODO: implement
        return null;
    }

    @Nonnull
    @Override
    public Entry newEntry(@Nonnull final Revision from,
                          @Nonnull final Revision to) {
        return new MemoryEntry(from, to) {

            private BasicDBObject commit = new BasicDBObject();

            {
                commit.put("_id", to.toString());
                commit.put("_b", from.toString());
            }

            @Override
            public void append(@Nonnull String path, @Nonnull String changes) {
                // super.append() will apply to diff cache in base class
                super.append(path, changes);
                BasicDBObject current = commit;
                for (String name : PathUtils.elements(path)) {
                    String escName = Utils.escapePropertyName(name);
                    if (current.containsField(escName)) {
                        current = (BasicDBObject) current.get(escName);
                    } else {
                        BasicDBObject child = new BasicDBObject();
                        current.append(escName, child);
                        current = child;
                    }
                }
                current.append("_c", checkNotNull(changes));
            }

            @Override
            public void done() {
                try {
                    changes.insert(commit, WriteConcern.UNACKNOWLEDGED);
                } catch (MongoException e) {
                    LOG.warn("Write back of diff cache entry failed", e);
                }
            }
        };
    }

    private void applyToDiffCache(DBObject obj,
                                  String path,
                                  Entry entry) {
        String diff = (String) obj.get("_c");
        if (diff != null) {
            entry.append(path, diff);
        }
        for (String k : obj.keySet()) {
            if (Utils.isPropertyName(k)) {
                String name = Utils.unescapePropertyName(k);
                applyToDiffCache((DBObject) obj.get(k),
                        PathUtils.concat(path, name), entry);
            }
        }
    }
}
