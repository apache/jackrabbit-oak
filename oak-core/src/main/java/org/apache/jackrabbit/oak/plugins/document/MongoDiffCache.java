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

import java.util.Set;
import java.util.concurrent.locks.Lock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Striped;
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

    private static final String COLLECTION_NAME = "changes";

    private final DBCollection changes;

    private final Cache<String, String> blacklist = CacheBuilder.newBuilder().maximumSize(1024).build();

    private final Striped<Lock> locks = Striped.lock(16);

    public MongoDiffCache(DB db, int sizeMB, DocumentMK.Builder builder) {
        super(builder);
        if (db.collectionExists(COLLECTION_NAME)) {
            changes = db.getCollection(COLLECTION_NAME);
        } else {
            changes = db.createCollection(COLLECTION_NAME,
                    BasicDBObjectBuilder.start().add("capped", true)
                            .add("size", sizeMB * MB).get());
        }
    }

    @CheckForNull
    @Override
    public String getChanges(@Nonnull Revision from,
                             @Nonnull Revision to,
                             @Nonnull String path) {
        Lock lock = locks.get(from);
        lock.lock();
        try {
            // first try to serve from cache
            String diff = super.getChanges(from, to, path);
            if (diff != null) {
                return diff;
            }
            if (from.getClusterId() != to.getClusterId()) {
                return null;
            }
            // check blacklist
            if (blacklist.getIfPresent(from + "/" + to) != null) {
                return null;
            }
            Revision id = to;
            Diff d = null;
            int numCommits = 0;
            for (;;) {
                // grab from mongo
                DBObject obj = changes.findOne(new BasicDBObject("_id", id.toString()));
                if (obj == null) {
                    return null;
                }
                numCommits++;
                if (numCommits > 32) {
                    // do not merge more than 32 commits
                    blacklist.put(from + "/" + to, "");
                    return null;
                }
                if (d == null) {
                    d = new Diff(obj);
                } else {
                    d.mergeBeforeDiff(new Diff(obj));
                }

                // the from revision of the current diff
                id = Revision.fromString((String) obj.get("_b"));
                if (from.equals(id)) {
                    // diff is complete
                    LOG.debug("Built diff from {} commits", numCommits);
                    // apply to diff cache and serve later requests from cache
                    d.applyToEntry(super.newEntry(from, to)).done();
                    // return changes
                    return d.getChanges(path);
                }

                if (StableRevisionComparator.INSTANCE.compare(id, from) < 0) {
                    break;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    @Override
    public Entry newEntry(@Nonnull final Revision from,
                          @Nonnull final Revision to) {
        return new MemoryEntry(from, to) {

            private Diff commit = new Diff(from, to);

            @Override
            public void append(@Nonnull String path, @Nonnull String changes) {
                // super.append() will apply to diff cache in base class
                super.append(path, changes);
                commit.append(path, changes);
            }

            @Override
            public void done() {
                try {
                    changes.insert(commit.doc, WriteConcern.UNACKNOWLEDGED);
                } catch (MongoException e) {
                    LOG.warn("Write back of diff cache entry failed", e);
                }
            }
        };
    }

    static class Diff {

        private final DBObject doc;

        Diff(Revision from, Revision to) {
            this.doc = new BasicDBObject();
            this.doc.put("_id", to.toString());
            this.doc.put("_b", from.toString());
        }

        Diff(DBObject doc) {
            this.doc = doc;
        }

        void append(String path, String changes) {
            DBObject current = doc;
            for (String name : PathUtils.elements(path)) {
                String escName = Utils.escapePropertyName(name);
                if (current.containsField(escName)) {
                    current = (DBObject) current.get(escName);
                } else {
                    BasicDBObject child = new BasicDBObject();
                    current.put(escName, child);
                    current = child;
                }
            }
            current.put("_c", checkNotNull(changes));
        }

        String getChanges(String path) {
            DBObject current = doc;
            for (String name : PathUtils.elements(path)) {
                String n = Utils.unescapePropertyName(name);
                current = (DBObject) current.get(n);
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

        Entry applyToEntry(Entry entry) {
            applyInternal(doc, "/", entry);
            return entry;
        }

        void mergeBeforeDiff(Diff before) {
            mergeInternal(doc, before.doc, Sets.<String>newHashSet(),
                    Sets.<String>newHashSet(), Sets.<String>newHashSet());
            doc.put("_b", before.doc.get("_b"));
        }

        private static void mergeInternal(DBObject doc, DBObject before,
                                          final Set<String> added,
                                          final Set<String> removed,
                                          final Set<String> modified) {
            added.clear();
            removed.clear();
            modified.clear();
            String changes = (String) doc.get("_c");
            if (changes != null) {
                parse(changes, new ParserCallback() {
                    @Override
                    public void added(String name) {
                        added.add(name);
                    }

                    @Override
                    public void removed(String name) {
                        removed.add(name);
                    }

                    @Override
                    public void modified(String name) {
                        modified.add(name);
                    }
                });
            }

            changes = (String) before.get("_c");
            if (changes != null) {
                parse(changes, new ParserCallback() {
                    @Override
                    public void added(String name) {
                        if (modified.remove(name) || !removed.remove(name)) {
                            added.add(name);
                        }
                    }

                    @Override
                    public void removed(String name) {
                        if (added.remove(name)) {
                            modified.add(name);
                        } else {
                            removed.add(name);
                        }
                    }

                    @Override
                    public void modified(String name) {
                        if (added.remove(name) || !removed.contains(name)) {
                            modified.add(name);
                        }
                    }
                });
                doc.put("_c", serialize(added, removed, modified));
            }

            // merge recursively
            for (String k : before.keySet()) {
                if (Utils.isPropertyName(k)) {
                    DBObject beforeChild = (DBObject) before.get(k);
                    DBObject thisChild = (DBObject) doc.get(k);
                    if (thisChild == null) {
                        thisChild = new BasicDBObject();
                        doc.put(k, thisChild);
                    }
                    mergeInternal(thisChild, beforeChild, added, removed, modified);
                }
            }
        }

        private static String serialize(final Set<String> added,
                                        final Set<String> removed,
                                        final Set<String> modified) {
            JsopWriter w = new JsopStream();
            for (String p : added) {
                w.tag('+').key(PathUtils.getName(p)).object().endObject().newline();
            }
            for (String p : removed) {
                w.tag('-').value(PathUtils.getName(p)).newline();
            }
            for (String p : modified) {
                w.tag('^').key(PathUtils.getName(p)).object().endObject().newline();
            }
            return w.toString();
        }

        private static void parse(String changes, ParserCallback callback) {
            JsopTokenizer t = new JsopTokenizer(changes);
            for (;;) {
                int r = t.read();
                if (r == JsopReader.END) {
                    break;
                }
                switch (r) {
                    case '+': {
                        callback.added(t.readString());
                        t.read(':');
                        t.read('{');
                        t.read('}');
                        break;
                    }
                    case '-': {
                        callback.removed(t.readString());
                        break;
                    }
                    case '^': {
                        callback.modified(t.readString());
                        t.read(':');
                        t.read('{');
                        t.read('}');
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("jsonDiff: illegal token '"
                                + t.getToken() + "' at pos: " + t.getLastPos() + ' ' + changes);
                }
            }
        }

        private void applyInternal(DBObject obj,
                                   String path,
                                   Entry entry) {
            String diff = (String) obj.get("_c");
            if (diff != null) {
                entry.append(path, diff);
            }
            for (String k : obj.keySet()) {
                if (Utils.isPropertyName(k)) {
                    String name = Utils.unescapePropertyName(k);
                    applyInternal((DBObject) obj.get(k), PathUtils.concat(path, name), entry);
                }
            }
        }

        private interface ParserCallback {

            void added(String name);
            void removed(String name);
            void modified(String name);
        }
    }
}
