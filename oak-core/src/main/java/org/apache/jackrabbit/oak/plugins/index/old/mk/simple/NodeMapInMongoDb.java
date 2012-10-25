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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.jackrabbit.mk.util.Cache;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl.ChildVisitor;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

/**
 * A node map that stores data in a Mongo DB.
 */
public class NodeMapInMongoDb extends NodeMap implements Cache.Backend<Long, NodeImpl> {

    private static final String DB = "ds";
    private static final String NODES_COLLECTION = "nodes";
    private static final String SETTINGS_COLLECTION = "settings";
    private static final String KEY_FIELD = "key";
    private static final String DATA_FIELD = "data";

    final HashMap<Long, NodeImpl> temp = new HashMap<Long, NodeImpl>();
    private final TreeMap<String, String> properties = new TreeMap<String, String>();
    private final Cache<Long, NodeImpl> cache = Cache.newInstance(this, 10 * 1024 * 1024);
    private final HashMap<Long, Long> pos = new HashMap<Long, Long>();
    private long nextId;
    private NodeImpl root = new NodeImpl(this, 0);

    private Mongo con;
    private DB db;
    private DBCollection nodesStore;
    private DBCollection settingsStore;

    NodeMapInMongoDb(String dir) {
        try {
            con = new Mongo();
            db = con.getDB(DB);
            db.setWriteConcern(WriteConcern.SAFE);
            nodesStore = db.getCollection(NODES_COLLECTION);
            nodesStore.ensureIndex(
                    new BasicDBObject(KEY_FIELD, 1),
                    new BasicDBObject("unique", true));

            settingsStore = db.getCollection(SETTINGS_COLLECTION);
            settingsStore.ensureIndex(
                    new BasicDBObject(KEY_FIELD, 1),
                    new BasicDBObject("unique", true));

            BasicDBObject k = new BasicDBObject(KEY_FIELD, "root");
            DBObject dataObject = settingsStore.findOne(k);
            if (dataObject != null) {
                root.setId(NodeId.get(Long.parseLong(dataObject.get(DATA_FIELD).toString())));
            }

        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    @Override
    public synchronized NodeId addNode(NodeImpl node) {
        return addNode(node, true);
    }

    private NodeId addNode(NodeImpl node, boolean allowInline) {
        NodeId x = node.getId();
        if (x == null) {
            if (allowInline && node.getDescendantInlineCount() < descendantInlineCount) {
                x = NodeId.getInline(node);
            } else {
                long t = -temp.size() - 1;
                temp.put(t, node);
                x = NodeId.get(t);
            }
            node.setId(x);
        }
        return x;
    }

    @Override
    public NodeImpl getNode(long x) {
        return x < 0 ? temp.get(x) : cache.get(x);
    }

    @Override
    public NodeImpl load(Long key) {
        try {
            BasicDBObject k = new BasicDBObject(KEY_FIELD, key);
            DBObject dataObject = nodesStore.findOne(k);
            String n = (String) dataObject.get(DATA_FIELD);
            return NodeImpl.fromString(this, n);
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
    }

    @Override
    public synchronized NodeId commit(NodeImpl newRoot) {
        addNode(newRoot, false);
        try {
            final NodeMap map = this;
            final ArrayList<Long> list = new ArrayList<Long>();
            newRoot.visit(new ChildVisitor() {
                @Override
                public void accept(NodeId childId) {
                    if (childId.getLong() < 0) {
                        NodeImpl t = temp.get(childId.getLong());
                        t.visit(this);
                        list.add(childId.getLong());
                        if (hash) {
                            t.getHash();
                        }
                    } else if (childId.isInline()) {
                        NodeImpl t = childId.getNode(map);
                        t.visit(this);
                        if (hash) {
                            t.getHash();
                        }
                    }
                }
            });
            list.add(newRoot.getId().getLong());
            ArrayList<DBObject> add = new ArrayList<DBObject>();
            for (Long id : list) {
                NodeImpl n = temp.get(id);
                long newId;
                newId = nextId++;
                n.setId(NodeId.get(newId));
                pos.put(id, newId);
                BasicDBObject dataObject = new BasicDBObject(KEY_FIELD, newId);
                dataObject.append(DATA_FIELD, n.asString());
                add.add(dataObject);
                cache.put(n.getId().getLong(), n);
            }
            nodesStore.insert(add);
            String r = String.valueOf(newRoot.getId().getLong());
            settingsStore.insert(new BasicDBObject(KEY_FIELD, r));
            temp.clear();
            pos.clear();
        } catch (Exception e) {
            throw ExceptionFactory.convert(e);
        }
        root = newRoot;
        return root.getId();
    }

    @Override
    public NodeId getId(NodeId id) {
        long x = id.getLong();
        return (x > 0 || !pos.containsKey(x)) ? id : NodeId.get(pos.get(x));
    }

    @Override
    public NodeId getRootId() {
        return root.getId();
    }

    @Override
    public NodeImpl getInfo(String path) {
        NodeImpl n = new NodeImpl(this, 0);
        for (Entry<String, String> e : properties.entrySet()) {
            n.setProperty("property." + e.getKey(), e.getValue());
        }
        n.setProperty("cache.size", "" + cache.size());
        n.setProperty("cache.memoryUsed", "" + cache.getMemoryUsed());
        n.setProperty("cache.memoryMax", "" + cache.getMemoryMax());
        n.setProperty("nextId", "" + nextId);
        n.setProperty("root", "" + root.getId());
        return n;
    }

    @Override
    public synchronized void close() {
        con.close();
    }

}
