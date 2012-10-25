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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.util.Cache;
import org.apache.jackrabbit.oak.plugins.index.old.mk.ExceptionFactory;
import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.NodeImpl.ChildVisitor;

/**
 * A node map that stores data in a database.
 */
public class NodeMapInDb extends NodeMap implements Cache.Backend<Long, NodeImpl> {

    final HashMap<Long, NodeImpl> temp = new HashMap<Long, NodeImpl>();
    private Connection conn;
    private final String url;
    private final TreeMap<String, String> properties = new TreeMap<String, String>();
    private final Cache<Long, NodeImpl> cache = Cache.newInstance(this, 10 * 1024 * 1024);
    private final HashMap<Long, Long> pos = new HashMap<Long, Long>();
    private final PreparedStatement select, insert, merge;
    private long nextId;
    private NodeImpl root = new NodeImpl(this, 0);

    NodeMapInDb(String dir) {
        try {
            String path = new File(dir, "nodes").getAbsolutePath();
            url = "jdbc:h2:" + path + System.getProperty("mk.db", "");
            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection(url);
            Statement stat = conn.createStatement();
            stat.execute("create table if not exists nodes(id bigint primary key, data varchar)");
            stat.execute("create table if not exists roots(key bigint primary key, value bigint) as select 0, 0");
            ResultSet rs = stat.executeQuery("select max(id) from nodes");
            rs.next();
            nextId = rs.getLong(1) + 1;
            rs = stat.executeQuery("select max(value) from roots");
            rs.next();
            long x = rs.getLong(1);
            if (x != 0) {
                root.setId(NodeId.get(x));
            }
            select = conn.prepareStatement("select * from nodes where id = ?");
            insert = conn.prepareStatement("insert into nodes sorted select ?, ?");
            merge = conn.prepareStatement("update roots set value = ?");
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
            select.setLong(1, key);
            ResultSet rs = select.executeQuery();
            if (!rs.next()) {
                throw ExceptionFactory.get("Node not found: " + key);
            }
            return NodeImpl.fromString(this, rs.getString(2));
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
            for (Long id : list) {
                NodeImpl n = temp.get(id);
                long newId;
                newId = nextId++;
                n.setId(NodeId.get(newId));
                pos.put(id, newId);
                insert.setLong(1, newId);
                insert.setString(2, n.asString());
                insert.execute();
                cache.put(n.getId().getLong(), n);
            }
            merge.setLong(1, newRoot.getId().getLong());
            merge.execute();
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
        n.setProperty("url", JsopBuilder.encode(url));
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
        try {
            conn.close();
        } catch (SQLException e) {
            throw ExceptionFactory.convert(e);
        }
    }

}
