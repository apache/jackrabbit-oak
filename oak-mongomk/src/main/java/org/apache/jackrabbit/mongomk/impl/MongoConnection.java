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
package org.apache.jackrabbit.mongomk.impl;

import org.apache.jackrabbit.mongomk.model.CommitMongo;
import org.apache.jackrabbit.mongomk.model.HeadMongo;
import org.apache.jackrabbit.mongomk.model.NodeMongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;

/**
 * The {@code MongoConnection} contains connection properties for the {@code MongoDB}.
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class MongoConnection {

    private static final String COLLECTION_COMMITS = "commits";
    private static final String COLLECTION_HEAD = "head";
    private static final String COLLECTION_NODES = "nodes";
    private final DB db;
    private final GridFS gridFS;

    /**
     * Constructs a new {@code MongoConnection}.
     *
     * @param host The host address.
     * @param port The port.
     * @param database The database name.
     * @throws Exception If an error occurred while trying to connect.
     */
    public MongoConnection(String host, int port, String database) throws Exception {
        db = new Mongo(host, port).getDB(database);
        gridFS = new GridFS(db);
    }

    /**
     * Returns the commit {@link DBCollection}.
     *
     * @return The commit {@link DBCollection}.
     */
    public DBCollection getCommitCollection() {
        DBCollection commitCollection = db.getCollection(COLLECTION_COMMITS);
        commitCollection.setObjectClass(CommitMongo.class);
        return commitCollection;
    }

    /**
     * Returns the {@link DB}.
     *
     * @return The {@link DB}.
     */
    public DB getDB() {
        return db;
    }

    /**
     * Returns the {@link GridFS}.
     *
     * @return The {@link GridFS}.
     */
    public GridFS getGridFS() {
        return gridFS;
    }

    /**
     * Returns the head {@link DBCollection}.
     *
     * @return The head {@link DBCollection}.
     */
    public DBCollection getHeadCollection() {
        DBCollection headCollection = db.getCollection(COLLECTION_HEAD);
        headCollection.setObjectClass(HeadMongo.class);
        return headCollection;
    }

    /**
     * Returns the node {@link DBCollection}.
     *
     * @return The node {@link DBCollection}.
     */
    public DBCollection getNodeCollection() {
        DBCollection nodeCollection = db.getCollection(COLLECTION_NODES);
        nodeCollection.setObjectClass(NodeMongo.class);
        return nodeCollection;
    }
}