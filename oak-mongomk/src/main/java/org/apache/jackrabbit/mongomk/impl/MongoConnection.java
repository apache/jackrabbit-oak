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

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * The {@code MongoConnection} abstracts connection to the {@code MongoDB}.
 */
public class MongoConnection {

    private final DB db;
    private final Mongo mongo;

    /**
     * Constructs a new {@code MongoConnection}.
     *
     * @param host The host address.
     * @param port The port.
     * @param database The database name.
     * @throws Exception If an error occurred while trying to connect.
     */
    public MongoConnection(String host, int port, String database) throws Exception {
        mongo = new Mongo(host, port);
        db = mongo.getDB(database);
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
     * Closes the underlying Mongo instance
     */
    public void close(){
        if (mongo != null){
            mongo.close();
        }
    }
}