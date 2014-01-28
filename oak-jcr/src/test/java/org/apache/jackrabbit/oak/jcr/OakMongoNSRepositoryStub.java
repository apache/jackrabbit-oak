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
package org.apache.jackrabbit.oak.jcr;

import java.util.Properties;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.document.MongoMK;
import org.apache.jackrabbit.oak.plugins.document.MongoNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;

/**
 * A repository stub using the MongoNodeStore.
 */
public class OakMongoNSRepositoryStub extends OakMongoMKRepositoryStub {

    public OakMongoNSRepositoryStub(Properties settings)
            throws RepositoryException {
        super(settings);
    }

    @Override
    protected Repository createRepository(MongoConnection connection) {
        MongoNodeStore store = new MongoMK.Builder().setClusterId(1).
                memoryCacheSize(64 * 1024 * 1024).
                setMongoDB(connection.getDB()).getNodeStore();
        return new Jcr(store).createRepository();
    }
}
