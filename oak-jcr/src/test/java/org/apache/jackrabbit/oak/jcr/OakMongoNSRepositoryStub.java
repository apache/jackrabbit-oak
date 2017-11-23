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

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;

/**
 * A repository stub using the DocumentNodeStore.
 */
public class OakMongoNSRepositoryStub extends OakRepositoryStub {

    static {
        MongoConnection c = MongoUtils.getConnection();
        if (c != null) {
            MongoUtils.dropCollections(c.getDB());
            c.close();
        }
    }

    private final MongoConnection connection;
    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     *
     * @param settings repository settings
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    public OakMongoNSRepositoryStub(Properties settings) throws RepositoryException {
        super(settings);
        final DocumentNodeStore store;
        try {
            this.connection = MongoUtils.getConnection();
            store = new DocumentMK.Builder().
                    memoryCacheSize(64 * 1024 * 1024).
                    setPersistentCache("target/persistentCache,time").
                    setMongoDB(connection.getDB()).
                    getNodeStore();
            Jcr jcr = new Jcr(store);
            preCreateRepository(jcr);
            this.repository = jcr.createRepository();
            loadTestContent(repository);
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                store.dispose();
            }
        }));
    }

    public static boolean isMongoDBAvailable() {
        return MongoUtils.isAvailable();
    }

    static MongoConnection createConnection(String db) throws Exception {
        return MongoUtils.getConnection(db);
    }

    /**
     * Returns the configured repository instance.
     *
     * @return the configured repository instance.
     */
    @Override
    public synchronized Repository getRepository() {
        return repository;
    }

}
