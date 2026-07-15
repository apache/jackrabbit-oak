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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RdbConnectionUtils;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;

/**
 * A repository stub implementation for the RDB document store.
 */
public class OakDocumentRDBRepositoryStub extends BaseRepositoryStub {

    private static final String jdbcUrl = RdbConnectionUtils.mapJdbcURL();

    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     * 
     * @param settings
     *            repository settings
     * @throws javax.jcr.RepositoryException
     *             If an error occurs.
     */
    public OakDocumentRDBRepositoryStub(Properties settings) throws RepositoryException {
        super(settings);

        final DocumentNodeStore m;
        try {
            String prefix = "T" + Long.toHexString(System.currentTimeMillis());
            RDBOptions options = new RDBOptions().tablePrefix(prefix).dropTablesOnClose(true);
            m = new RDBDocumentNodeStoreBuilder().
                    memoryCacheSize(64 * 1024 * 1024).
                    setPersistentCache("target/persistentCache,time").
                    setRDBConnection(RDBDataSourceFactory.forJdbcUrl(jdbcUrl, RdbConnectionUtils.USERNAME, RdbConnectionUtils.PASSWD), options).
                    build();
            Jcr jcr = new Jcr(m);
            preCreateRepository(jcr);
            this.repository = jcr.createRepository();
            loadTestContent(repository);
        } catch (Exception e) {
            throw new RepositoryException(e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                m.dispose();
            }
        }));
    }

    public static boolean isAvailable() {
        try {
            Connection c = DriverManager.getConnection(jdbcUrl, RdbConnectionUtils.USERNAME, RdbConnectionUtils.PASSWD);
            c.close();
            return true;
        }
        catch (SQLException ex) {
            // expected
            return false;
        }
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
