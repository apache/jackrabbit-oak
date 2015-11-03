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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;

/**
 * A repository stub implementation for the RDB document store.
 */
public class OakDocumentRDBRepositoryStub extends OakRepositoryStub {

    protected static final String URL = System.getProperty("rdb.jdbc-url", "jdbc:h2:file:./{fname}oaktest");

    protected static final String USERNAME = System.getProperty("rdb.jdbc-user", "sa");

    protected static final String PASSWD = System.getProperty("rdb.jdbc-passwd", "");

    private final Repository repository;

    private static final String fname = (new File("target")).isDirectory() ? "target/" : "";
    private static final String jdbcUrl = URL.replace("{fname}", fname);

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

        Session session = null;
        try {
            this.repository = createRepository(OakDocumentRDBRepositoryStub.jdbcUrl, USERNAME, PASSWD);
            session = getRepository().login(superuser);
            TestContentLoader loader = new TestContentLoader();
            loader.loadTestContent(session);
        } catch (Exception e) {
            throw new RepositoryException(e);
        } finally {
            if (session != null) {
                session.logout();
            }
        }
        // Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(connection)));
    }

    protected Repository createRepository(String url, String username, String password) {
        String prefix = "T" + UUID.randomUUID().toString().replace("-",  "");
        RDBOptions options = new RDBOptions().tablePrefix(prefix).dropTablesOnClose(true);
        DocumentNodeStore m = new DocumentMK.Builder().
                memoryCacheSize(64 * 1024 * 1024).
                setPersistentCache("target/persistentCache,time").
                setRDBConnection(RDBDataSourceFactory.forJdbcUrl(url, username, password), options).
                getNodeStore();
        QueryEngineSettings qs = new QueryEngineSettings();
        qs.setFullTextComparisonWithoutIndex(true);
        return new Jcr(m).with(qs).createRepository();
    }

    public static boolean isAvailable() {
        try {
            Connection c = DriverManager.getConnection(OakDocumentRDBRepositoryStub.jdbcUrl, USERNAME, PASSWD);
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
