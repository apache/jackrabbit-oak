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
import java.security.Principal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.RepositoryStub;

/**
 * A repository stub implementation for the RDB document store.
 */
public class OakDocumentRDBRepositoryStub extends RepositoryStub {

    protected static final String URL = System.getProperty("rdb.jdbc-url", "");

    protected static final String USERNAME = System.getProperty("rdb.jdbc-user", "sa");

    protected static final String PASSWD = System.getProperty("rdb.jdbc-passwd", "");

    private static final Principal UNKNOWN_PRINCIPAL = new Principal() {
        @Override
        public String getName() {
            return "an_unknown_user";
        }
    };

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
        final DocumentNodeStore m;
        try {
            String prefix = "T" + Long.toHexString(System.currentTimeMillis());
            RDBOptions options = new RDBOptions().tablePrefix(prefix).dropTablesOnClose(true);
            m = new DocumentMK.Builder().
                    setClusterId(1).
                    memoryCacheSize(64 * 1024 * 1024).
                    setRDBConnection(RDBDataSourceFactory.forJdbcUrl(jdbcUrl, USERNAME, PASSWD), options).
                    getNodeStore();
            this.repository = new Jcr(m).createRepository();
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
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    m.dispose();
                } catch (Exception e) {
                    // ignore when shutdown fails
                }
            }
        }));
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

    @Override
    public Credentials getReadOnlyCredentials() {
        return new GuestCredentials();
    }

    @Override
    public Principal getKnownPrincipal(Session session)
            throws RepositoryException {
        if (session instanceof JackrabbitSession) {
            return ((JackrabbitSession) session).getPrincipalManager().getPrincipal(session.getUserID());
        }
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public Principal getUnknownPrincipal(Session session)
            throws RepositoryException, NotExecutableException {
        return UNKNOWN_PRINCIPAL;
    }
}
