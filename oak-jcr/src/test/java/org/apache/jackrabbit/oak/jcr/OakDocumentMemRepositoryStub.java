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
import javax.jcr.Session;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.test.RepositoryStubException;

/**
 * A repository stub using the DocumentNodeStore with an in-memory DocumentStore.
 */
public class OakDocumentMemRepositoryStub extends OakRepositoryStub {

    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     *
     * @param settings repository settings
     * @throws javax.jcr.RepositoryException If an error occurs.
     */
    public OakDocumentMemRepositoryStub(Properties settings)
            throws RepositoryException {
        super(settings);
        Session session = null;
        final DocumentNodeStore store;
        try {
            store = new DocumentMK.Builder().getNodeStore();
            QueryEngineSettings qs = new QueryEngineSettings();
            qs.setFullTextComparisonWithoutIndex(true);
            this.repository = new Jcr(store).with(qs).with(BundlingConfigInitializer.INSTANCE).createRepository();

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
                store.dispose();
            }
        }));
    }

    @Override
    public Repository getRepository() throws RepositoryStubException {
        return repository;
    }
}
