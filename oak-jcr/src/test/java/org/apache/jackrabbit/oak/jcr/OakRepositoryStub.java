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

import java.security.Principal;
import java.util.Properties;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.RepositoryStub;

/**
 * Base RepositoryStub for the Oak Repository
 */
abstract class OakRepositoryStub extends RepositoryStub {

    private static final Principal UNKNOWN_PRINCIPAL = new Principal() {
        @Override
        public String getName() {
            return "an_unknown_user";
        }
    };

    protected OakRepositoryStub(Properties env) {
        super(env);
    }

    protected QueryEngineSettings getQueryEngineSettings() {
        QueryEngineSettings settings = new QueryEngineSettings();
        settings.setFullTextComparisonWithoutIndex(true);
        return settings;
    }

    @Override
    public Credentials getReadOnlyCredentials() {
        return new GuestCredentials();
    }

    @Override
    public Principal getKnownPrincipal(Session session) throws RepositoryException {
        return EveryonePrincipal.getInstance();
    }

    @Override
    public Principal getUnknownPrincipal(Session session) throws RepositoryException, NotExecutableException {
        return UNKNOWN_PRINCIPAL;
    }

    /**
     * Override in subclass and perform additional configuration on the
     * {@link Jcr} builder before the repository is created. This default
     * implementation set query engine settings as returned by
     * {@link #getQueryEngineSettings()} and adds a
     * {@link BundlingConfigInitializer}.
     *
     * @param jcr the builder.
     */
    protected void preCreateRepository(Jcr jcr) {
        jcr.with(getQueryEngineSettings());
        jcr.with(BundlingConfigInitializer.INSTANCE);
    }

    protected void loadTestContent(Repository repository)
            throws RepositoryException {
        Session session = repository.login(superuser);
        try {
            TestContentLoader loader = new TestContentLoader();
            loader.loadTestContent(session);
        } catch (RepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new RepositoryException(e);
        } finally {
            session.logout();
        }
    }

}