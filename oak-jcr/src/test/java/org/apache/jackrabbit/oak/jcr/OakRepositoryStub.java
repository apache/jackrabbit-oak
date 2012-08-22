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

import static org.apache.jackrabbit.oak.jcr.RepositoryTestUtils.buildDefaultCommitEditor;

import java.io.IOException;
import java.security.Principal;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.RepositoryStub;

public class OakRepositoryStub extends RepositoryStub {

    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     * 
     * @param settings repository settings
     * @throws javax.jcr.RepositoryException If an error occurs.
     * @throws java.io.IOException
     */
    public OakRepositoryStub(Properties settings) throws RepositoryException, IOException {
        super(settings);

        MicroKernel microkernel = new MicroKernelImpl("target/mk-tck-" + System.currentTimeMillis());
        ContentRepository contentRepository = new ContentRepositoryImpl(microkernel, null, buildDefaultCommitEditor());
        repository = new RepositoryImpl(contentRepository, Executors.newScheduledThreadPool(1));

        Session session = repository.login(superuser);
        try {
            TestContentLoader loader = new TestContentLoader();
            loader.loadTestContent(session);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        } finally {
            session.logout();
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
    public Principal getKnownPrincipal(Session session) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    private static Principal UNKNOWN_PRINCIPAL = new Principal() {
        @Override
        public String getName() {
            return "an_unknown_user";
        }
    };

    @Override
    public Principal getUnknownPrincipal(Session session) throws RepositoryException, NotExecutableException {
        return UNKNOWN_PRINCIPAL;
    }

}
