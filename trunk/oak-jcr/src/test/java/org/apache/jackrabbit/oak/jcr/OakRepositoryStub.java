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

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.RepositoryStub;
import org.apache.jackrabbit.test.RepositoryStubException;

import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;

/**
 * A generic Oak repository stub implementation that delegates to a specific
 * backend configured using the fixtures mechanism.
 */
public class OakRepositoryStub extends BaseRepositoryStub {

    private final RepositoryStub delegate;

    public OakRepositoryStub(Properties env) throws RepositoryException {
        super(env);
        this.delegate = newDelegate(env);
    }

    @Override
    public Repository getRepository() throws RepositoryStubException {
        return delegate.getRepository();
    }

    @Override
    public Principal getKnownPrincipal(Session session)
            throws RepositoryException {
        return delegate.getKnownPrincipal(session);
    }

    @Override
    public Principal getUnknownPrincipal(Session session)
            throws RepositoryException, NotExecutableException {
        return delegate.getUnknownPrincipal(session);
    }

    private static RepositoryStub newDelegate(Properties settings)
            throws RepositoryException {
        // use first fixture for stub with segment-tar as default
        Fixture f = Iterables.getFirst(getFixtures(), Fixture.SEGMENT_TAR);
        if (f == Fixture.DOCUMENT_MEM) {
            return new OakDocumentMemRepositoryStub(settings);
        } else if (f == Fixture.DOCUMENT_RDB) {
            return new OakDocumentRDBRepositoryStub(settings);
        } else if (f == Fixture.DOCUMENT_NS) {
            return new OakMongoNSRepositoryStub(settings);
        } else if (f == Fixture.SEGMENT_MK) {
            return new OakTarMKRepositoryStub(settings);
        } else {
            return new OakSegmentTarRepositoryStub(settings);
        }
    }
}