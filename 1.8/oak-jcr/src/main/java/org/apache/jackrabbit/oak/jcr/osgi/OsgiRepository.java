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
package org.apache.jackrabbit.oak.jcr.osgi;

import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * Workaround to a JAAS class loading issue in OSGi environments.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-256">OAK-256</a>
 */
public class OsgiRepository extends RepositoryImpl {

    public OsgiRepository(ContentRepository repository,
                          Whiteboard whiteboard,
                          SecurityProvider securityProvider,
                          int observationQueueLength,
                          CommitRateLimiter commitRateLimiter,
                          boolean fastQueryResultSize) {
        super(repository, whiteboard, securityProvider, observationQueueLength, 
                commitRateLimiter, fastQueryResultSize);
    }

    @Override
    public Session login(Credentials credentials, String workspace)
            throws RepositoryException {
        // TODO: The context class loader hack below shouldn't be needed
        // with a properly OSGi-compatible JAAS implementation
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(Oak.class.getClassLoader());
            return super.login(credentials, workspace);
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

}
