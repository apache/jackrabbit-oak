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

import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.jcr.RepositoryImpl;

/**
 * Workaround to a JAAS class loading issue in OSGi environments.
 *
 * @see <a href="https://issues.apache.org/jira/browse/OAK-256">OAK-256</a>
 */
public class OsgiRepository extends RepositoryImpl {

    public OsgiRepository(
            ContentRepository repository, ScheduledExecutorService executor) {
        super(repository, executor);
    }

    @Override
    public Session login(Credentials credentials, String workspace)
            throws RepositoryException {
        Thread thread = Thread.currentThread();
        ClassLoader loader = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(
                    ContentRepositoryImpl.class.getClassLoader());
            return super.login(credentials, workspace);
        } finally {
            thread.setContextClassLoader(loader);
        }
    }

}
