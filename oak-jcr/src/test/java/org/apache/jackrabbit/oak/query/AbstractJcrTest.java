/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.query;


import com.google.common.io.Closer;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.junit.After;
import org.junit.Before;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.QueryManager;
import java.io.IOException;

public abstract class AbstractJcrTest {

    private Repository jcrRepository;
    protected Session adminSession;
    protected Session anonymousSession;
    protected QueryManager qm;
    protected Closer closer;

    @Before
    public void before() throws Exception {
        closer = Closer.create();
        jcrRepository = createJcrRepository();

        adminSession = jcrRepository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        // we'd always query anonymously
        anonymousSession = jcrRepository.login(new GuestCredentials(), null);
        anonymousSession.refresh(true);
        anonymousSession.save();
        qm = anonymousSession.getWorkspace().getQueryManager();
        initialize();
    }

    @After
    public void after() throws IOException {
        closer.close();
        adminSession.logout();
        anonymousSession.logout();
        if (jcrRepository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) jcrRepository).shutdown();
        }
    }

    abstract protected Repository createJcrRepository() throws RepositoryException;
    /*
        Use this method to initialize variables/execute something after repository creation
     */
    protected void initialize(){ }
}
