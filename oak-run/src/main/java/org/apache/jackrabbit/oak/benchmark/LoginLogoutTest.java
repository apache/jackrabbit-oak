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
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

public class LoginLogoutTest extends AbstractTest {

    @Override
    public void setUp(Repository repository, Credentials credentials)
            throws Exception {
        super.setUp(repository,
                new SimpleCredentials("admin", "admin".toCharArray()));
    }
    
    @Override
    public void runTest() throws RepositoryException {
        Repository repository = getRepository();
        for (int i = 0; i < 1000; i++) {
            Session session = repository.login(getCredentials());
            try {
                session.getRootNode();
            } finally {
                session.logout();
            }
        }
    }

}
