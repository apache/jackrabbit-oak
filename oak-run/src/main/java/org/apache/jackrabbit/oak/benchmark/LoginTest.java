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

import javax.jcr.RepositoryException;
import javax.jcr.Session;

public class LoginTest extends AbstractLoginTest {

    private final Session[] sessions = new Session[COUNT];

    public LoginTest() {
        this("admin", false, DEFAULT_ITERATIONS);
    }

    public LoginTest(String runAsUser, boolean runWithToken, int noIterations) {
        super(runAsUser, runWithToken, noIterations);
    }

    @Override
    public void runTest() throws RepositoryException {
        for (int i = 0; i < sessions.length; i++) {
            sessions[i] = getRepository().login(getCredentials());
        }
    }

    @Override
    public void afterTest() throws RepositoryException {
        for (Session session : sessions) {
            if (session.isLive()) {
                session.logout();
            }
        }
    }
}
