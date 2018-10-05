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

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.core.security.SystemPrincipal;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;

public class LoginSystemTest extends AbstractLoginTest {

    private Subject subject;

    @Override
    public void beforeSuite() throws Exception {
        super.beforeSuite();
        if (getRepository() instanceof RepositoryImpl) {
            subject = SystemSubject.INSTANCE;
        } else {
            subject = new Subject(true, ImmutableSet.of(new SystemPrincipal()), Collections.emptySet(), Collections.emptySet());
        }
    }

    @Override
    public void runTest() throws RepositoryException {
        for (int i = 0; i < COUNT; i++) {
            try {
                Subject.doAsPrivileged(subject, new PrivilegedExceptionAction<Session>() {
                    @Override
                    public Session run() throws Exception {
                        return getRepository().login(null, null);
                    }
                }, null).logout();
            } catch (PrivilegedActionException e) {
                throw new RepositoryException("failed to retrieve admin session.", e);
            }
        }
    }
}
