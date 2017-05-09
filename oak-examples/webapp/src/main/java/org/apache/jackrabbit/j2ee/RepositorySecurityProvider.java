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

package org.apache.jackrabbit.j2ee;

import javax.jcr.Credentials;
import javax.jcr.LoginException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.felix.webconsole.WebConsoleSecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple WebConsoleSecurityProvider implementation which only allows
 * repository admin user to perform login
 */
class RepositorySecurityProvider implements WebConsoleSecurityProvider {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Repository repository;

    RepositorySecurityProvider(Repository repository) {
        this.repository = repository;
    }

    @Override
    public Object authenticate(String userName, String password) {
        final Credentials creds = new SimpleCredentials(userName,
                (password == null) ? new char[0] : password.toCharArray());
        Session session = null;
        try {
            session = repository.login(creds);

            //TODO Need to extend it support login for any user with AdminPrincipal
            //credential
            if ("admin".equals(userName)) {
                return userName;
            }

        } catch (LoginException re) {
            log.info("authenticate: User {} failed to authenticate with the repository " +
                    "for Web Console access", userName, re);
        } catch (RepositoryException re) {
            log.info("authenticate: Generic problem trying grant User {} access to the Web Console", userName, re);
        } finally {
            if (session != null) {
                session.logout();
            }
        }
        return null;
    }

    @Override
    public boolean authorize(Object user, String role) {
        //No fine grained access control for now
        return true;
    }
}