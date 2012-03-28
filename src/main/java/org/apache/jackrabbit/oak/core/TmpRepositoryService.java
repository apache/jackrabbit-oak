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
package org.apache.jackrabbit.oak.core;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.Connection;
import org.apache.jackrabbit.oak.api.RepositoryService;
import org.apache.jackrabbit.oak.api.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

/**
 * TmpRepositoryService...
 */
public class TmpRepositoryService implements RepositoryService {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(TmpRepositoryService.class);

    // TODO: retrieve default wsp-name from configuration
    private static final String DEFAULT_WORKSPACE_NAME = "default";

    private final MicroKernel mk;

    public TmpRepositoryService(MicroKernel mk) {
        this.mk = mk;

        // FIXME: default mk-setup must be done elsewhere...
        String headRev = mk.getHeadRevision();
        if (!mk.nodeExists("/" + DEFAULT_WORKSPACE_NAME, headRev)) {
            mk.commit("/", "+ \"" + DEFAULT_WORKSPACE_NAME + "\" : {}", headRev, null);
        }
    }

    @Override
    public SessionInfo login(Object credentials, String workspaceName) throws LoginException, NoSuchWorkspaceException {
        // TODO: add proper implementation
        // TODO  - authentication against configurable spi-authentication
        // TODO  - validation of workspace name (including access rights for the given 'user')

        final SimpleCredentials sc;
        if (credentials == null || credentials instanceof GuestCredentials) {
            sc = new SimpleCredentials("anonymous", new char[0]);
        } else if (credentials instanceof SimpleCredentials) {
            sc = (SimpleCredentials) credentials;
        } else {
            sc = null;
        }

        final String wspName = (workspaceName == null) ? DEFAULT_WORKSPACE_NAME : workspaceName;
        final String revision = getRevision(credentials);

        if (sc != null) {
            return new SessionInfoImpl(sc, wspName, revision);
        } else {
            throw new LoginException("login failed...");
        }
    }

    @Override
    public Connection getConnection(SessionInfo sessionInfo) {
        // TODO
        return null;
    }

    /**
     * @param credentials The credentials object used for authentication.
     * @return The microkernal revision. If the give credentials don't specify
     * a specific revision number the current head revision is returned.
     */
    private String getRevision(Object credentials) {
        // TODO: define if/how desired revision can be passed in by the credentials object.
        return mk.getHeadRevision();
    }
}