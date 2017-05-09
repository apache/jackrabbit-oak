/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.http.handler;

import org.apache.jackrabbit.oak.remote.RemoteCredentials;
import org.apache.jackrabbit.oak.remote.RemoteLoginException;
import org.apache.jackrabbit.oak.remote.RemoteRepository;
import org.apache.jackrabbit.oak.remote.RemoteSession;
import org.apache.jackrabbit.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.apache.jackrabbit.oak.remote.http.handler.ResponseUtils.sendInternalServerError;

class AuthenticationWrapperHandler implements Handler {

    private static final Logger logger = LoggerFactory.getLogger(AuthenticationWrapperHandler.class);

    private final Handler authenticated;

    private final Handler notAuthenticated;

    public AuthenticationWrapperHandler(Handler authenticated, Handler notAuthenticated) {
        this.authenticated = authenticated;
        this.notAuthenticated = notAuthenticated;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        RemoteSession session = (RemoteSession) request.getAttribute("session");

        if (session != null) {
            authenticated.handle(request, response);
            return;
        }

        RemoteRepository repository = (RemoteRepository) request.getAttribute("repository");

        if (repository == null) {
            sendInternalServerError(response, "repository not found");
            return;
        }

        RemoteCredentials credentials = extractCredentials(request, repository);

        if (credentials == null) {
            notAuthenticated.handle(request, response);
            return;
        }

        try {
            session = repository.login(credentials);
        } catch (RemoteLoginException e) {
            logger.warn("unable to authenticate to the repository", e);
            notAuthenticated.handle(request, response);
            return;
        }

        request.setAttribute("session", session);

        authenticated.handle(request, response);
    }

    private RemoteCredentials extractCredentials(HttpServletRequest request, RemoteRepository repository) {
        String authorization = request.getHeader("Authorization");

        if (authorization == null) {
            return null;
        }

        String scheme = getScheme(authorization);

        if (!scheme.equalsIgnoreCase("basic")) {
            return null;
        }

        String token = getToken(authorization);

        if (token == null) {
            return null;
        }

        String decoded;

        try {
            decoded = Base64.decode(token);
        } catch (IllegalArgumentException e) {
            return null;
        }

        String user = getUser(decoded);

        if (user == null) {
            return null;
        }

        String password = getPassword(decoded);

        if (password == null) {
            return null;
        }

        return repository.createBasicCredentials(user, password.toCharArray());
    }

    private String getScheme(String authorization) {
        int index = authorization.indexOf(' ');

        if (index < 0) {
            return authorization;
        }

        return authorization.substring(0, index);
    }

    private String getToken(String authorization) {
        int index = authorization.indexOf(' ');

        if (index < 0) {
            return null;
        }

        while (index < authorization.length()) {
            if (authorization.charAt(index) != ' ') {
                break;
            }

            index += 1;
        }

        if (index < authorization.length()) {
            return authorization.substring(index);
        }

        return null;
    }

    private String getUser(String both) {
        int index = both.indexOf(':');

        if (index < 0) {
            return null;
        }

        return both.substring(0, index);
    }

    private String getPassword(String both) {
        int index = both.indexOf(':');

        if (index < 0) {
            return null;
        }

        if (index + 1 < both.length()) {
            return both.substring(index + 1);
        }

        return null;
    }

}
