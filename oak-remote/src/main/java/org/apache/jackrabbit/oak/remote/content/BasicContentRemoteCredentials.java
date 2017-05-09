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

package org.apache.jackrabbit.oak.remote.content;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.remote.RemoteLoginException;

import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

class BasicContentRemoteCredentials implements ContentRemoteCredentials {

    private final String user;

    private final char[] password;

    public BasicContentRemoteCredentials(String user, char[] password) {
        this.user = user;
        this.password = password;
    }

    @Override
    public ContentSession login(ContentRepository repository) throws RemoteLoginException {
        ContentSession session;

        try {
            session = repository.login(new SimpleCredentials(user, password), null);
        } catch (LoginException e) {
            throw new RemoteLoginException("unable to login", e);
        } catch (NoSuchWorkspaceException e) {
            throw new RemoteLoginException("unable to use the default workspace", e);
        }

        return session;
    }

}
