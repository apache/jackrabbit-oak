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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.remote.RemoteCredentials;
import org.apache.jackrabbit.oak.remote.RemoteLoginException;
import org.apache.jackrabbit.oak.remote.RemoteRepository;
import org.apache.jackrabbit.oak.remote.RemoteSession;

import java.util.Set;

public class ContentRemoteRepository implements RemoteRepository {

    private final ContentRepository contentRepository;

    private final ContentRemoteRevisions contentRemoteRevisions;

    private final ContentRemoteBinaries contentRemoteBinaries;

    public ContentRemoteRepository(ContentRepository contentRepository) {
        this.contentRemoteRevisions = new ContentRemoteRevisions();
        this.contentRemoteBinaries = new ContentRemoteBinaries();
        this.contentRepository = contentRepository;
    }

    @Override
    public RemoteCredentials createBasicCredentials(final String user, final char[] password) {
        return new BasicContentRemoteCredentials(user, password);
    }

    @Override
    public RemoteCredentials createImpersonationCredentials(Set<String> principals) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public RemoteSession login(RemoteCredentials remoteCredentials) throws RemoteLoginException {
        ContentRemoteCredentials contentRemoteCredentials = null;

        if (remoteCredentials instanceof ContentRemoteCredentials) {
            contentRemoteCredentials = (ContentRemoteCredentials) remoteCredentials;
        }

        if (contentRemoteCredentials == null) {
            throw new IllegalArgumentException("invalid credentials");
        }

        Thread thread = Thread.currentThread();

        ClassLoader loader = thread.getContextClassLoader();

        thread.setContextClassLoader(Oak.class.getClassLoader());

        ContentSession session;

        try {
            session = contentRemoteCredentials.login(contentRepository);
        } finally {
            thread.setContextClassLoader(loader);
        }

        return new ContentRemoteSession(session, contentRemoteRevisions, contentRemoteBinaries);
    }

}