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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.remote.RemoteCredentials;
import org.apache.jackrabbit.oak.remote.RemoteLoginException;
import org.junit.Ignore;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.security.auth.login.LoginException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContentRemoteRepositoryTest {

    private ContentRemoteRepository createRepository() {
        return createRepository(mock(ContentRepository.class));
    }

    private ContentRemoteRepository createRepository(ContentRepository repository) {
        return new ContentRemoteRepository(repository);
    }

    @Test
    public void testCreateBasicCredentials() {
        assertNotNull(createRepository().createBasicCredentials("admin", "admin".toCharArray()));
    }

    @Test
    @Ignore
    public void testCreateImpersonationCredentials() {
        assertNotNull(createRepository().createImpersonationCredentials(Sets.newHashSet("admin")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoginWithNullCredentials() throws Exception {
        createRepository().login(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoginWithInvalidCredentials() throws Exception {
        createRepository().login(mock(RemoteCredentials.class));
    }

    @Test
    public void testSuccessfulLoginWithBasicCredentials() throws Exception {
        ContentRepository repository = mock(ContentRepository.class);
        when(repository.login(any(Credentials.class), anyString())).thenReturn(mock(ContentSession.class));

        ContentRemoteRepository remoteRepository = createRepository(repository);
        assertNotNull(remoteRepository.login(remoteRepository.createBasicCredentials("admin", "admin".toCharArray())));
    }

    @Test(expected = RemoteLoginException.class)
    public void testUnsuccessfulLoginWithBasicCredentials() throws Exception {
        ContentRepository repository = mock(ContentRepository.class);
        when(repository.login(any(Credentials.class), anyString())).thenThrow(LoginException.class);

        ContentRemoteRepository remoteRepository = createRepository(repository);
        remoteRepository.login(remoteRepository.createBasicCredentials("admin", "admin".toCharArray()));
    }

}
