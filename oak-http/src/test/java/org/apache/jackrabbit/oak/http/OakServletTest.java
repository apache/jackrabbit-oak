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
package org.apache.jackrabbit.oak.http;

import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.util.Base64;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OakServletTest {

    private static String basicHeader(String userId, String password) {
        return "Basic " + Base64.encode(userId + ":" + password);
    }

    /**
     * Drives {@link OakServlet#service} and captures the credentials it derives
     * from the {@code Authorization} header. The repository is stubbed to throw
     * {@link NoSuchWorkspaceException} so the request short-circuits (handled as
     * a 404) right after the credentials are parsed.
     */
    private static SimpleCredentials parsedCredentials(String authorization)
            throws Exception {
        ContentRepository repository = mock(ContentRepository.class);
        ArgumentCaptor<Credentials> captor =
                ArgumentCaptor.forClass(Credentials.class);
        when(repository.login(captor.capture(), isNull()))
                .thenThrow(new NoSuchWorkspaceException());

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader("Authorization")).thenReturn(authorization);
        HttpServletResponse response = mock(HttpServletResponse.class);

        new OakServlet(repository).service(request, response);

        verify(response).sendError(HttpServletResponse.SC_NOT_FOUND);
        Credentials credentials = captor.getValue();
        assertTrue(credentials instanceof SimpleCredentials);
        return (SimpleCredentials) credentials;
    }

    /**
     * Asserts that a malformed {@code Authorization} header is rejected with a
     * 401 challenge and never reaches the repository login.
     */
    private static void assertUnauthorized(String authorization)
            throws Exception {
        ContentRepository repository = mock(ContentRepository.class);
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader("Authorization")).thenReturn(authorization);
        HttpServletResponse response = mock(HttpServletResponse.class);

        new OakServlet(repository).service(request, response);

        verify(response).setHeader("WWW-Authenticate", "Basic realm=\"Oak\"");
        verify(response).sendError(HttpServletResponse.SC_UNAUTHORIZED);
        verify(repository, never()).login(any(), any());
    }

    @Test
    public void parsesSimpleCredentials() throws Exception {
        SimpleCredentials credentials = parsedCredentials(basicHeader("admin", "secret"));
        assertEquals("admin", credentials.getUserID());
        assertArrayEquals("secret".toCharArray(), credentials.getPassword());
    }

    /**
     * Core security regression: a password may contain colons (RFC 7617), and
     * the whole password must be preserved. The previous {@code split(":")}
     * implementation silently truncated it, weakening authentication.
     */
    @Test
    public void preservesColonsInPassword() throws Exception {
        SimpleCredentials credentials = parsedCredentials(basicHeader("admin", "p4ss:w0rd:!"));
        assertEquals("admin", credentials.getUserID());
        assertArrayEquals("p4ss:w0rd:!".toCharArray(), credentials.getPassword());
    }

    @Test
    public void acceptsEmptyPassword() throws Exception {
        SimpleCredentials credentials = parsedCredentials(basicHeader("admin", ""));
        assertEquals("admin", credentials.getUserID());
        assertArrayEquals(new char[0], credentials.getPassword());
    }

    @Test
    public void acceptsEmptyUserId() throws Exception {
        SimpleCredentials credentials = parsedCredentials(basicHeader("", "secret"));
        assertEquals("", credentials.getUserID());
        assertArrayEquals("secret".toCharArray(), credentials.getPassword());
    }

    /**
     * A decoded value without a colon must be rejected cleanly (401) rather than
     * throwing an unhandled {@link ArrayIndexOutOfBoundsException} that would
     * surface as an HTTP 500.
     */
    @Test
    public void rejectsMissingColon() throws Exception {
        assertUnauthorized("Basic " + Base64.encode("nocolon"));
    }

    @Test
    public void rejectsMissingHeader() throws Exception {
        assertUnauthorized(null);
    }

    @Test
    public void rejectsNonBasicScheme() throws Exception {
        assertUnauthorized("Bearer sometoken");
    }
}
