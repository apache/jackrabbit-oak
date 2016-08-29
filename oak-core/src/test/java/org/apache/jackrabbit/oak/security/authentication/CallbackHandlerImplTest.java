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
package org.apache.jackrabbit.oak.security.authentication;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.WhiteboardCallback;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class CallbackHandlerImplTest extends AbstractSecurityTest {

    private final SimpleCredentials simpleCreds = new SimpleCredentials("id", "pw".toCharArray());
    private final Whiteboard whiteboard = new DefaultWhiteboard();

    private CallbackHandlerImpl callbackHandler;

    @Override
    public void before() throws Exception {
        super.before();

        callbackHandler = create(simpleCreds);
    }

    private CallbackHandlerImpl create(@Nonnull Credentials creds) {
        return new CallbackHandlerImpl(creds, root.getContentSession().getWorkspaceName(), getContentRepository(), getSecurityProvider(), whiteboard);
    }

    @Test
    public void testCredentialsCallback() throws Exception {
        CredentialsCallback cb = new CredentialsCallback();

        callbackHandler.handle(new Callback[] {cb});
        assertSame(simpleCreds, cb.getCredentials());
    }

    @Test
    public void handlePasswordCallback() throws Exception {
        PasswordCallback cb = new PasswordCallback("prompt", false);

        callbackHandler.handle(new Callback[] {cb});
        assertArrayEquals(simpleCreds.getPassword(), cb.getPassword());
    }

    @Test
    public void handlePasswordCallback2() throws Exception {
        PasswordCallback cb = new PasswordCallback("prompt", false);

        create(new Credentials() {}).handle(new Callback[]{cb});
        assertNull(cb.getPassword());
    }

    @Test
    public void handleNameCallback() throws Exception {
        NameCallback cb = new NameCallback("prompt");

        callbackHandler.handle(new Callback[] {cb});
        assertEquals("id", cb.getName());
    }

    @Test
    public void handleNameCallback2() throws Exception {
        NameCallback cb = new NameCallback("prompt");

        create(new Credentials() {}).handle(new Callback[]{cb});
        assertNull(cb.getName());
    }

    @Test
    public void handleWhiteboardCallback() throws Exception {
        WhiteboardCallback cb = new WhiteboardCallback();

        callbackHandler.handle(new Callback[] {cb});
        assertSame(whiteboard, cb.getWhiteboard());
    }

    @Test
    public void handleRepositoryCallback() throws Exception {
        RepositoryCallback cb = new RepositoryCallback();

        callbackHandler.handle(new Callback[] {cb});
        assertSame(getContentRepository(), cb.getContentRepository());
        assertSame(getSecurityProvider(), cb.getSecurityProvider());
        assertEquals(root.getContentSession().getWorkspaceName(), cb.getWorkspaceName());
    }

    @Test(expected = UnsupportedCallbackException.class)
    public void handleUnknownCallback() throws Exception {
        callbackHandler.handle(new Callback[] {new Callback() {}});
    }

}