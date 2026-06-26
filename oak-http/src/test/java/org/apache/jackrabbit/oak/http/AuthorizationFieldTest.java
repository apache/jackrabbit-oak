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

import org.junit.Test;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AuthorizationFieldTest {

    @Test(expected = LoginException.class)
    public void testNone() throws LoginException {
        AuthorizationField.valueOf(Collections.enumeration(List.of()));
    }

    @Test(expected = LoginException.class)
    public void testTwo() throws LoginException {
        AuthorizationField.valueOf(Collections.enumeration(List.of("a", "b")));
    }

    @Test
    public void testValid() throws LoginException {
        String b64 = Base64.getEncoder().encodeToString("foo:bar".getBytes(StandardCharsets.UTF_8));
        SimpleCredentials credentials = AuthorizationField.valueOf(Collections.enumeration(List.of("Basic " + b64)));
        assertEquals("foo", credentials.getUserID());
        assertEquals("bar", new String(credentials.getPassword()));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class) // BUG: OAK-12259
    public void testInvalidBase64() throws LoginException {
        String b64 = Base64.getEncoder().encodeToString("foo:bar".getBytes(StandardCharsets.UTF_8));
        SimpleCredentials credentials = AuthorizationField.valueOf(Collections.enumeration(List.of("Basic dksjdkj" + b64)));
        assertEquals("foo", credentials.getUserID());
        assertEquals("bar", new String(credentials.getPassword()));
    }

    @Test(expected = LoginException.class)
    public void testNoScheme() throws LoginException {
        String b64 = Base64.getEncoder().encodeToString("foo:bar".getBytes(StandardCharsets.UTF_8));
        AuthorizationField.valueOf(Collections.enumeration(List.of(b64)));
    }

    @Test
    public void testColonInPassword() throws LoginException {
        String b64 = "Basic " + Base64.getEncoder().encodeToString("foo:bar:qux".getBytes(StandardCharsets.UTF_8));
        SimpleCredentials credentials =AuthorizationField.valueOf(Collections.enumeration(List.of(b64)));
        assertEquals("foo", credentials.getUserID());
        // BUG: OAK-12259
        assertEquals("bar", new String(credentials.getPassword()));
    }
}