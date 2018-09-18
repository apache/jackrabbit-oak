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
package org.apache.jackrabbit.oak.spi.security.authentication;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class ImpersonationCredentialsTest {

    private final AuthInfo info = new AuthInfoImpl(null, null, null);

    @Test
    public void testGetBaseCredentials() {
        Credentials creds = new GuestCredentials();
        assertSame(creds, new ImpersonationCredentials(creds, info).getBaseCredentials());

        Credentials simpleCreds = new SimpleCredentials("userId", new char[0]);
        assertSame(simpleCreds, new ImpersonationCredentials(simpleCreds, info).getBaseCredentials());
    }

    @Test
    public void testGetAuthInfo() {
        assertSame(info, new ImpersonationCredentials(new Credentials() {}, info).getImpersonatorInfo());
    }
}