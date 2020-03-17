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

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JaasLoginContextTest {

    @Before
    public void before() {
        Configuration c = ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
        Configuration.setConfiguration(c);
    }

    @After
    public void after() {
        Configuration.setConfiguration(null);
    }

    @Test(expected = LoginException.class)
    public void testMissingConfiguration() throws Exception {
        Configuration.setConfiguration(null);
        JaasLoginContext ctx = new JaasLoginContext("name");
    }

    @Test(expected = LoginException.class)
    public void testNullNameConstructor() throws Exception {
        Configuration.setConfiguration(null);
        JaasLoginContext ctx = new JaasLoginContext(null);
    }


    @Test
    public void testNameConstructor() throws Exception {
        JaasLoginContext ctx = new JaasLoginContext("name");
        assertNull(ctx.getSubject());
    }

    @Test
    public void testNameSubjectConstructor() throws Exception {
        Subject subject = new Subject();
        JaasLoginContext ctx = new JaasLoginContext("name", subject);
        assertEquals(subject, ctx.getSubject());
    }

    @Test(expected = LoginException.class)
    public void testNameSubjectNullCallbackConstructor() throws Exception {
        Subject subject = new Subject();
        JaasLoginContext ctx = new JaasLoginContext("name", subject, null);
    }

    @Test
    public void testNameSubjectCallbackConstructor() throws Exception {
        Subject subject = new Subject();
        JaasLoginContext ctx = new JaasLoginContext("name", subject, callbacks -> {});

        assertEquals(subject, ctx.getSubject());
    }

    @Test
    public void testNameSubjectCallbackConfigurationConstructor() throws Exception {
        Subject subject = new Subject();
        JaasLoginContext ctx = new JaasLoginContext("name", subject, callbacks -> {
        }, ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY));
        assertEquals(subject, ctx.getSubject());
    }
}