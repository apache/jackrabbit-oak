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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.mockito.Answers;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.withSettings;

public class LoginModuleMonitorTest {

    private final LoginModuleMonitor noop = LoginModuleMonitor.NOOP;
    private final LoginModuleMonitor monitor = mock(TestLoginModuleMonitor.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));

    @Test
    public void testLoginError() {
        noop.loginError();
        verifyNoInteractions(monitor);

        monitor.loginError();
        verify(monitor, times(1)).loginError();
    }

    @Test
    public void testLoginFailed() {
        LoginException e = new LoginException();
        Credentials creds = new SimpleCredentials("uid", new char[0]);

        noop.loginFailed(e, creds);
        verifyNoInteractions(monitor);

        monitor.loginFailed(e, creds);
        verify(monitor, times(1)).loginFailed(e, creds);
    }

    @Test
    public void testPrincipalsCollected() {
        noop.principalsCollected(38, 7);
        verifyNoInteractions(monitor);

        monitor.principalsCollected(21, 11);
        verify(monitor, times(1)).principalsCollected(21, 11);
    }

    @Test
    public void testGetMonitorClass() {
        assertSame(LoginModuleMonitor.class, noop.getMonitorClass());
        assertSame(LoginModuleMonitor.class, monitor.getMonitorClass());
    }

    @Test
    public void testGetMonitorProperties() {
        assertTrue(noop.getMonitorProperties().isEmpty());
        assertTrue(monitor.getMonitorProperties().isEmpty());
    }

    public static class TestLoginModuleMonitor implements LoginModuleMonitor {

        @Override
        public void loginError() {
            //nop
        }

        @Override
        public void loginFailed(@NotNull LoginException loginException, @Nullable Credentials creds) {
            //nop
        }

        @Override
        public void principalsCollected(long timeTakenNanos, int numberOfPrincipals) {
            //nop
        }
    }
}
